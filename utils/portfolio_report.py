#!/usr/bin/env python3
"""
portfolio_report.py

Generates a full report on trading operations activity by parsing the
immediate child directories of a root "/portfolio" directory, and writes
the result as BOTH a plain-text report and an HTML report.

Dependencies:
    pip install prettytable

Report structure (two separate passes over the children, per spec):

    1. LOGS section (for every immediate child of root):
         - Locates every "log source" directory found under <child>/logs.
           A "log source" directory is any directory (including logs/
           itself) that directly contains files. This covers both layouts
           seen in practice:
               lex/logs/lex.20260702.log          (files directly in logs/)
               admin/logs/ace/ace.20260702.log    (files in a subdir of logs/)
               admin/logs/hub/hub.20260702.log
         - For each log source directory found, prints/writes the filename
           of the most current file, then the last N lines of that file.

    2. POSITIONS / TRADING ACTIVITY section (for every immediate child of root):
         - POSITIONS: if <child>/positions exists, finds the most current
           file whose name contains "positions", parses it, and renders
           each logical sub-table (summary, current positions, position
           detail, account allocations) as a PrettyTable.
         - TRADING ACTIVITY: if <child>/trades exists, finds the most
           current "*pnl*" (CSV), "*orders*" (JSON), and "*trades*" (JSON)
           files, parses each, and renders their tables the same way.

By default, "most current" is determined by file modification time. Pass
--date to instead pin the report to a specific date embedded in each
file's name (e.g. --date 20260702 selects only files whose name contains
"20260702" -- logs, positions, pnl, orders, and trades files alike --
rather than picking the most recently modified file). If more than one
file happens to match the date, the most recently modified of those
matches is used as a tie-break.

Architecture note:
    Each process_*_file() function is a pure parser: file on disk in,
    List[TableData] out. Nothing about text or HTML lives in that layer.
    A single ReportBuilder instance then renders each TableData once
    (via PrettyTable) into both a text block and an HTML block, so every
    file is parsed exactly once regardless of how many output formats are
    requested.

Usage:
    python3 portfolio_report.py [--root /portfolio] [--tail-lines 50] [--output-dir .] [--date 20260702]
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from prettytable import PrettyTable

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

DEFAULT_ROOT = "/portfolio"
DEFAULT_TAIL_LINES = 50
DEFAULT_OUTPUT_DIR = "."


# --------------------------------------------------------------------------
# Generic filesystem helpers
# --------------------------------------------------------------------------

def get_immediate_child_dirs(path: Path) -> List[Path]:
    """Return the immediate child directories of `path`, sorted by name."""
    if not path.is_dir():
        return []
    return sorted(
        (p for p in path.iterdir() if p.is_dir()),
        key=lambda p: p.name.lower(),
    )


def find_target_file(
    directory: Path,
    name_contains: Optional[str] = None,
    date_filter: Optional[str] = None,
) -> Optional[Path]:
    """
    Return the *file* directly inside `directory` (non-recursive) that the
    report should use.

    If `name_contains` is given, only files whose name contains that
    substring (case-insensitive) are considered. This is how the
    "*positions*" / "*pnl*" / "*orders*" / "*trades*" matching is done.

    If `date_filter` is given, only files whose name contains that literal
    date string (e.g. "20260702") are considered -- this pins the report
    to one date instead of picking the most recently modified file.

    Among whatever survives both filters, the most recently modified file
    is returned (with no date_filter, that's simply "the most current
    file"; with a date_filter, it's a tie-break in the rare case more than
    one file matches that date).
    """
    if not directory.is_dir():
        return None

    candidates = [p for p in directory.iterdir() if p.is_file()]
    if name_contains:
        needle = name_contains.lower()
        candidates = [p for p in candidates if needle in p.name.lower()]
    if date_filter:
        candidates = [p for p in candidates if date_filter in p.name]

    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


def find_log_source_dirs(logs_dir: Path) -> List[Path]:
    """
    Recursively walk `logs_dir` and return every directory (including
    `logs_dir` itself) that directly contains at least one file.

    Each such directory is treated as one independent "log source" and is
    reported on separately, which naturally handles both a flat logs/
    directory and a logs/ directory that fans out into per-source
    subdirectories (e.g. admin/logs/ace, admin/logs/hub).
    """
    if not logs_dir.is_dir():
        return []

    source_dirs = []
    for dirpath, _dirnames, filenames in os.walk(logs_dir):
        if filenames:
            source_dirs.append(Path(dirpath))

    return sorted(source_dirs, key=lambda p: str(p).lower())


def tail_file(filepath: Path, num_lines: int = DEFAULT_TAIL_LINES) -> List[str]:
    """
    Return the last `num_lines` lines of `filepath`.

    Reads the file backwards in chunks rather than loading the whole thing
    into memory, since some log files can be tens of MB.
    """
    chunk_size = 8192
    lines: List[bytes] = []
    with open(filepath, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        block = b""

        while pos > 0 and len(lines) <= num_lines:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            block = f.read(read_size) + block
            lines = block.split(b"\n")

        result = lines[-num_lines:] if len(lines) > num_lines else lines

    return [line.decode("utf-8", errors="replace") for line in result]


# --------------------------------------------------------------------------
# Table data model -- the shared contract between parsing and rendering
# --------------------------------------------------------------------------

@dataclass
class TableData:
    """A normalized, format-agnostic table. Parsers build these; the
    ReportBuilder is the only thing that knows how to turn one into text
    or HTML."""
    title: str
    headers: List[str]
    rows: List[List[str]] = field(default_factory=list)
    notes: str = ""


def format_value(value: Any) -> str:
    """
    Render a single JSON/CSV scalar (or nested dict/list) as a display
    string suitable for a table cell.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, dict):
        return ", ".join(f"{k}: {format_value(v)}" for k, v in value.items())
    if isinstance(value, list):
        return ", ".join(format_value(v) for v in value)
    if isinstance(value, float):
        # Whole-number floats (5127.0) display as "5127"; otherwise trim
        # trailing zeros but keep meaningful precision (e.g. 143.922733).
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def records_to_table(title: str, records: List[Dict[str, Any]]) -> Optional[TableData]:
    """
    Convert a list of flat (or shallow-nested) JSON dicts into a TableData.
    Column order follows first-seen key order across all records so it's
    stable even if later records add fields. Returns None if there are no
    records, so callers can skip empty sub-tables entirely.
    """
    if not records:
        return None

    headers: List[str] = []
    for rec in records:
        for k in rec.keys():
            if k not in headers:
                headers.append(k)

    rows = [[format_value(rec.get(h)) for h in headers] for rec in records]
    return TableData(title=title, headers=headers, rows=rows)


def summary_table(title: str, fields: Dict[str, Any]) -> TableData:
    """Build a simple two-column Field/Value table for scalar metadata
    (e.g. strategy_id, total_allocation) that isn't itself a list of
    records."""
    rows = [[k, format_value(v)] for k, v in fields.items()]
    return TableData(title=title, headers=["Field", "Value"], rows=rows)


def csv_to_table(title: str, filepath: Path) -> Optional[TableData]:
    """Convert a CSV file directly into a TableData, preserving the
    original cell text as-is (no reformatting of financial figures)."""
    with open(filepath, newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return None
        rows = [row for row in reader]

    return TableData(title=title, headers=headers, rows=rows)


# --------------------------------------------------------------------------
# Placeholder processing functions
#   File on disk in -> List[TableData] out. No printing, no text/HTML here.
# --------------------------------------------------------------------------

def process_positions_file(filepath: Path) -> List[TableData]:
    """
    Parse a "*positions*" JSON file. Observed shape:
        {
          "strategy_id": ...,
          "positions": [ {...}, ... ],        -> current position snapshot
          "position_detail": [ {...}, ... ],  -> trade-level position changes
          "allocations": [ {...}, ... ],      -> per-account allocation/cash
          "total_allocation": ...
        }
    """
    with open(filepath) as f:
        data = json.load(f)

    tables: List[TableData] = [
        summary_table(
            f"Summary -- {filepath.name}",
            {
                "strategy_id": data.get("strategy_id"),
                "total_allocation": data.get("total_allocation"),
            },
        )
    ]

    for title, key in (
        ("Current Positions", "positions"),
        ("Position Detail (Trade-Level)", "position_detail"),
        ("Account Allocations", "allocations"),
    ):
        t = records_to_table(title, data.get(key, []))
        if t is not None:
            tables.append(t)

    return tables


def process_orders_file(filepath: Path) -> List[TableData]:
    """Parse a "*orders*" JSON file. Observed shape: a bare JSON array of
    flat order records."""
    with open(filepath) as f:
        data = json.load(f)

    records = data if isinstance(data, list) else data.get("orders", [])
    t = records_to_table(f"Orders -- {filepath.name}", records)
    return [t] if t is not None else []


def process_trades_file(filepath: Path) -> List[TableData]:
    """
    Parse a "*trades*" JSON file. Observed shape:
        {
          "strategy_id": ...,
          "trades": [ {...}, ... ],       -> individual fills
          "allocations": [ {...}, ... ],  -> per-account allocation/cash
          "total_allocation": ...
        }
    """
    with open(filepath) as f:
        data = json.load(f)

    tables: List[TableData] = [
        summary_table(
            f"Summary -- {filepath.name}",
            {
                "strategy_id": data.get("strategy_id"),
                "total_allocation": data.get("total_allocation"),
            },
        )
    ]

    for title, key in (
        ("Trades", "trades"),
        ("Account Allocations", "allocations"),
    ):
        t = records_to_table(title, data.get(key, []))
        if t is not None:
            tables.append(t)

    return tables


def process_pnl_file(filepath: Path) -> List[TableData]:
    """Parse a "*pnl*" CSV file into a single table."""
    t = csv_to_table(f"PnL -- {filepath.name}", filepath)
    return [t] if t is not None else []


# --------------------------------------------------------------------------
# Report builder -- renders TableData (and plain text) into TXT + HTML
# --------------------------------------------------------------------------

class ReportBuilder:
    """
    Accumulates a report in two parallel formats as the report is walked
    once: `text_lines` (for the .txt file / console) and `html_parts`
    (for the .html file). Every method writes to both, so callers never
    have to think about format -- they just describe report structure.
    """

    def __init__(self, echo_to_console: bool = True) -> None:
        self.text_lines: List[str] = []
        self.html_parts: List[str] = []
        self.echo_to_console = echo_to_console

    def text(self, line: str = "") -> None:
        if self.echo_to_console:
            print(line)
        self.text_lines.append(line)

    def title_banner(self, title: str) -> None:
        self.text("#" * 80)
        self.text(title)
        self.text("#" * 80)
        self.html_parts.append(f"<h1 class='report-title'>{html.escape(title)}</h1>")

    def child_banner(self, name: str) -> None:
        self.text("")
        self.text(f">>> {name}")
        self.html_parts.append(f"<h1 class='child'>{html.escape(name)}</h1>")

    def section_header(self, title: str) -> None:
        self.text("")
        self.text("=" * 80)
        self.text(title)
        self.text("=" * 80)
        self.html_parts.append(f"<h2 class='section'>{html.escape(title)}</h2>")

    def plain(self, message: str) -> None:
        self.text(f"    {message}")
        self.html_parts.append(f"<p class='empty'>{html.escape(message)}</p>")

    def table(self, table_data: TableData) -> None:
        pt = PrettyTable()
        pt.field_names = table_data.headers
        for row in table_data.rows:
            pt.add_row(row)

        self.text(f"-- {table_data.title} --")
        self.text(pt.get_string())
        if table_data.notes:
            self.text(f"    {table_data.notes}")

        self.html_parts.append(f"<h3>{html.escape(table_data.title)}</h3>")
        self.html_parts.append(pt.get_html_string(attributes={"class": "data-table"}))
        if table_data.notes:
            self.html_parts.append(f"<p class='note'>{html.escape(table_data.notes)}</p>")

    def raw_block(self, title: str, lines: List[str]) -> None:
        """For unstructured content (raw log tail) -- printed as-is in
        text, wrapped in <pre> for HTML."""
        self.text(f"-- {title} --")
        self.text("-" * 80)
        for line in lines:
            self.text(line)
        self.text("-" * 80)

        self.html_parts.append(f"<h3>{html.escape(title)}</h3>")
        escaped = html.escape("\n".join(lines))
        self.html_parts.append(f"<pre class='log-block'>{escaped}</pre>")

    def finalize_html(self, report_title: str) -> str:
        css = """
        body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #1a1a1a; }
        h1.report-title { border-bottom: 3px solid #222; padding-bottom: 8px; }
        h1.child { margin-top: 2.5rem; border-bottom: 2px solid #333; padding-bottom: 4px; }
        h2.section { color: #444; margin-top: 1.5rem; }
        h3 { color: #666; margin-bottom: 0.25rem; }
        table.data-table { border-collapse: collapse; margin-bottom: 1rem; font-size: 0.9rem; }
        table.data-table th, table.data-table td { border: 1px solid #ccc; padding: 4px 10px; text-align: left; }
        table.data-table th { background: #f0f0f0; }
        pre.log-block { background: #f7f7f7; padding: 0.75rem; overflow-x: auto; font-size: 0.85rem; }
        p.note { font-style: italic; color: #777; }
        p.empty { color: #999; }
        """
        body = "\n".join(self.html_parts)
        return (
            "<!DOCTYPE html><html><head><meta charset='utf-8'>"
            f"<title>{html.escape(report_title)}</title><style>{css}</style></head>"
            f"<body>{body}</body></html>"
        )


# --------------------------------------------------------------------------
# Report sections
# --------------------------------------------------------------------------

def report_logs(
    rb: ReportBuilder,
    children: List[Path],
    tail_lines: int,
    date_filter: Optional[str] = None,
) -> None:
    """Section 1: for every immediate child of root, report on the
    child's "logs" directory (if any). If `date_filter` is given, each log
    source is restricted to the file matching that date instead of its
    most recently modified file."""
    for child in children:
        rb.child_banner(child.name)
        rb.section_header("LOGS")

        logs_dir = child / "logs"
        if not logs_dir.is_dir():
            rb.plain("(no 'logs' directory found)")
            continue

        source_dirs = find_log_source_dirs(logs_dir)
        if not source_dirs:
            rb.plain("(no log files found under 'logs')")
            continue

        for source_dir in source_dirs:
            label = source_dir.relative_to(logs_dir)
            label_str = str(label) if str(label) != "." else logs_dir.name

            target = find_target_file(source_dir, date_filter=date_filter)
            if target is None:
                if date_filter:
                    rb.plain(f"    Log source: {label_str} -- no file found for date '{date_filter}'")
                else:
                    rb.plain(f"    Log source: {label_str} -- no log files found")
                continue

            rb.text(f"    Log source: {label_str}")
            rb.text(f"    {'Selected' if date_filter else 'Most current'} file: {target.name}")
            rb.raw_block(
                f"Last {tail_lines} lines of {target.name}",
                tail_file(target, tail_lines),
            )


def report_positions_and_trading_activity(
    rb: ReportBuilder,
    children: List[Path],
    date_filter: Optional[str] = None,
) -> None:
    """Section 2: for every immediate child of root, report on the
    child's "positions" and "trades" directories (if any). If
    `date_filter` is given, each of positions/pnl/orders/trades is
    restricted to the file matching that date instead of its most
    recently modified file."""
    selected_label = "Selected" if date_filter else "Most current"

    def not_found_message(pattern: str) -> str:
        if date_filter:
            return f"(no '*{pattern}*' file found for date '{date_filter}')"
        return f"(no '*{pattern}*' file found)"

    for child in children:
        rb.child_banner(child.name)

        # ---- POSITIONS -------------------------------------------------
        rb.section_header("POSITIONS")
        positions_dir = child / "positions"
        if positions_dir.is_dir():
            target = find_target_file(positions_dir, name_contains="positions", date_filter=date_filter)
            if target is not None:
                rb.text(f"    {selected_label} positions file: {target.name}")
                for t in process_positions_file(target):
                    rb.table(t)
            else:
                rb.plain(not_found_message("positions"))
        else:
            rb.plain("(no 'positions' directory found)")

        # ---- TRADING ACTIVITY -------------------------------------------
        rb.section_header("TRADING ACTIVITY")
        trades_dir = child / "trades"
        if trades_dir.is_dir():
            pnl_target = find_target_file(trades_dir, name_contains="pnl", date_filter=date_filter)
            if pnl_target is not None:
                rb.text(f"    {selected_label} pnl file: {pnl_target.name}")
                for t in process_pnl_file(pnl_target):
                    rb.table(t)
            else:
                rb.plain(not_found_message("pnl"))

            orders_target = find_target_file(trades_dir, name_contains="orders", date_filter=date_filter)
            if orders_target is not None:
                rb.text(f"    {selected_label} orders file: {orders_target.name}")
                for t in process_orders_file(orders_target):
                    rb.table(t)
            else:
                rb.plain(not_found_message("orders"))

            trades_target = find_target_file(trades_dir, name_contains="trades", date_filter=date_filter)
            if trades_target is not None:
                rb.text(f"    {selected_label} trades file: {trades_target.name}")
                for t in process_trades_file(trades_target):
                    rb.table(t)
            else:
                rb.plain(not_found_message("trades"))
        else:
            rb.plain("(no 'trades' directory found)")


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a portfolio trading operations report.")
    parser.add_argument(
        "--root",
        default=DEFAULT_ROOT,
        help=f"Root portfolio directory (default: {DEFAULT_ROOT})",
    )
    parser.add_argument(
        "--tail-lines",
        type=int,
        default=DEFAULT_TAIL_LINES,
        help=f"Number of log lines to tail per log source (default: {DEFAULT_TAIL_LINES})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write portfolio_report.txt / .html into (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress console echo; still writes the .txt and .html files.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Restrict the report to files whose filename contains this date string "
            "(e.g. --date 20260702), instead of picking the most recently modified "
            "file. Applies to logs, positions, pnl, orders, and trades files alike."
        ),
    )
    args = parser.parse_args()

    if args.date and not re.fullmatch(r"\d{8}", args.date):
        print(
            f"Warning: --date '{args.date}' doesn't look like an 8-digit YYYYMMDD "
            "date. Proceeding anyway -- it will be matched as a literal substring "
            "against filenames."
        )

    root = Path(args.root)
    if not root.is_dir():
        raise SystemExit(f"Root directory not found: {root}")

    children = get_immediate_child_dirs(root)
    if not children:
        raise SystemExit(f"No child directories found under: {root}")

    title = f"PORTFOLIO TRADING OPERATIONS REPORT -- root: {root}"
    if args.date:
        title += f" -- date: {args.date}"

    rb = ReportBuilder(echo_to_console=not args.quiet)
    rb.title_banner(title)

    # 1. LOGS -- one full pass over all children
    report_logs(rb, children, args.tail_lines, date_filter=args.date)

    # 2. POSITIONS / TRADING ACTIVITY -- second full pass over all children
    report_positions_and_trading_activity(rb, children, date_filter=args.date)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"_{args.date}" if args.date else ""
    txt_path = output_dir / f"portfolio_report{suffix}.txt"
    html_path = output_dir / f"portfolio_report{suffix}.html"

    txt_path.write_text("\n".join(rb.text_lines) + "\n")
    html_path.write_text(rb.finalize_html(title))

    print()
    print(f"Text report written to:  {txt_path}")
    print(f"HTML report written to:  {html_path}")


if __name__ == "__main__":
    main()
