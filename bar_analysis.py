"""
bar_analysis.py — Per-file 10-minute bar analysis.

For each 1-minute CSV in a given directory:
  • Converts to 10-minute bars via BarAggregator
  • Extracts the first bar's open and close
  • Computes the average close of the first 3 bars
  • Computes percentage deltas from the first open and first close to that avg
  • Writes one summary row per file to an output CSV

After all files are processed:
  • Computes mean and standard deviation for both percentage-difference series
  • Builds a 10-bin histogram for each series
  • Appends the statistics and histogram to the output CSV

Usage:
    python bar_analysis.py                              # defaults: ./uploads → ./output.csv
    python bar_analysis.py /path/to/data                # custom input dir
    python bar_analysis.py /path/to/data results.csv    # custom input dir + output file
"""

import csv
import math
import sys
from pathlib import Path
from typing import List, Tuple

from bar_aggregator import BarAggregator


# ------------------------------------------------------------------
# Stats helpers (no numpy dependency)
# ------------------------------------------------------------------
def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _stdev(values: List[float], ddof: int = 1) -> float:
    """Sample standard deviation (ddof=1) by default."""
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    return math.sqrt(sum((v - m) ** 2 for v in values) / (len(values) - ddof))


def _histogram(values: List[float], num_bins: int = 10) -> List[Tuple[float, float, int]]:
    """
    Return a list of (bin_low, bin_high, count) tuples.
    Bins span [min, max] evenly; the last bin is inclusive on both ends.
    """
    if not values:
        return []

    lo, hi = min(values), max(values)
    if lo == hi:
        return [(lo, hi, len(values))]

    width = (hi - lo) / num_bins
    bins = [(lo + i * width, lo + (i + 1) * width, 0) for i in range(num_bins)]

    for v in values:
        idx = int((v - lo) / width)
        if idx >= num_bins:
            idx = num_bins - 1
        low_edge, high_edge, cnt = bins[idx]
        bins[idx] = (low_edge, high_edge, cnt + 1)

    return bins


def _pct_diff(base: float, target: float) -> float:
    """Percentage change from *base* to *target*."""
    if base == 0:
        return 0.0
    return ((target - base) / base) * 100.0


# ------------------------------------------------------------------
# Per-file analysis
# ------------------------------------------------------------------
def analyse_file(filepath: Path, bar_minutes: int = 10) -> dict:
    agg = BarAggregator(bar_minutes=bar_minutes)
    agg.load_file(filepath)
    agg.finalise()

    n = len(agg)
    if n == 0:
        raise ValueError(f"No bars produced from {filepath}")

    first_bar = agg[n - 1]
    first_open = first_bar.open
    first_close = first_bar.close

    bars_for_avg = min(3, n)
    avg_close_3 = sum(agg[n - 1 - i].close for i in range(bars_for_avg)) / bars_for_avg

    pct_open_to_avg = _pct_diff(first_open, avg_close_3)
    pct_close_to_avg = _pct_diff(first_close, avg_close_3)

    return {
        "file": filepath.name,
        "first_bar_ts": first_bar.timestamp,
        "first_open": first_open,
        "first_close": first_close,
        "avg_close_3": round(avg_close_3, 4),
        "pct_open_to_avg": round(pct_open_to_avg, 4),
        "pct_close_to_avg": round(pct_close_to_avg, 4),
    }


# ------------------------------------------------------------------
# Print a histogram to the console
# ------------------------------------------------------------------
def _print_histogram(label: str, bins: List[Tuple[float, float, int]], total: int) -> None:
    max_count = max(c for _, _, c in bins) if bins else 0
    bar_max = 40  # max width in characters

    print(f"\n  {label}")
    print(f"  {'Bin Range':>24s}  {'Count':>5s}  Distribution")
    print(f"  {'─' * 24}  {'─' * 5}  {'─' * bar_max}")
    for lo, hi, cnt in bins:
        bar_len = int((cnt / max_count) * bar_max) if max_count > 0 else 0
        print(f"  [{lo:+10.4f}, {hi:+10.4f})  {cnt:5d}  {'█' * bar_len}")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
DATA_FIELDNAMES = [
    "file", "first_bar_ts", "first_open", "first_close",
    "avg_close_3", "pct_open_to_avg", "pct_close_to_avg",
]


def run(input_dir: str, output_file: str) -> None:
    input_path = Path(input_dir)
    if not input_path.is_dir():
        print(f"Error: {input_dir} is not a directory")
        sys.exit(1)

    csv_files = sorted(input_path.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        sys.exit(1)

    # ---- per-file analysis ----
    results = []
    for fp in csv_files:
        row = analyse_file(fp, bar_minutes=10)
        results.append(row)
        print(
            f"{row['file']:30s}  open={row['first_open']:<10}  "
            f"close={row['first_close']:<10}  "
            f"avg3={row['avg_close_3']:<12}  "
            f"Δopen%={row['pct_open_to_avg']:+.4f}%  "
            f"Δclose%={row['pct_close_to_avg']:+.4f}%"
        )

    # ---- aggregate statistics ----
    pct_open_vals = [r["pct_open_to_avg"] for r in results]
    pct_close_vals = [r["pct_close_to_avg"] for r in results]

    mean_open = _mean(pct_open_vals)
    std_open = _stdev(pct_open_vals)
    mean_close = _mean(pct_close_vals)
    std_close = _stdev(pct_close_vals)

    print(f"\n{'═' * 60}")
    print(f"  pct_open_to_avg   — mean: {mean_open:+.4f}%   stdev: {std_open:.4f}%")
    print(f"  pct_close_to_avg  — mean: {mean_close:+.4f}%   stdev: {std_close:.4f}%")

    # ---- conditional breakdowns ----
    total = len(results)

    open_above = [r["pct_open_to_avg"] for r in results if r["pct_open_to_avg"] > 0]
    open_below = [r["pct_open_to_avg"] for r in results if r["pct_open_to_avg"] < 0]
    close_above = [r["pct_close_to_avg"] for r in results if r["pct_close_to_avg"] > 0]
    close_below = [r["pct_close_to_avg"] for r in results if r["pct_close_to_avg"] < 0]

    avg_open_above = _mean(open_above) if open_above else 0.0
    avg_open_below = _mean(open_below) if open_below else 0.0
    pct_open_below_occ = (len(open_below) / total) * 100.0

    avg_close_above = _mean(close_above) if close_above else 0.0
    avg_close_below = _mean(close_below) if close_below else 0.0
    pct_close_below_occ = (len(close_below) / total) * 100.0

    print(f"\n{'─' * 60}")
    print(f"  Open vs 3-bar avg breakdown:")
    print(f"    avg %diff where open > 3-bar avg:  {avg_open_above:+.4f}%  (n={len(open_above)})")
    print(f"    avg %diff where open < 3-bar avg:  {avg_open_below:+.4f}%  (n={len(open_below)})")
    print(f"    % of days  where open < 3-bar avg: {pct_open_below_occ:.1f}%")

    print(f"\n  Close vs 3-bar avg breakdown:")
    print(f"    avg %diff where close > 3-bar avg: {avg_close_above:+.4f}%  (n={len(close_above)})")
    print(f"    avg %diff where close < 3-bar avg: {avg_close_below:+.4f}%  (n={len(close_below)})")
    print(f"    % of days  where close < 3-bar avg:{pct_close_below_occ:.1f}%")

    print(f"\n  Total count: {total}")

    hist_open = _histogram(pct_open_vals, num_bins=10)
    hist_close = _histogram(pct_close_vals, num_bins=10)

    _print_histogram("pct_open_to_avg histogram", hist_open, len(pct_open_vals))
    _print_histogram("pct_close_to_avg histogram", hist_close, len(pct_close_vals))

    # ---- write output CSV (per-file data only) ----
    out = Path(output_file)
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=DATA_FIELDNAMES)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWrote {len(results)} data rows to {out}")


if __name__ == "__main__":
    default_dir = "/Users/jcarter/hannibal/dev/upro_samples"
    default_out = "bar_analysis_output.csv"

    dir_arg = sys.argv[1] if len(sys.argv) > 1 else default_dir
    out_arg = sys.argv[2] if len(sys.argv) > 2 else default_out

    run(dir_arg, out_arg)
