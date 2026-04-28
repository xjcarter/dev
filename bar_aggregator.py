"""
BarAggregator — Aggregates 1-minute stock price snapshots into OHLCV bars
of configurable time-window length.

Usage:
    from bar_aggregator import BarAggregator

    agg = BarAggregator(bar_minutes=5)
    agg.load_files(['NVDA_20260102.csv', 'NVDA_20260105.csv'])

    latest_bar = agg[0]        # most recent *complete* bar
    current_bar = agg[-1]      # incomplete bar currently being built
    prev_bar = agg[1]          # second most recent complete bar
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, List, Optional, Sequence, Union
import logging

import os
import re
from typing import Iterator

# Create a logger specific to __main__ module
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Bar dataclass
# ---------------------------------------------------------------------------
@dataclass
class Bar:
    """A single OHLCV bar with a count of contributing snapshots."""

    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0
    count: int = 0

    # Convenience for display / debugging
    timestamp: str = ""  # "YYYYMMDD-HH:MM" of the bar's start

    # indicator mappings
    _indicators: dict[str, float | None] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> 'Bar':
        core_fields = {'open', 'high', 'low', 'close', 'volume', 'count', 'timestamp', 'date', 'time'}
        
        core_kwargs = {}
        # Force float conversion for price fields
        for field in ['open', 'high', 'low', 'close']:
            if field in data:
                core_kwargs[field] = float(data[field])  # Convert string to float
        
        # Force int conversion for volume/count
        for field in ['volume', 'count']:
            if field in data:
                core_kwargs[field] = int(data[field])    # Convert string to int
        
        # NOTE ignoring 'date' and 'time' columns
        # they have been added as nice-to-haves in 'bar data' output, 
        # but they gum up operations as start-up input (not true annotated data)
        if 'timestamp' in data:
            core_kwargs['timestamp'] = str(data['timestamp'])
        
        instance = cls(**core_kwargs)
        
        # Convert indicator values to numbers when possible
        instance._indicators = {}
        for key, value in data.items():
            if key not in core_fields:
                # Try to convert to float if possible
                if isinstance(value, (int, float)):
                    instance._indicators[key] = value
                elif isinstance(value, str):
                    try:
                        instance._indicators[key] = float(value)  # Convert string to float
                    except ValueError:
                        instance._indicators[key] = value  # Keep as string if not numeric
                else:
                    instance._indicators[key] = value

        return instance

    def annotate(self, new_data: dict) -> None:
        if self._indicators is None:
            self._indicators = dict()
        try:
            self._indicators.update(new_data)
        except:
            logger.warning(f'Annotation to bar data failed.')

    def _dump_indicators(self) -> str:
        s = []
        if self._indicators is not None:
            for k, v in self._indicators.items():
                s.append(f'{k}: {v:.3f}')
            return ", ".join(s)
        return ""

    def __str__(self) -> str:
        return (
            f"Bar(ts={self.timestamp!r}, O={self.open}, H={self.high}, "
            f"L={self.low}, C={self.close}, V={self.volume}, n={self.count}), "
            f"Indicators({self._dump_indicators()})"
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return self.__str__()


# ---------------------------------------------------------------------------
# Internal accumulator used while a bar is still being built
# ---------------------------------------------------------------------------
@dataclass
class _BarAccumulator:
    """Mutable workspace for assembling a bar from successive snapshots."""

    open: float = 0.0
    high: float = float("-inf")
    low: float = float("inf")
    close: float = 0.0
    volume_values: list = field(default_factory=list)
    count: int = 0
    timestamp: str = ""

    def update(self, price: float, volume: float) -> None:
        if self.count == 0:
            self.open = price
            self.high = price
            self.low = price
        else:
            if price > self.high:
                self.high = price
            if price < self.low:
                self.low = price
        self.close = price
        self.volume_values.append(volume)
        self.count += 1

    def to_bar(self, volume_fn: Callable[[List[float]], int]) -> Bar:
        return Bar(
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=volume_fn(self.volume_values),
            count=self.count,
            timestamp=self.timestamp,
        )


# ---------------------------------------------------------------------------
# Default volume aggregation: average of all snapshots in the bin
# ---------------------------------------------------------------------------
def _default_volume_fn(volumes: List[float]) -> int:
    """Return the average of *volumes*, rounded to int."""
    if not volumes:
        return 0
    return int(round(sum(volumes) / len(volumes)))


# ---------------------------------------------------------------------------
# Helpers: parse a time string "HH:MM:SS" or "HH:MM" → "HH:MM"
# ---------------------------------------------------------------------------
def _truncate_to_minute(time_str: str) -> str:
    """Drop seconds → 'HH:MM'."""
    parts = time_str.split(":")
    return f"{parts[0]}:{parts[1]}"


def _time_to_minutes(hhmm: str) -> int:
    """Convert 'HH:MM' → total minutes since midnight."""
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def _minutes_to_hhmm(total: int) -> str:
    """Convert total minutes since midnight → 'HH:MM'."""
    return f"{total // 60:02d}:{total % 60:02d}"


def _bar_start(minute_of_day: int, bar_minutes: int) -> int:
    """Return the bin-start minute for *minute_of_day*."""
    return (minute_of_day // bar_minutes) * bar_minutes


# ---------------------------------------------------------------------------
# CSV / dict row normalisation
# ---------------------------------------------------------------------------

# Canonical column order expected from the sample CSVs
_CSV_COLUMNS = [
    "date", "time", "_updated", "last", "bid", "ask",
    "bid_sz", "ask_sz", "volume", "symbol", "conid",
]


def _parse_row(raw: Union[str, dict]) -> dict:
    """
    Accept either:
      • a CSV-formatted string (with or without a trailing newline)
      • a dict / JSON-style mapping with the same keys
    and return a normalised dict.
    """
    if isinstance(raw, dict):
        return raw

    if isinstance(raw, str):
        # Try JSON first
        stripped = raw.strip()
        if stripped.startswith("{"):
            return json.loads(stripped)

        # Otherwise treat as a CSV line
        reader = csv.reader([stripped])
        values = next(reader)
        if len(values) != len(_CSV_COLUMNS):
            raise ValueError(
                f"Expected {len(_CSV_COLUMNS)} CSV columns, got {len(values)}: {stripped!r}"
            )
        return dict(zip(_CSV_COLUMNS, values))

    raise TypeError(f"push() expects str or dict, got {type(raw).__name__}")


# ---------------------------------------------------------------------------
# Main aggregator
# ---------------------------------------------------------------------------
class BarAggregator:
    """
    Aggregates 1-minute stock-price snapshots into OHLCV bars of a
    configurable *bar_minutes* window.

    Parameters
    ----------
    bar_minutes : int
        Width of each bar in minutes (default 5).
    volume_fn : callable, optional
        ``f(list[float]) -> int``  — reduces per-snapshot volume values into a
        single bar volume.  Default: arithmetic mean.
    """

    def __init__(
        self,
        bar_minutes: int = 5,
        volume_fn: Optional[Callable[[List[float]], int]] = None,
        indicator_set = None
    ) -> None:
        if bar_minutes < 1:
            raise ValueError("bar_minutes must be >= 1")

        self.bar_minutes: int = bar_minutes
        self.volume_fn: Callable[[List[float]], int] = volume_fn or _default_volume_fn

        # Completed bars stored newest-first so that index 0 = most recent
        self._bars: List[Bar] = []

        # The bar currently being assembled (may be incomplete)
        self._current: Optional[_BarAccumulator] = None
        self._current_bin_key: Optional[str] = None  # "YYYYMMDD-HH:MM"

        # object that holds indicators, and other calcs to
        # annotate the bars created
        self.indicator_set = indicator_set

                
    # ------------------------------------------------------------------
    # annotate() — attach indicators values (Optional) 
    # ------------------------------------------------------------------
    def annotate(self, bar: 'Bar') -> 'Bar':
        try:
            # add on indicator calculations if warranted
            if self.indicator_set is not None:
                bar = self.indicator_set.run_indicators(bar)
        except:
            logger.critical(f'Bar annotation failed. Bar:{bar}')

        return bar

    # ------------------------------------------------------------------
    # push() — primary ingestion entry point
    # ------------------------------------------------------------------
    def push(self, row: Union[str, dict]) -> Optional['Bar']:
        """
        Ingest a single 1-minute snapshot.
        

        *row* may be:
          • a CSV-formatted string (matching the sample column layout)
          • a ``dict`` (or JSON string) with at least ``date``, ``time``,
            ``last``, and ``volume`` keys.
        """
        
        rec = _parse_row(row)

        date_str = str(rec["date"]).strip()
        time_hhmm = _truncate_to_minute(str(rec["time"]).strip())
        price = float(rec["last"])
        vol_raw = rec.get("volume", 0)
        volume = float(vol_raw) if vol_raw not in (None, "", " ") else 0.0

        minute_of_day = _time_to_minutes(time_hhmm)
        bin_start = _bar_start(minute_of_day, self.bar_minutes)
        bin_hhmm = _minutes_to_hhmm(bin_start)
        bin_key = f"{date_str}-{bin_hhmm}"

        # Same bin as current accumulator → keep accumulating
        if self._current is not None and bin_key == self._current_bin_key:
            self._current.update(price, volume)
            return None

        current_bar = None

        # New bin → finalise the previous accumulator (if any) and start fresh
        if self._current is not None and self._current.count > 0:
            bar = self._current.to_bar(self.volume_fn)
            bar = self.annotate(bar)
            self._bars.insert(0, bar)  # prepend so index 0 = newest
            current_bar = Bar(bar)

        self._current = _BarAccumulator(timestamp=bin_key)
        self._current_bin_key = bin_key
        self._current.update(price, volume)

        # return newest bar (complete with annotations!)
        return current_bar 

    # ------------------------------------------------------------------
    # Finalise — call when you know no more data is coming
    # ------------------------------------------------------------------
    def finalise(self) -> None:
        """
        Promote the current in-progress bar to a completed bar.

        Useful after all files have been loaded so the last partial window
        is not silently discarded.
        """
        if self._current is not None and self._current.count > 0:
            bar = self._current.to_bar(self.volume_fn)
            bar = self.annotate(bar)
            self._bars.insert(0, bar)
            self._current = None
            self._current_bin_key = None

    # ------------------------------------------------------------------
    # Indexing: [0] = newest complete, [-1] = incomplete/current
    # ------------------------------------------------------------------
    def __getitem__(self, index: int) -> Bar:
        if index == -1:
            if self._current is None or self._current.count == 0:
                raise IndexError("No incomplete bar currently in progress")
            return self._current.to_bar(self.volume_fn)
        if index < 0:
            raise IndexError(
                "Negative indices other than -1 are not supported. "
                "Use 0 for the most recent complete bar, 1 for the one before, etc."
            )
        if index >= len(self._bars):
            raise IndexError(
                f"Bar index {index} out of range (only {len(self._bars)} complete bars)"
            )
        return self._bars[index]

    # ------------------------------------------------------------------
    # Iteration & length  (iterate newest → oldest over complete bars)
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        """Number of *completed* bars."""
        return len(self._bars)

    def __iter__(self) -> Iterator[Bar]:
        """Iterate over completed bars, oldest first."""
        flipped = self._bars[::-1]
        return iter(flipped)

    # ------------------------------------------------------------------
    # load_checkpoint — initial setup of new bar
    # ------------------------------------------------------------------

    def load_checkpoint(self, filepath: Union[str, Path]) -> None:
        """
            Grab the checkpoint file that will initialize the BarAggregator
            The checkpoint file is a CSV file holding completed Bar Object
            and annotated data (indicators, etc.) that will 'start' the 
            self._bar series

            Bar data is organized where timestamps are ordered in the file 
            from the oldest bar to newest bar. Therefore we need to reverse the
            list when indexing it in code.
        """

        ## initialize the indicators to continue building their timeseries
        if self.indicator_set is None:
            logger.critical(f'Cannot load checkpoint- no indicator set given')
            return

        # clear the indicators 
        self.indicator_set.reset()
        filepath = Path(filepath)
        with filepath.open(newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                bar = Bar.from_dict(row) 
                """
                    load checkpoint bars into indicator history
                    NOTE THIS IS ONLY POPULATING INDICATORS
                    TO CARRY CALCULATIONS FORWARD. 
                    this sets the foundation for new indicator
                    calculations as new bars are created.
                """
                self.annotate(bar)

    # ------------------------------------------------------------------
    # Create data checkpoint -  collect most recent history to carry forward
    # for continued indicator calculations on the next day.
    # history_needed = number of 'days back' of bar data needed to 
    # continue poplating any running annotations (indicators, etc)
    # ------------------------------------------------------------------
    def write_checkpoint(self, filepath: Union[str, Path]) -> None:

        if self.indicator_set is None:
            logger.critical(f'Cannot write checkpoint- no indicator set given')
            return

        bar_history = list(self._bars[:self.indicator_set.history_needed])
        bar_history.reverse()

        filepath = Path(filepath)
        with filepath.open("w", newline="") as fh:
            writer = csv.writer(fh)
            header = [ 
                "timestamp", "date", "time",
                "open", "high", "low", "close", "volume", "count",
                ]
            annotated = False
            try:
                header.extend(self._bars[0]._indicators.keys())
                annotated = True
            except:
                pass

            writer.writerow(header)
            for bar in bar_history:
                # Split "YYYYMMDD-HH:MM" → date, time columns
                parts = bar.timestamp.split("-", 1)
                date_part = parts[0] if len(parts) == 2 else ""
                time_part = parts[1] if len(parts) == 2 else bar.timestamp
                data_row = [
                    bar.timestamp, date_part, time_part,
                    bar.open, bar.high, bar.low, bar.close,
                    bar.volume, bar.count,
                ]
                if annotated:
                    data_row.extend(bar._indicators.values())
                writer.writerow(data_row)


    # ------------------------------------------------------------------
    # Bulk CSV loading helpers
    # ------------------------------------------------------------------
    def load_file(self, filepath: Union[str, Path]) -> None:
        """Read a single CSV file and push every data row."""

        filepath = Path(filepath)
        with filepath.open(newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                self.push(row)


    def load_files(self, filepaths: Sequence[Union[str, Path]]) -> None:
        """
        Read multiple CSV files in order and push their rows.

        Files are expected to be in ascending chronological order.
        """
        for fp in filepaths:
            self.load_file(fp)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------
    @property
    def complete_bars(self) -> List[Bar]:
        """Return a copy of all completed bars (newest first)."""
        return list(self._bars)

    @property
    def current_bar(self) -> Optional[Bar]:
        """Return the in-progress bar, or None if there isn't one."""
        if self._current is not None and self._current.count > 0:
            return self._current.to_bar(self.volume_fn)
        return None

    def __str__(self) -> str:
        cur = "yes" if self._current and self._current.count > 0 else "no"
        return (
            f"BarAggregator(bar_minutes={self.bar_minutes}, "
            f"complete_bars={len(self._bars)}, in_progress={cur})"
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return self.__str__()


    # ------------------------------------------------------------------
    # Save aggregated bars to CSV
    # ------------------------------------------------------------------
    def save(
        self,
        filepath: Union[str, Path],
        include_incomplete: bool = False,
        chronological: bool = True,
        history: int = 0
    ) -> Path:
        """
        Save the aggregated bar series to a CSV file.
 
        Parameters
        ----------
        filepath : str or Path
            Destination file path (e.g. ``"NVDA_5min.csv"``).
        include_incomplete : bool
            If ``True``, append the in-progress bar (index -1) as the last
            row.  Default ``False`` — only completed bars are written.
        chronological : bool
            If ``True`` (default), rows are written oldest → newest
            (ascending time).  If ``False``, newest → oldest (the same
            order as iteration / indexing).
 
        Returns
        -------
        Path
            The resolved path of the written file.
        """
        filepath = Path(filepath)
 
        bars: List[Bar] = list(self._bars)  # newest-first copy
 
        if chronological:
            bars = bars[::-1]  # flip to oldest-first

        if include_incomplete:
            current = self.current_bar
            current = self.annotate(current)
            if current is not None:
                if chronological:
                    bars.append(current)      # newest last
                else:
                    bars.insert(0, current)   # newest first

        # only save the last 'history' bars
        if history > 0:
            bars = bars[:history]
 
        with filepath.open("w", newline="") as fh:
            writer = csv.writer(fh)
            header = [ 
                "timestamp", "date", "time",
                "open", "high", "low", "close", "volume", "count",
                ]
            annotated = False
            try:
                header.extend(self._bars[0]._indicators.keys())
                annotated = True
            except:
                pass

            writer.writerow(header)
            for bar in bars:
                # Split "YYYYMMDD-HH:MM" → date, time columns
                parts = bar.timestamp.split("-", 1)
                date_part = parts[0] if len(parts) == 2 else ""
                time_part = parts[1] if len(parts) == 2 else bar.timestamp
                data_row = [
                    bar.timestamp, date_part, time_part,
                    bar.open, bar.high, bar.low, bar.close,
                    bar.volume, bar.count,
                ]
                if annotated:
                    data_row.extend(bar._indicators.values())
                writer.writerow(data_row)

# ------------------------------------------------------------------
# Helper function to capture data files
# ------------------------------------------------------------------

def get_filtered_filenames(directory: str, symbol: str) -> Iterator[str]:
    """
    Extract filenames for a specific symbol, yielding them in ascending order.
    
    Note: This loads all matching filenames into memory to sort them using
    Python's Timsort algorithm (O(n log n) time, O(n) memory).
    
    For directories with millions of files, consider using external sorting
    or processing files in chunks.
    
    Args:
        directory: Path to the directory to scan
        symbol: Stock symbol to filter (e.g., 'NVDA', 'AAPL')
    
    Returns:
        Iterator yielding matching filenames in ascending order
        
    Example:
        >>> files = get_filtered_filenames('/data', 'NVDA')
        >>> next(files)
        'NVDA.20260101.csv'
    """
    pattern = re.compile(rf'^{re.escape(symbol)}\.(\d{{8}})\.csv$')
    
    try:
        with os.scandir(directory) as entries:
            # Collect all matching files (O(n) time, O(n) memory)
            matching_files = [
                entry.name for entry in entries
                if entry.is_file() and pattern.match(entry.name)
            ]
        
        # Sort using Timsort (O(n log n) time, O(n) memory)
        matching_files.sort()
        
        # Return iterator (lazy evaluation, but data already in memory)
        return iter(matching_files)
        
    except FileNotFoundError:
        raise FileNotFoundError(f"Directory not found: {directory}")
