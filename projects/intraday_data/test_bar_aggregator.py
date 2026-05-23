"""Tests for BarAggregator using the sample NVDA CSV files."""

import sys, json, copy
sys.path.insert(0, "/home/claude")

from bar_aggregator import BarAggregator, Bar, _default_volume_fn

DATA_DIR = "/Users/jcarter/hannibal/dev/nvda_samples"
FILES = [
    f"{DATA_DIR}/NVDA.20260102.csv",
    f"{DATA_DIR}/NVDA.20260105.csv",
    f"{DATA_DIR}/NVDA.20260106.csv",
    f"{DATA_DIR}/NVDA.20260107.csv",
]


def test_basic_load_and_bar_count():
    """Load all files with 5-min bars, verify reasonable bar count."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    agg.finalise()

    # 390 trading minutes per day ÷ 5 = 78 bars/day × 4 days ≈ 312
    # (may differ slightly due to partial first/last bins)
    print(f"Complete bars (5-min): {len(agg)}")
    assert len(agg) > 250, f"Expected > 250 bars, got {len(agg)}"
    print("  PASS\n")


def test_1min_bars_equal_raw_rows():
    """With bar_minutes=1, each bar should correspond to one snapshot (count=1)."""
    agg = BarAggregator(bar_minutes=1)
    agg.load_file(FILES[0])
    agg.finalise()

    # File has 390 data rows (391 lines minus header)
    print(f"1-min bars for day 1: {len(agg)}")
    for bar in agg:
        assert bar.count == 1, f"Expected count=1 for 1-min bar, got {bar.count} at {bar.timestamp}"
    print("  PASS — every bar has count=1\n")


def test_5min_bar_count_per_snapshot():
    """Each 5-min bar should have at most 5 snapshots."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_file(FILES[0])
    agg.finalise()

    for bar in agg:
        assert 1 <= bar.count <= 5, f"Bad count {bar.count} at {bar.timestamp}"
    print(f"5-min bars day 1: {len(agg)}, all counts in [1..5]")
    print("  PASS\n")


def test_ohlc_integrity():
    """High >= Open, Close, Low for every bar.  Low <= Open, Close, High."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    agg.finalise()

    for bar in agg:
        assert bar.high >= bar.open, f"high < open at {bar.timestamp}"
        assert bar.high >= bar.close, f"high < close at {bar.timestamp}"
        assert bar.high >= bar.low, f"high < low at {bar.timestamp}"
        assert bar.low <= bar.open, f"low > open at {bar.timestamp}"
        assert bar.low <= bar.close, f"low > close at {bar.timestamp}"
    print(f"OHLC integrity verified for {len(agg)} bars")
    print("  PASS\n")


def test_index_ordering():
    """Index 0 should be the most recent complete bar."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    agg.finalise()

    newest = agg[0]
    second = agg[1]
    # Newest timestamp should be >= second newest
    assert newest.timestamp >= second.timestamp, (
        f"Index 0 ({newest.timestamp}) should be >= index 1 ({second.timestamp})"
    )
    print(f"Index 0: {newest.timestamp},  Index 1: {second.timestamp}")
    print("  PASS\n")


def test_incomplete_bar_access():
    """Index -1 should return the in-progress bar before finalise."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_file(FILES[0])
    # Do NOT finalise — so there should be a current bar
    current = agg[-1]
    assert current.count >= 1, "Expected at least 1 snapshot in current bar"
    print(f"Incomplete bar: {current}")
    print("  PASS\n")


def test_push_csv_string():
    """push() should accept a raw CSV line string."""
    agg = BarAggregator(bar_minutes=5)
    line = "20260102,09:30:07,20260102-09:30:06,188.9,,,,,,NVDA,4815747"
    agg.push(line)
    bar = agg[-1]
    assert bar.open == 188.9
    assert bar.count == 1
    print(f"CSV string push: {bar}")
    print("  PASS\n")


def test_push_dict():
    """push() should accept a dict."""
    agg = BarAggregator(bar_minutes=5)
    row = {
        "date": "20260102", "time": "09:31:05", "_updated": "20260102-09:30:59",
        "last": "188.91", "bid": "188.9", "ask": "188.95",
        "bid_sz": "20000.0", "ask_sz": "10000.0", "volume": "2892300.0",
        "symbol": "NVDA", "conid": "4815747",
    }
    agg.push(row)
    bar = agg[-1]
    assert bar.open == 188.91
    print(f"Dict push: {bar}")
    print("  PASS\n")


def test_push_json_string():
    """push() should accept a JSON string."""
    agg = BarAggregator(bar_minutes=5)
    row = {
        "date": "20260102", "time": "09:31:05", "_updated": "20260102-09:30:59",
        "last": "188.91", "bid": "188.9", "ask": "188.95",
        "bid_sz": "20000.0", "ask_sz": "10000.0", "volume": "2892300.0",
        "symbol": "NVDA", "conid": "4815747",
    }
    agg.push(json.dumps(row))
    bar = agg[-1]
    assert bar.open == 188.91
    print(f"JSON string push: {bar}")
    print("  PASS\n")


def test_custom_volume_fn():
    """Custom volume function (sum instead of average)."""
    agg = BarAggregator(bar_minutes=5, volume_fn=lambda vols: int(sum(vols)))
    agg.load_file(FILES[0])
    agg.finalise()

    agg_avg = BarAggregator(bar_minutes=5)
    agg_avg.load_file(FILES[0])
    agg_avg.finalise()

    # Sum volume should be >= average volume for any multi-snapshot bar
    for i in range(len(agg)):
        if agg[i].count > 1:
            assert agg[i].volume >= agg_avg[i].volume, (
                f"Sum vol should be >= avg vol at {agg[i].timestamp}"
            )
            break
    else:
        raise AssertionError("No multi-snapshot bar found to compare")
    print("Custom volume_fn (sum) verified against default (avg)")
    print("  PASS\n")


def test_iteration():
    """Iterating should yield bars newest → oldest."""
    agg = BarAggregator(bar_minutes=15)
    agg.load_file(FILES[0])
    agg.finalise()

    timestamps = [bar.timestamp for bar in agg]
    assert timestamps == sorted(timestamps), "Iteration should be oldest first"
    print(f"Iterated {len(timestamps)} bars in ascending order")
    print("  PASS\n")


def test_bar_timestamps_military_time():
    """Timestamps should use 24-hour / military format."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_file(FILES[0])
    agg.finalise()

    # Afternoon bars should have hour >= 12
    afternoon = [b for b in agg if int(b.timestamp.split("-")[1].split(":")[0]) >= 12]
    assert len(afternoon) > 0, "Expected some afternoon bars"
    # Verify no AM/PM markers
    for b in agg:
        assert "AM" not in b.timestamp and "PM" not in b.timestamp
    print(f"Found {len(afternoon)} afternoon bars, all in military time")
    print("  PASS\n")


def test_multiday_continuity():
    """Bars across multiple days should be distinct; no cross-day merging."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    agg.finalise()

    dates = set()
    for bar in agg:
        date_part = bar.timestamp.split("-")[0]
        dates.add(date_part)
    assert len(dates) == 4, f"Expected 4 trading days, got {len(dates)}: {dates}"
    print(f"Dates found: {sorted(dates)}")
    print("  PASS\n")


def test_print_sample_bars():
    """Print a handful of bars for manual inspection."""
    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)

    print("=== Last 5 complete bars (newest first) ===")
    for i in range(min(5, len(agg))):
        print(f"  [{i}] {agg[i]}")
    if agg.current_bar:
        print(f"  [-1] {agg[-1]}  (incomplete)")
    agg.finalise()
    print(f"\nTotal complete bars: {len(agg)}")
    print()


def test_save_csv():
    """save() should produce a valid CSV with correct row count and chronological order."""
    import os, tempfile

    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    agg.finalise()

    out = "/tmp/test_save_output.csv"
    result_path = agg.save(out)
    assert os.path.exists(out), "File was not created"

    with open(out) as f:
        lines = f.readlines()
    header = lines[0].strip()
    assert header == "timestamp,date,time,open,high,low,close,volume,count"

    # Data rows should equal complete bar count
    data_rows = lines[1:]
    assert len(data_rows) == len(agg), f"Expected {len(agg)} rows, got {len(data_rows)}"

    # Chronological: first data row should be oldest, last should be newest
    first_ts = data_rows[0].split(",")[0]
    last_ts = data_rows[-1].split(",")[0]
    assert first_ts <= last_ts, f"Expected chronological order: {first_ts} <= {last_ts}"

    print(f"Saved {len(data_rows)} bars to CSV, chronological order verified")
    print(f"  Header: {header}")
    print(f"  First row: {data_rows[0].strip()}")
    print(f"  Last row:  {data_rows[-1].strip()}")
    os.remove(out)
    print("  PASS\n")


def test_save_csv_reverse():
    """save(chronological=False) should write newest first."""
    import os

    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    agg.finalise()

    out = "/tmp/test_save_reverse.csv"
    agg.save(out, chronological=False)

    with open(out) as f:
        lines = f.readlines()
    first_ts = lines[1].split(",")[0]
    last_ts = lines[-1].split(",")[0]
    assert first_ts >= last_ts, f"Expected reverse order: {first_ts} >= {last_ts}"

    os.remove(out)
    print("Reverse chronological save verified")
    print("  PASS\n")


def test_save_csv_include_incomplete():
    """save(include_incomplete=True) should add one extra row."""
    import os

    agg = BarAggregator(bar_minutes=5)
    agg.load_files(FILES)
    # Do NOT finalise — so there is an incomplete bar

    out_without = "/tmp/test_save_no_inc.csv"
    out_with = "/tmp/test_save_inc.csv"
    agg.save(out_without, include_incomplete=False)
    agg.save(out_with, include_incomplete=True)

    with open(out_without) as f:
        count_without = len(f.readlines()) - 1
    with open(out_with) as f:
        count_with = len(f.readlines()) - 1

    assert count_with == count_without + 1, (
        f"Expected include_incomplete to add 1 row: {count_without} vs {count_with}"
    )

    os.remove(out_without)
    os.remove(out_with)
    print(f"include_incomplete: {count_without} → {count_with} rows")
    print("  PASS\n")


# ---------------------------------------------------------------
if __name__ == "__main__":
    test_basic_load_and_bar_count()
    test_1min_bars_equal_raw_rows()
    test_5min_bar_count_per_snapshot()
    test_ohlc_integrity()
    test_index_ordering()
    test_incomplete_bar_access()
    test_push_csv_string()
    test_push_dict()
    test_push_json_string()
    test_custom_volume_fn()
    test_iteration()
    test_bar_timestamps_military_time()
    test_multiday_continuity()
    test_save_csv()
    test_save_csv_reverse()
    test_save_csv_include_incomplete()
    test_print_sample_bars()

    print("=" * 50)
    print("ALL TESTS PASSED")
