from indicator_sets import Test_Indicator_Set
from bar_aggregator import BarAggregator, Bar, _default_volume_fn, get_filtered_filenames 
import os

DATA_DIR = "/Users/jcarter/hannibal/dev/nvda_samples"
FILES = [
    f"{DATA_DIR}/NVDA.20260102.csv",
    f"{DATA_DIR}/NVDA.20260105.csv",
    f"{DATA_DIR}/NVDA.20260106.csv",
    f"{DATA_DIR}/NVDA.20260107.csv",
]

test_indicator_set = Test_Indicator_Set()

def test_write_checkpoint_and_csv():
    """save() should produce a valid CSV with correct row count and chronological order."""

    agg = BarAggregator(bar_minutes=5, indicator_set=test_indicator_set)
    agg.load_file(FILES[0])
    agg.finalise()

    agg.write_checkpoint(f'{DATA_DIR}/package1.checkpoint')
    out = f"{DATA_DIR}/test_save_output_00.csv"
    result_path = agg.save(out)
    assert os.path.exists(out), "File was not created"

def test_load_checkpoint_and_csv():
    """load in checkpoint and continue annotated calculations"""

    agg = BarAggregator(bar_minutes=5, indicator_set=test_indicator_set)
    agg.load_checkpoint(f'{DATA_DIR}/package1.checkpoint')
    agg.load_file(FILES[1])
    agg.finalise()

    agg.write_checkpoint(f'{DATA_DIR}/package2.checkpoint')
    out = f"{DATA_DIR}/test_save_output_01.csv"
    result_path = agg.save(out)
    assert os.path.exists(out), "File was not created"

def test_continuous():
    """verify that segmented calcs (w/ checkpoints) match continuous calculations"""

    agg = BarAggregator(bar_minutes=5, indicator_set=test_indicator_set)
    agg.load_file(FILES[0])
    agg.finalise()
    agg.load_file(FILES[1])
    agg.finalise()
    out = f"{DATA_DIR}/test_save_output_02.csv"
    result_path = agg.save(out)
    assert os.path.exists(out), "File was not created"

def test_backfill():
	for filename in get_filtered_filenames(f'{DATA_DIR}', 'NVDA'):
		print(f'Processing: {filename}')
		agg = BarAggregator(bar_minutes=5, indicator_set=test_indicator_set)
		try:
			agg.load_checkpoint(f'{DATA_DIR}/nvda.checkpoint')
		except:
			pass
		agg.load_file(f'{DATA_DIR}/{filename}')
		agg.finalise()
		agg.write_checkpoint(f'{DATA_DIR}/nvda.checkpoint')
		date = filename.split('.')[1]
		agg.save(f"{DATA_DIR}/analytics/{date}.csv")


if __name__ == '__main__':
	"""
		test_write_checkpoint_and_csv()
		test_load_checkpoint_and_csv()
		test_continuous()
	"""
	test_backfill()

