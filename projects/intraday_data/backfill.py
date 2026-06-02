from indicator_sets import EMA_Indicator_Set
from bar_aggregator import BarAggregator, get_filtered_filenames
from pathlib import Path

## at least a half day's worth of data
DATA_THRESHOLD = (60 * 6.5)/2
#DATA_DIR = '/portfolio/pacman/data/'
DATA_DIR = '/Users/jcarter/hannibal/dev/data/upro_samples/'
#OUTPUT_DIR = '/portfolio/basic_ema/data/backfill_ema_set/'
OUTPUT_DIR = '/Users/jcarter/hannibal/dev/data/backfill/'

def count_lines(filename):
    with open(filename, 'r') as f:
        return sum(1 for _ in f)

def run_backfill(symbol, indicator_set):
	agg = BarAggregator(bar_minutes=10, indicator_set=indicator_set)
	for filename in get_filtered_filenames(f'{DATA_DIR}', symbol):
		filename = f'{DATA_DIR}/{filename}'
		lines = count_lines(filename)
		if lines < DATA_THRESHOLD:
			print(f'{filename} ignored. Line count = {lines}')
			continue

		print(f'Processing: {filename}')

		checkpoint_file = f'{OUTPUT_DIR}/ema_set.checkpoint'

		if Path(checkpoint_file).exists():
			agg.load_checkpoint(checkpoint_file)

		agg.load_file(filename)
		agg.finalise()
		if agg.count >= indicator_set.history_needed: 
			agg.write_checkpoint(checkpoint_file)
		date = filename.split('.')[1]
		agg.save(f"{OUTPUT_DIR}/{date}.csv", date_filter=date)

if __name__ == '__main__':
	run_backfill('UPRO', EMA_Indicator_Set())
