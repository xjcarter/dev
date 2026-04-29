
from indicators import MA
from dataclasses import dataclass, field

class Test_Indicator_Set:
	"""
	this is a custom backfill module that can be used
	by a BarAggregator to generate analytics for a specific strategy.

	agg = BarAggregator(bar_minutes: int = 5, volume_fn = None, annotate_fn = run_indicators)
	"""


	def __init__(self):
		self.ma5 = MA(5)
		self.ma10 = MA(10)
		self.name = 'ma5-ma10'

	@property
	def history_needed(self):
		return 10 

	# reset- create new indicators
	def reset(self):
		self.ma5 = MA(5)
		self.ma10 = MA(10)

	## annotate the given bar with updated indicator values
	def run_indicators(self, bar):
		"""A single OHLCV bar with a count of contributing snapshots.
		class Bar():
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
	    """


		ma5_value = self.ma5.push(bar.close)
		ma10_value = self.ma10.push(bar.close)


		result = {
					'ma5': ma5_value,
					'ma10': ma10_value
				}

		## attach new info to the current bar
		bar.annotate(result)

		return bar

class EMA_Indicator_Set:
	"""
	this is a custom backfill module that can be used
	by a BarAggregator to generate analytics for a specific strategy.

	agg = BarAggregator(bar_minutes: int = 5, volume_fn = None, annotate_fn = run_indicators)
	"""


	def __init__(self):
		self.ema3 = EMA(3)
		self.ema13 = EMA(13)
		self.name = 'ema3-ema13'

	@property
	def history_needed(self):
		return 13 

	# reset- create new indicators
	def reset(self):
		self.ema3 = EMA(3)
		self.ema13 = EMA(13)

	## annotate the given bar with updated indicator values
	def run_indicators(self, bar):
		"""A single OHLCV bar with a count of contributing snapshots.
		class Bar():
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
	    """


		ema3_value = self.ema3.push(bar.close)
		ema13_value = self.ema13.push(bar.close)


		result = {
					'ema3': ema3_value,
					'ema13': ema13_value
				}

		## attach new info to the current bar
		bar.annotate(result)

		return bar