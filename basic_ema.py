from datetime import datetime
import os, sys, json
import logging
import time
import argparse
import time, pandas
from indicators import StDev
from indicator_sets import EMA_Indicator_Set 
from bar_aggregator import BarAggregator
import calendar_calcs
from strategy2 import Strategy
from posmgr2 import OrderType, OrderStatus
from clockutils import create_tripwire, unix_time_to_string
import functools

def get_time():
    return datetime.today().strftime('%Y%m%d')

## IMPORTANT - always label correct STRATEGY tag
STRATEGY = 'basic_ema'
PORTFOLIO_DIRECTORY = os.getenv('PORTFOLIO_DIRECTORY', '/portfolio/')
DATA_DIR = os.getenv('DATA_DIR', '/trading/data/')

## IMPORTANT - Connectivity set up
## The hub server, 'hub_server.py' handles global connectivity IB
## if NOT using the hub server, the strategy must connect to IB itself via IB.establish_connection()
USING_HUB = (os.getenv('USE_HUB', 'TRUE')).upper() == 'TRUE'

# Create a FileHandler in 'append' mode
log_filename=f"{PORTFOLIO_DIRECTORY}/{STRATEGY}/logs/{STRATEGY}.{get_time()}.log"
file_handler = logging.FileHandler(log_filename, mode='a')
file_handler.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
logging.basicConfig(
    level = logging.INFO,
    format=FORMAT,
    handlers=[file_handler, console_handler],
    datefmt='%a %Y-%m-%d %H:%M:%S'
)

## all messages at INFO level and above will be captured
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

## Basic = Standard Model Setup
class BasicStrategy(Strategy):
    def __init__(self, strategy_id, configuration_file, kill_strategy):
        super().__init__(strategy_id, configuration_file, kill_strategy)

        self.intra_prices = list()
        self.contract_id = None
        self.symbol = None

        ## indicators
        self.anchor = None
        self.stdv = None
        self.holidays = None

    def load_historical_data(self, symbol):
        ## load yahoo OHLC data
        try:
            stock_file = f'{DATA_DIR}/{symbol}.csv'
            stock_df = pandas.read_csv(stock_file)
            stock_df.set_index('Date', inplace=True)
            logger.info(f'{symbol} historical data loaded.')
        except Exception as e:
            logger.critical(f'{symbol} historical data load failed.\n' + str(e))
            raise e

        return stock_df

    def fetch_prices(self):
        try:
            market_data = self.get_market_snapshot(self.contract_id)
            ## ensure that a price quote was posted.
            ## sometime the first call only establishes a connection
            """
            --- returned market_data dict:
            {
                "last": 173.96,
                "ask": 173.95,
                "bid": 173.96,
                "bid_sz": 80000,
                "ask_sz": 20000,
                "volume": 83916700,
                "symbol": "AAPL",
                "conid": 265598,
                "date": "20230913",
                "time": "17:15:00",
                "_updated": "20230913-17:14:59"
            }
            """
            if market_data.get('last'):
                self.intra_prices.append(market_data)
                return market_data
        except:
            logger.error('Could not fetch market data')
        
        return None

    def dump_intraday_prices(self, filepath):
        try:
            df = pandas.DataFrame(self.intra_prices)
            df = df[['date','time','_updated','last','bid','ask','bid_sz','ask_sz','volume','symbol','conid']]
            df.to_csv(filepath, index=False)
        except:
            logger.error(f"couldn't write intraday data: {filepath}")
            # raise RuntimeError(f"couldn't write intraday data: {filepath}")


    def daily_calc_metrics(self, stock_df):

        daysback = 50
        self.holidays = calendar_calcs.load_holidays()
        self.stdv = StDev(sample_size=daysback)

        ss = len(stock_df)
        if ss < daysback:
            logger.error(f'Not enoungh data to calc metrics: len={ss}, daysback={daysback}')
            raise RuntimeError(f'Not enoungh data to calc metrics: len={ss}, daysback={daysback}')

        gg = stock_df[-daysback:]

        last_indicator_date = None
        last_close = None
        for i in range(gg.shape[0]):
            idate = gg.index[i]
            stock_bar = gg.loc[idate]
            cur_dt = datetime.strptime(idate,"%Y-%m-%d").date()
            self.stdv.push(stock_bar['Close'])
            last_indicator_date = cur_dt
            last_close = stock_bar['Close']

        today = datetime.today().date()
        ## make sure the signal is for the previous trading day
        prev_trading_dt = calendar_calcs.prev_trading_day(today, self.holidays)
        if last_indicator_date != prev_trading_dt:
            msg = f'incomplete data for indicators, last_indicator_date= {last_indicator_date}, prev_trading_date= {prev_trading_dt}'
            logger.error(msg)
            raise RuntimeError(msg)


    def calc_entry_targets(self, alloc_node, prices_dict):

        ## based on the dict of prices and the cash alloc given (for this account)
        ## calc risk exposure for every name in the universe
        #  ## stp_price drives both limit and stop orders
        ## don't calc new targets if orders are open

        opens = self.get_open_orders()
        if opens:
            logger.critical(f"\ncan't calc entries - open orders exist:\n{opens}")
            return []

        cash_alloc = alloc_node.cash

        targets = []
        for symbol, bid_ask in prices_dict.items():
            bid, ask = bid_ask

            if symbol == self.symbol:

                target_amt = int(cash_alloc/(ask + abs(ask-bid)) )

                ## define the type of order you want to execute with
                ## target amount
                tgt = { 'symbol': symbol,
                        'target_amt': target_amt,
                        'order_type': OrderType.MKT,
                        'stop_price': None,
                        'limit_price': None
                }
                targets.append( tgt )

        return targets


    def calc_exit_targets(self, alloc_node, prices_dict):

        ## don't calc new targets if orders are open
        opens = self.get_open_orders()
        if opens:
            logger.warning(f"\ncan't calc exits - open orders exist:\n{opens}")
            return []
    
        ## unwind the positions created in calc_entry_targets
        targets = []
        for symbol, bid_ask in prices_dict.items():
            bid, ask = bid_ask

            if symbol == self.symbol:
                try:

                    ## INSERT IF NEEDED!
                    ## pos_node = self.get_position(symbol)

                    ## define the type of order you want to execute with
                    ## target amount
                    tgt = { 'symbol': symbol,
                            'target_amt': 0,
                            'order_type': OrderType.MKT,
                            'stowwwp_price': None,
                            'limit_price': None
                    }
                    targets.append( tgt )

                except (TypeError, KeyError):
                    logger.critical(f'symbol: {symbol}, position=0, unwind failed')

        return targets


    def check_entry(self, bar_repo):
        """
        no open position, no open orders
        current bar ma3 > ma13 -> BUY
        """
        if self.get_open_orders():
            return False

        position_node = self.get_position(self.symbol)
        current_pos = position_node.position
        if current_pos is not None and current_pos != 0:
            return False

        bar = bar_repo[0]
        ema3 = bar._indicators.get('ema3')
        ema13 = bar._indicators.get('ema13')
        if all([ema3, ema13]) and ema3 > ema13:
            return True

        return False


    def check_exit(self, bar_repo):
        """
        open position, no open orders
        current bar ma3 < ma13 -> SELL 
        """
        if self.get_open_orders():
            return False

        position_node = self.get_position(self.symbol)
        current_pos = position_node.position
        if current_pos is not None and current_pos > 0:
            bar = bar_repo[0]
            ema3 = bar._indicators.get('ema3')
            ema13 = bar._indicators.get('ema13')
            if all([ema3, ema13]) and ema3 < ema13:
                return True

        return False


    def exit_on_close(self):
        ## no exit on close
        return False


    def run_strategy(self):

        logger.info('starting strategy.')

        self.open_trading_book()

        if self.position_count() != 1:
            msg = f'Position Error: {self.get_positions()} - this a single name strategy'
            logger.critical(msg)
            raise RuntimeError(msg)

        ## grab the only instrument in the universe
        self.symbol = self.cfg['universe'][0]

        ## look up con_id 
        self.contract_id = self.get_contract_id(self.symbol)
        logger.info(f'{self.symbol}: conid = {self.contract_id}')

        ## returns a PosNode object
        position_node = self.get_position(self.symbol)
        current_pos = position_node.position
        logger.info(f'{self.symbol} current OPENING position = {current_pos}')

        ## get historical data for the symbol DRIVING trading signals
        ## this is different from the symbol that is used to enter positions
        """
        logger.info('calculating trading metrics from daily history.')
        data = self.load_historical_data(self.symbol)
        self.daily_calc_metrics(data)
        logger.info('trading metrics calculated.')
        """

        self.connect_to_market(self.symbol)

        ## trading operations schedule
        fetch_prices = create_tripwire(self.cfg.get('fetch_prices'))
        at_close = create_tripwire(self.cfg.get('at_close'))
        at_end_of_day = create_tripwire(self.cfg.get('at_eod'))

        ## reporting TripWires
        yy = [fetch_prices, at_close, at_end_of_day]
        logger.info(f'\nTripWire setup:\n{yy}')

        ## create 10-minute bars and add 3,13-bar EMAs
        checkpoint_file = f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/ema_set/ema_set.checkpoint'
        bar_repo = BarAggregator(bar_minutes=10, indicator_set=EMA_Indicator_Set())
        ## grab checkpoint to continue indicator calculations
        try:
            bar_repo.load_checkpoint(checkpoint_file)
            logger.info(f'checkpoint: {checkpoint_file} loaded.')
        except:
            logger.info(f'no checkpoint_file loaded.')
        
        logger.info('starting trading loop.')

        while True:

            ## capturing 1 min price snapshots
            ## and building 10min bars + indicators in bar_repo
            with fetch_prices as get_prices:
                new_bar = None
                if get_prices:
                    market_data = self.fetch_prices()
                    if market_data is not None:
                        new_bar = bar_repo.push(market_data)
                        if new_bar is not None:
                            logger.info(f'BAR -> {new_bar}')

                if new_bar:
                    if self.check_entry(bar_repo):
                        _target_map = self.get_targets( self.calc_entry_targets )
                        self.send_orders( _target_map, order_notes='EMA Entry')
                    elif self.check_exit(bar_repo):
                        _target_map = self.get_targets( self.calc_exit_targets )
                        self.send_orders( _target_map, order_notes='EMA Exit')
                    else:
                        position_node = self.get_position(self.symbol)
                        current_pos = position_node.position
                        if current_pos:
                            logger.info(f'working open position for strategy: {self.strategy_id}: {self.symbol} {current_pos}')

            if self.get_open_orders():
                logger.info('checking for fills')
                for fill in self.check_orders():
                    self.process_fill(fill)

            with at_close as closing:
                if closing:
                    if self.exit_on_close(): 
                        _target_map = self.get_targets( self.calc_exit_targets )
                        self.send_orders( _target_map, order_notes='Exit On Close' )
                    else:
                        position_node = self.get_position(self.symbol)
                        current_pos = position_node.position
                        if current_pos:
                            logger.info(f'no exit signal for strategy: {self.strategy_id}: holding: {self.symbol} {current_pos}')

            with at_end_of_day as end_of_day:
                if end_of_day:
                    today = datetime.today().strftime("%Y%m%d")
                    self.create_directory(f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/')
                    intra_file = f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/{self.symbol}.{today}.csv'
                    logger.info(f'saving intraday prices to: {intra_file}')
                    self.dump_intraday_prices(intra_file)

                    self.create_directory(f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/ema_set')
                    analytics_file = f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/ema_set/{self.symbol}.{today}.csv'
                    logger.info(f'saving intraday analytics: {analytics_file}')
                    bar_repo.save(analytics_file)
                    logger.info(f'saving checkpoint: {checkpoint_file}')
                    bar_repo.write_checkpoint(checkpoint_file)

                    self.close_trading_book()
                    logger.critical('end of day completed.')
                    self.logout()
                    break

            time.sleep(3)


if __name__ == "__main__":
    parser =  argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    parser.add_argument("--strategy_id", help="strategy id", required=True)
    parser.add_argument("--kill", help="kill switch", action='store_true')
    u = parser.parse_args()

    holidays = calendar_calcs.load_holidays()
    today = datetime.today().date()
    if today not in holidays:

        if not USING_HUB: IB.establish_connection()

        basic = BasicStrategy(u.strategy_id, u.config, u.kill)
        basic.run_strategy()

    else:
        today_str = today.strftime("%Y-%m-%d")
        logger.critical(f'Today:{today_str} is a holiday. Strategy:{u.strategy_id} disabled.')

