from datetime import datetime
import os, sys, json
import logging
import argparse
import time, pandas
from indicators import MondayAnchor, StDev
import ib_endpoints2 as IB
from strategy import Strategy
from posmgr import PosMgr, TradeSide, Trade, OrderType
from clockutils import create_tripwire, unix_time_to_string


def get_time():
    return datetime.today().strftime('%Y%m%d')

STRATEGY = os.getenv('STRATEGY', 'test')
PORTFOLIO_DIRECTORY = os.getenv('PORTFOLIO_DIRECTORY', '/portfolio/')

# Create a FileHandler in 'append' mode
log_filename=f"{PORTFOLIO_DIRECTORY}/{STRATEGY}/logs/{STRATEGY}.{get_time()}.log"
file_handler = logging.FileHandler(log_filename, mode='a')
file_handler.setLevel(logging.INFO)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
logging.basicConfig(
    level = logging.INFO,
    format=FORMAT,
    handlers=[file_handler],
    datefmt='%a %Y-%m-%d %H:%M:%S'
)

## all messages at INFO level and above will be captured
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TestTrader(Strategy):
    def __init__(self, strategy_id, configuration_file):
        super().__init__(strategy_id, configuration_file)

        self.order_monitor = IB.OrderMonitor()
        self.pos_mgr = PosMgr()
        self.intra_prices = list()
        self.contract_id = None
        self.symbol = None


    def get_bid_ask(self):
        bid = ask = bid_size = ask_size = None
        symbol = None 
        try:
            market_data = IB.market_snapshot(self.contract_id)
            bid = market_data.get('bid')
            ask = market_data.get('ask')
            bid_size = market_data.get('bid_size')
            ask_size = market_data.get('ask_size')
            symbol = market_data.get('symbol')

            logger.info(f'current bid/ask for symbol = {symbol}({self.contract_id}): bid:{bid}|({bid_size}), ask:{ask}|({ask_size})')
        except Exception as exc:
            logger.critical(f'No price data available for contract_id = {self.contract_id}!')
            logger.critical(exc)

        return bid, ask, bid_size, ask_size


    def fetch_prices(self):
        market_data = IB.market_snapshot(self.contract_id)
        ## ensure that a price quote was posted.
        ## sometime the first call only establishes a connection
        if market_data.get('last'):
            self.intra_prices.append(market_data)


    def create_order(self, side, amount, order_type=OrderType.MKT, order_notes=None):

        logger.info('sending order.')

        order_info = IB.order_request(self.contract_id, order_type.value, side.value, amount)
        if order_info.get('reply_id') is not None:
            ## confirm to server that you want to send this order
            ## repeat flag forces all subsequent rder_replies to be resolved before returning
            order_info = IB.order_reply(order_info['reply_id'], repeat=True)

        order_id = order_info['order_id']
        logger.info(f'order_id: {order_id} successfully sent.')

        order_info = {
            'order_id': order_id,
            'symbol': self.symbol,
            'quantity': amount,
            'side': side.value,
            'order_type': order_type.value,
            'info': order_notes
        }

        order_info_dump = 'order_info:\n' + json.dumps(order_info, ensure_ascii=False, indent =4 )
        logger.info(order_info_dump)

        return order_info


    def process_fill(self, fill):

        def _get_side(fill):
            sides = { 'BUY': TradeSide.BUY, 'SELL': TradeSide.SELL }
            v = fill.get('side', None)
            if v is not None:
                return sides[v.upper()]

            fill_json = json.dumps(fill, ensure_ascii=False, indent=4)
            raise RuntimeError(f'no BUY/SELL action indicated in order fill!\n order fill: {fill_json}')

        ## map ib web api order fill
        def _convert_ib_fill(fill):
            trd = Trade( fill['trade_id'] )
            trd.asset = fill["ticker"]
            trd.order_id = fill['order_id']
            trd.side = _get_side(fill)
            trd.units = fill['qty']
            trd.price = fill['price']
            ## conditionals
            tms = fill.get('lastExecutionTime_r')
            if tms is not None:
                trd.timestamp = unix_time_to_string(tms)
            elif trd.timestamp is None:
                trd.stamp_timestamp()
            trd.commission = fill.get("commission")
            trd.exchange = fill.get("conidex")

            return trd

        trade_deets = fill['trade_id'] 
        logger.info(f'Processing trade_id: {trade_deets}')
        self.pos_mgr.update_trades( fill, conversion_func=_convert_ib_fill )


    def create_directory(self, directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    def dump_intraday_prices(self, filepath):
        try:
            df = pandas.DataFrame(self.intra_prices)
            df = df[['date','time','_updated','last','bid','ask','bid_sz','ask_sz','volume','symbol','conid']]
            df.to_csv(filepath, index=False)
        except:
            logger.error(f"couldn't write intraday data: {filepath}")
            raise RuntimeError(f"couldn't write intraday data: {filepath}")

    def run_strategy(self):
        logger.info('starting test trader.')

        self.pos_mgr.initialize(self.strategy_id, set(self.cfg['universe']))
        logger.info('pos mgr initialized.')


        pp = self.pos_mgr.position_count()
        if pp == 0:
            raise RuntimeError(f'No targeted positions for universe: {self.cfg["universe"]}')
        if pp != 1:
            raise RuntimeError(f'Too many names: {self.pos_mgr.positions} - this a single name strategy')

        ## grab the only instrument in the universe
        self.symbol = self.cfg['universe'][0]
        self.contract_id = IB.symbol_to_contract_id(self.symbol)

        ## returns a PosNode object
        position_node = self.pos_mgr.get_position(self.symbol)
        current_pos = position_node.position
        logger.info(f'{self.symbol} current position = {current_pos}')


        logger.info('pinging IB server.')
        ping = IB.tickle()
        logger.info(json.dumps(ping, ensure_ascii=False, indent=4 ))

        logger.info(f'initialize market data connection for symbol= {self.symbol}, contract_id= {self.contract_id}')
        market_init = IB.market_connect(self.contract_id, retry=5)
        if not market_init:
            logger.critical(f'market data initialization failed. conid= {self.contract_id}')

        logger.info('fetch account information from IB')
        account_info = IB.account_summary()
        account_file =f"{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/{STRATEGY}.{get_time()}.account.json"
        with open(account_file, 'w') as f:
            acc_info = json.dumps(account_info, ensure_ascii=False, indent=4)
            f.write(acc_info)

        ## trading operations schedule
        at_open = create_tripwire(self.cfg.get('at_open')) 
        at_close = create_tripwire(self.cfg.get('at_close'))
        at_end_of_day = create_tripwire(self.cfg.get('at_eod')) 
        fetch_open_prices = create_tripwire(self.cfg.get('fetch_open')) 
        fetch_close_prices = create_tripwire(self.cfg.get('fetch_close')) 

        at_mid = create_tripwire(self.cfg.get('at_mid')) 
        at_bubba = create_tripwire(self.cfg.get('at_bubba')) 

        tws = [at_open, at_mid, at_bubba, at_close, at_end_of_day, fetch_open_prices, fetch_close_prices]
        for tw in tws:
            logger.info(tw)

        logger.info(f'starting trading loop.')

        while True:

            ## capturing 1 min price snapshots - first 2 hours
            with fetch_open_prices as fetch_open:
                if fetch_open:
                    self.fetch_prices()

            with fetch_close_prices as fetch_close:
                if fetch_close:
                    self.fetch_prices()

            with at_open as opening:
                if opening:
                    trade_amt = 80 
                    _bid, open_price, _bidsz, _asksz = self.get_bid_ask()

                    logger.info(f'opening ask price: {open_price}')
                    order_info = self.create_order(TradeSide.BUY, trade_amt, order_notes=self.strategy_id)
                    self.pos_mgr.register_order(order_info)

            with at_mid as mike:
                if mike:
                    trade_amt = 25 
                    _bid, _ask, _bidsz, _asksz = self.get_bid_ask()

                    logger.info(f'mike ask price: {_ask}')
                    order_info = self.create_order(TradeSide.BUY, trade_amt, order_notes=self.strategy_id)
                    self.pos_mgr.register_order(order_info)

            with at_bubba as bubba:
                if bubba:
                    trade_amt = 50 
                    _bid, _ask, _bidsz, _asksz = self.get_bid_ask()

                    logger.info(f'bubba bid price: {_bid}')
                    order_info = self.create_order(TradeSide.SELL, trade_amt, order_notes=self.strategy_id)
                    self.pos_mgr.register_order(order_info)

            for fill in self.order_monitor.check_orders():
                self.process_fill(fill)

            with at_close as closing:
                if closing:
                    position_node = self.pos_mgr.get_position(self.symbol)
                    current_pos, entry_price = position_node.position, position_node.price
                    logger.info(f'{self.symbol} pos={current_pos} entry_price={entry_price}')
                    close_price, _ask, _bidsz, _asksz = self.get_bid_ask()
                    logger.info(f'{self.symbol} current_price={close_price}')
                    order_info = self.create_order(TradeSide.SELL, current_pos, order_notes=self.strategy_id)
                    self.pos_mgr.register_order(order_info)

            with at_end_of_day as end_of_day:
                if end_of_day:
                    today = datetime.today().strftime("%Y%m%d")
                    self.create_directory(f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/')
                    intra_file = f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/{self.symbol}.{today}.csv'
                    logger.info('saving intraday prices ...')
                    self.dump_intraday_prices(intra_file)
                    logger.critical('end of day completed.')
                    logout = IB.logout()
                    logger.info(f'logged out = {logout}')
                    break

            time.sleep(3)


if __name__ == "__main__":
    parser =  argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    parser.add_argument("--strategy_id", help="strategy id", required=True)
    u = parser.parse_args()

    trader = TestTrader(u.strategy_id, u.config)
    trader.run_strategy()
