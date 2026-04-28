
import json
import fcntl
from datetime import datetime
import re
import os
import pytz
from enum import Enum
import logging
import pandas
import time
import mysql.connector
import calendar_calcs
from sec_master import SecMaster
import ib_endpoints2 as IB

# Create a logger specific to __main__ module
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
#FORMAT = "%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s"
formatter = logging.Formatter(FORMAT, datefmt='%a %Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
"""

##
## position node for each name traded
## all nodes are saved to a state file to be loaded and updated by the trading engine each day
## framework for loading daily trades and names to trade
##

DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/ibrk/lex')
PORTFOLIO_DIRECTORY = os.environ.get('PORTFOLIO_DIRECTORY', '/home/jcarter/junk/portfolio/')
MYSQL_HOSTNAME = os.environ.get('MYSQL_HOSTNAME', 'localhost')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'tarzan001')

#converts UTC timestamps to EST
def convert_timestamp( utc_dt ):
    # Define the UTC and EST timezones
    utc_zone = pytz.utc
    est_zone = pytz.timezone('US/Eastern')
    
    # Ensure the datetime is aware of its timezone
    utc_dt = utc_zone.localize(utc_dt)
    
    # Convert the datetime to EST
    est_dt = utc_dt.astimezone(est_zone)
    
    return est_dt


class TradeSide(str, Enum):
    BUY = 'BUY'
    SELL = 'SELL'

class OrderType(str, Enum):
    MKT = 'MKT'
    LIMIT = 'LIMIT'
    STOP = 'STOP'
    STOP_LIMIT = 'STOP_LIMIT'

class OrderStatus(str, Enum):
    OPEN = 'OPEN'
    FILLED = 'FILLED'
    CANCELLED = 'CANCELLED'
    REJECTED = 'REJECTED'
    RAW = 'RAW'

## PositionLayer - Position Status
## holds the current state of a live position
## price = average price of the position
## high, low = the highest/lowest price points of the live position
## stop, target = position stop/target levels
class PositionLayer():
    def __init__(self, layer_dict=None):
        self.price = None
        self.position = None
        self.high = None
        self.low = None
        self.duration = 0 
        self.target = None
        self.stop = None

        if isinstance(layer_dict, dict):
            self.__dict__.update(layer_dict) 

    def update_high(self, high):
        if high is not None and high > 0:
            self.high = max(high, self.high or high )

    def update_low(self, low):
        if low is not None and low > 0:
            self.low = min(low, self.low or low)

    def update_duration(self):
        self.duration = max(self.duration, 0) + 1

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        value = getattr(self, key)
        return value if value is not None else default

    def to_dict(self):
        m = dict()
        for k, v in self.__dict__.items():
            if v is not None: m.update({k:v})
        return m

    def from_dict(self, dikt):
        self.__dict__.update(dikt) 


class LayerMgr():
    def __init__(self):
        self.layers = {}

    def _calc_avg_price(self, curr_pos, curr_price, new_pos, new_price):
            x = abs(curr_pos)
            y = abs(new_pos)
            avp = (( x * curr_price ) + ( y* new_price)) / ( x+ y)
            return avp


    ## creates a dummy trade and adds it to the layer
    def apply(self, symbol, amt, price, layer_id):
        dummy = Trade()
        dummy.asset = symbol
        dummy.price = price 
        dummy.units = abs(amt)
        dummy.side = TradeSide.BUY if amt >= 0 else TradeSide.SELL
        dummy.layer_id= layer_id 

        self.add(dummy)


    def add(self, trade):

        ## NOTE trade units are always POSITIVE

        position = price = duration = None

        try:
            prev_layer = self.layers[trade.layer_id][trade.asset]
            prev_amt, prev_price = prev_layer['position'], prev_layer['price']
            duration = prev_layer['duration']

            position = price = None
            if prev_amt > 0:
                if trade.side == TradeSide.BUY:
                    price = self._calc_avg_price(prev_amt, prev_price, trade.units, trade.price)
                    position =  prev_amt + trade.units
                elif trade.side == TradeSide.SELL:
                    position = prev_amt - trade.units
                    price = prev_price
                    if position < 0:
                        price = trade.price

            ## manage short positions
            elif prev_amt < 0:
                if trade.side == TradeSide.SELL:
                    price = self._calc_avg_price(prev_amt, prev_price, trade.units, trade.price)
                    position = prev_amt - trade.units
                elif trade.side == TradeSide.BUY:
                    position = prev_amt + trade.units
                    price = prev_price
                    if position > 0:
                        price = trade.price

            ## start new position
            elif prev_amt == 0:
                position = trade.units if trade.side == TradeSide.BUY else -(trade.units)
                price = trade.price

        except KeyError:
            ## build a new layer
            ## if trade.units not None or 0
            if trade.units:
                price = trade.price
                position = trade.units
                if trade.side == TradeSide.SELL: position = -(position)
                duration = 0
                if self.layers.get(trade.layer_id) is None:
                    self.layers[trade.layer_id] = {}
                if self.layers[trade.layer_id].get(trade.asset) is None:
                    self.layers[trade.layer_id][trade.asset] = {}
                p = f'New Position Layer: Layer= {trade.layer_id}, Symbol= {trade.asset}'
                logger.info(p)

        if position != 0:
            new_layer = dict(price=price, position= position, duration=duration)
            self.layers[trade.layer_id][trade.asset] = PositionLayer(new_layer) 
            p = f'Position Layer UPDATE: Layer= {trade.layer_id}, Symbol= {trade.asset}\n'
            p += f'{json.dumps(new_layer, indent=4)}'
            logger.info(p)

        if position == 0:
            ## remove empty position layers
            layer = self.layers.get(trade.layer_id)
            if layer is not None:
                layer.pop(trade.asset, None)
                if not layer:
                    self.layers.pop(trade.layer_id, None)
            p = f'Position Layer REMOVED: Layer= {trade.layer_id}, Symbol= {trade.asset}'
            logger.info(p)


    def update_durations(self):
        ## grab each strategy layer
        for name, layer in self.layers.items():
            ## grab each position in the strategy layer
            for symbol, position_layer in layer.items():
                position_layer.update_duration()

    def get_position_layer(self, layer, symbol=None):

        ## complete_layer = a dict of PositionLayer objects, keyed by symbol
        complete_layer = self.layers.get(layer)
        if not symbol:
            return complete_layer

        ## single out specific symbol in the layer
        if complete_layer is not None:
            return complete_layer.get(symbol, {})


    def from_dict(self, layers_dict):
        ## dict of dicts - index = Symbol, dict=(price, position, duration)
        # OLD self.layers = layers_dict
        self.layers = self.dict_to_layers(layers_dict)

    def to_dict(self):
        # OLD return self.layers
        return self.layers_to_dict()

    def dict_to_layers(self, layers_dict):
        m = {}
        for layer_tag, layer_positions_dict in layers_dict.items():
            m[layer_tag] = {}
            for symbol, position_layer_dict in layer_positions_dict.items():
                m[layer_tag][symbol] = PositionLayer(position_layer_dict)
        return m

    def layers_to_dict(self):
        m = {}
        for layer_tag, layer_positions_dict in self.layers.items():
            m[layer_tag] = {}
            for symbol, position_layer in layer_positions_dict.items():
                m[layer_tag][symbol] = position_layer.to_dict() 
        return m

    def get_layer_ids(self):
        """
        Return the ids of all currently active layers.

        A layer is active as long as it holds at least one open position.
        LayerMgr removes a layer entry automatically when its position reaches
        zero (see the position==0 block in add()), so this list always reflects
        only layers that still have something open.

        A single-layer strategy will return [strategy_id] here.
        A multi-layer strategy will return one id per active layer.
        """
        return list(self.layers.keys())


class PosNode():
    def __init__(self, name):
        self.name = name
        self.position = 0
        self.duration = 0
        self.price = 0
        self.last_trade_id = '' 
        self.timestamp = ''

    def clear(self):
        self.position = 0
        self.duration = 0
        self.price = 0
        self.last_trade_id = '' 
        self.timestamp = ''

    def to_dict(self):
        m = dict()
        for k, v in self.__dict__.items():
            m.update({k:v})
        return m

    def from_dict(self, json_dict):
        for k, v in json_dict.items():
            self.__dict__.update({k:v})
        return self

    def copy(self):
        new_node = PosNode(self.name)
        new_node.__dict__.update(self.__dict__)
        return new_node

    def stamp_with_time(self):
        now = datetime.now()
        self.timestamp = now.strftime("%Y%m%d-%H:%M:%S")

    def __str__(self):
        pairs = [f'{k}={v}' for k,v in self.to_dict().items()]
        return f"PosNode({', '.join(pairs)})"

    def __repr__(self):
        return self.__str__()

class Order():
    def __init__(self):
        self.order_id = None
        self.symbol = None
        self.side = None 
        self.order_type = None 
        self.qty = 0
        self.open_qty = 0
        self.filled_qty = 0
        self.filled_avg_price = 0
        self.stop_price = 0
        self.limit_price = 0
        self.timestamp = None
        self.layer_id = None
        self.status = None
        self.fa_group = None

    def to_dict(self):
        m = dict()
        for k, v in self.__dict__.items():
            m.update({k:v})
        return m

    def from_dict(self, json_dict):
        for k, v in json_dict.items():
            self.__dict__.update({k:v})
        return self

    def copy(self):
        new_order = Order()
        new_order.__dict__.update(self.__dict__)
        return new_order

    def stamp_with_time(self):
        now = datetime.now()
        self.timestamp = now.strftime("%Y%m%d-%H:%M:%S")

    def __str__(self):
        pairs = [f'{k}={v}' for k,v in self.to_dict().items()]
        return f"Order({', '.join(pairs)})"

    def __repr__(self):
        return self.__str__()

##
## capital allocation node
## shows cash available 'cash' to total cash allocated across all accounts for the strategy 'total_cash'
##

class AllocNode():
    def __init__(self, account_id):
        self.account_id = account_id
        self.cash = 0
        self.timestamp = ''
        self.targets = {}
        self.positions = {}
        self.layer_mgr = LayerMgr()

    def get_target(self, symbol):
        return self.targets.get(symbol, 0)

    def add_target(self, symbol, amt):
        self.targets[symbol] = amt

    def del_target(self, symbol):
        try:
            del self.targets[symbol]
        except:
            pass
            
    def get_position(self, symbol):
        return self.positions.get(symbol, 0)

    def get_position_layer(self, layer, symbol=None):
        return self.layer_mgr.get_position_layer(layer, symbol)

    def add_position(self, symbol, amt):
        self.positions[symbol] = amt

    def del_position(self, symbol):
        try:
            del self.positions[symbol]
        except:
            pass

    def update_layer(self, symbol, amt, price, layer_id):
        self.layer_mgr.apply(symbol, amt, price, layer_id)

    def update_durations(self):
        self.layer_mgr.update_durations()

    def from_dict(self, json_dict):
        for k, v in json_dict.items():
            if k != 'position_layers':
                self.__dict__.update({k:v})
        ## handling special case for LayerMgr object
        self.layer_mgr.from_dict( json_dict.get('position_layers', {}) )
        return self

    def to_dict(self):
        m = dict()
        for k, v in self.__dict__.items():
            m.update({k:v})
        ## handling special case for LayerMgr object
        m['position_layers'] = self.layer_mgr.to_dict()
        return m

    def copy(self):
        new_node = AllocNode(self.account_id)
        new_node.__dict__.update(self.__dict__)
        new_node.layer_mgr = LayerMgr()
        new_node.layer_mgr.from_dict( self.layer_mgr.to_dict() )
        return new_node

    def stamp_with_time(self):
        now = datetime.now()
        self.timestamp = now.strftime("%Y%m%d-%H:%M:%S")

    def __str__(self):
        pairs = [f'{k}={v}' for k,v in self.to_dict().items()]
        return f"AllocNode({', '.join(pairs)})"

    def __repr__(self):
        return self.__str__()


class Trade():
    def __init__(self, trade_id=None ):
        ## execution id for the trade
        self.trade_id = trade_id
        ## originating order_id associated with this trade,
        ## ie multiple trade_ids can belong to a single order_id (split fills)
        self.order_id = None
        self.layer_id = None
        self.side = None
        self.asset = None
        self.units = 0
        self.price = 0
        self.commission = 0
        self.fees = 0
        self.broker = None
        self.exchange = None
        self.timestamp = None

    def to_dict(self):
        m = dict()
        for k, v in self.__dict__.items():
            m.update({k:v})
        return m

    def _float_from_string(self, value):
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                cleaned_amount = value.replace('$', '').replace(',', '')
                return float(clean_amount)
            except Exception as exec:
                logger.error('cannot convert {value} to a number.')
                raise Exception

    ## make sure all assigned values are of the correct type
    def __setattr__(self, name, value):
        if name in ['units', 'price', 'commissions', 'fees']:
            value = self._float_from_string(value)
        super().__setattr__(name, value)

    def from_dict(self, json_dict):
        for k, v in json_dict.items():
            if k in ['units', 'price', 'commissions', 'fees']: v = self._float_from_string(v)
            self.__dict__.update({k:v})
        return self

    def copy(self):
        new_trade = Trade()
        new_trade.__dict__.update(self.__dict__)
        return new_trade

    def stamp_timestamp(self):
        now = datetime.now()
        self.timestamp = now.strftime('%Y%m%d-%H:%M:%S')

    def __str__(self):
        pairs = [f'{k}={v}' for k,v in self.to_dict().items()]
        return f"Trade({', '.join(pairs)})"
        
    def __repr__(self):
        return self.__str__()



class PosMgr():
    def __init__(self):
        self.strategy_id = None
        self.universe = None
        self.order_ledger = dict()
        self.unmatched_orders = {} 
        self.order_monitor = IB.OrderMonitor()

        ## current names to trade w/ current positions
        self.positions = []
        ## incremental detail of position changes
        self.position_detail = []
        ## position summary 
        self.layer_mgr = LayerMgr()
        ## allocations per account for all the strategy trades
        self.allocations = []
        ## total cash available to initiate new positions
        self.total_allocation = 0
        ## trades
        self.trades = []
        ## security master
        self.security_master = SecMaster(f'{DATA_DIR}/security_master.json')


    def get_contract_id(self, symbol):
        contract_id = None
        try:
            contract_id = self.security_master.get_sec_def(symbol)['contract_id']
        except:
            contract_id = IB.symbol_to_contract_id(symbol)
            if contract_id:
                self.security_master.add(symbol, contract_id)

        return contract_id

    def position_count(self):
        return len(self.positions)

    def get_position(self, symbol):
        for pos_node in self.positions:
            if pos_node.name == symbol:
                return pos_node
        return None

    def get_position_layer(self, layer_id, symbol=None):
        return self.layer_mgr.get_position_layer(layer_id, symbol)

    def get_position_layers(self):
        return self.layer_mgr.to_dict()

    def get_layer_ids(self):
        """
        Return the ids of all currently active strategy layers.
        Delegates to LayerMgr.get_layer_ids().

        Used by the kill/flatten loop in strategy2 to enumerate every layer
        that needs to be unwound without hardcoding layer names.
        """
        return self.layer_mgr.get_layer_ids()

    def get_open_orders_for_layer(self, layer_id):
        """
        Return all open orders that belong to a specific layer.

        This is the per-layer open-order guard used by target-calc functions.
        Because every order carries an unambiguous layer_id (one order per
        layer per symbol), this filter is exact.

        Checking only this layer's open orders means other layers can proceed
        independently — Layer B is never blocked by Layer A's pending order.
        For a single-layer strategy (layer_id == strategy_id), behavior is
        identical to the strategy-wide get_open_orders() check.
        """
        open_orders = self.get_open_orders()
        layer_orders = [o for o in open_orders if o.layer_id == layer_id]
        logger.debug(
            f'get_open_orders_for_layer: layer={layer_id}, '
            f'open_count={len(layer_orders)}'
        )
        return layer_orders

    ## ask IB for any new live order information
    def check_orders(self):
        fill_package = self.order_monitor.check_orders()
        self.update_order_ledger(fill_package)

        ## tag fill with layer_id
        enriched_fills = list()
        for f in fill_package['fills']:
            order_id = f['order_id']
            try:
                order = self.order_ledger[ order_id ]
                nw = f.copy()
                nw['layer_id'] = order.layer_id
                enriched_fills.append(nw)
            except:
                logger.warning(f'failed to enrich order details  for {order_id}')

        return enriched_fills 

    def get_open_orders(self):
        if self.order_ledger:
            open_filter = lambda x: True if x.status == OrderStatus.OPEN else False
            open_orders = filter(open_filter, list(self.order_ledger.values()))
            return list(open_orders)
        return []

    def get_allocations(self):
        return self.allocations 

    def get_positions(self):
        return self.positions 

    def get_previous_trade_date(self):
        today = datetime.today().date()
        holidays = calendar_calcs.load_holidays()
        dt = calendar_calcs.prev_trading_day(today, holidays)
        if dt is not None:
            return dt
        return None

    ## validate that the YYYYMMDD tag in the filename is a valid date
    def _validate_file_date(self, filename):
        ## position filename format = <Strategy_id>.positions.<YYYYMMDD>.json
        date_string = filename.split('.')[2]
        try:
            datetime.strptime(date_string, "%Y%m%d")
            return True
        except ValueError:
            logger.error(f'incorrect date_string format for file: {filename}')
            return False

    ## read position node file:
    ## position filename format = <Strategy_id>.positions.<YYYYMMDD>.json
    def read_positions_and_allocations(self):

        ## Directory where the files are located
        directory = f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/positions/'

        ## Regex pattern to match the file names
        ## position filename format = <Strategy_id>.positions.<YYYYMMDD>.json
        regex_pattern = fr'{self.strategy_id}\.positions\.\d{{8}}\.json'

        sorted_files = []
        if os.path.exists(directory):
            matching_files = [f for f in os.listdir(directory) if re.match(regex_pattern, f)]
            valid_files = [f for f in matching_files if self._validate_file_date(f)]
            sorted_files = sorted(valid_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))

        pos_map = dict()
        pos_nodes = []
        alloc_nodes = []
        total_allocation = 0
        if len(sorted_files) > 0:
            most_recent_file = sorted_files[-1]
            logger.info(f'position file: {most_recent_file}')
            file_path = os.path.join(directory, most_recent_file)
            with open(file_path, 'rb') as file:
                file_contents = file.read()
                pos_json = json.loads(file_contents)

                ## json file expected is as follows:
                ##
                ## {
                ##     'positions': [ array of PosNodes: { name, position, duration, price, last_trade_id, timestamp } ],
                ##     'position_detail': [ array of position updates: { name, side, units, old_position, new_position, timestamp } ],
                ##     'position_layers': dict of position breakdown by strategy 
                ##     'allocations:' [ array of AllocNode: { accountId, cash} ]
                ##     'total_allocation': sum of cash allocations
                ## }
                ##

                ## map all names to position nodes found
                ## return None if 'positions' or 'allocations' not found
                pos_nodes = pos_json.get('positions', [])
                if len(pos_nodes) > 0:
                    for node in pos_nodes:
                        name = node['name']
                        n = PosNode(name).from_dict(node)
                        try:
                            pos_map[name].append(n)
                        except KeyError:
                            pos_map[name] = [n]

                self.layer_mgr.from_dict( pos_json.get('position_layers', {}) )
                alloc_nodes = pos_json.get('allocations', [])
                total_allocation = pos_json.get('total_allocation',0)

        else:
            logger.warning(f'no matching position files found in {directory} for strategy_id: {self.strategy_id}.')

        return pos_map, alloc_nodes, total_allocation


    ## recover position detail and trade detail information from
    ## the CURRENT trading day - in situations where there was a program restart
    def recover_current_detail(self):
        today = datetime.today().strftime("%Y%m%d")

        directory = f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/positions/'
        pos_file = f'{self.strategy_id}.positions.{today}.json'

        pos_detail = []
        file_path = os.path.join(directory, pos_file)
        if os.path.exists(file_path):
            with open(file_path, 'rb') as file:
                file_contents = file.read()
                pos_json = json.loads(file_contents)
                pos_detail = pos_json.get('position_detail', [])

        directory = f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/trades/'
        trade_file = f'{self.strategy_id}.trades.{today}.json'
        orders_file = f'{self.strategy_id}.orders.{today}.json'

        trade_detail = []
        file_path = os.path.join(directory, trade_file)
        if os.path.exists(file_path):
            with open(file_path, 'rb') as file:
                file_contents = file.read()
                trade_json = json.loads(file_contents)
                trade_detail = trade_json.get('trades', [])

        ## recover the orders from today's order file
        self.order_ledger.clear()
        file_path = os.path.join(directory, orders_file)
        if os.path.exists(file_path):
            with open(file_path, 'rb') as file:
                file_contents = file.read()
                orders_json = json.loads(file_contents)
                for order_dict in orders_json:
                    recovered_order = Order()
                    self.order_ledger[ order_dict['order_id'] ] = recovered_order.from_dict(order_dict)

        return pos_detail, trade_detail


    def _positions_to_df(self):
        if len(self.positions) > 0:
            ## get the attribute names of the first PosNode to use as columns
            cols = self.positions[0].keys()
            df = pandas.DataFrame(columns=cols, data=self.positions)
            return df

        return None


    def _fetch_cash_allocations(self, strategy_id):

        current_date = datetime.today().strftime("%Y-%m-%d")

        # Connect to the 'Operations' database
        connection = mysql.connector.connect(
            host=MYSQL_HOSTNAME,  # Replace with your MySQL server host
            user="root",  # Replace with your MySQL username
            password=MYSQL_PASSWORD,  # Replace with your MySQL password
            database="Operations"
        )

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        logger.info(f'alert: fetching new capital available for Strategy={strategy_id}')

        # Execute SQL statement to drop the table if it exists
        cursor.execute("DROP TABLE IF EXISTS liveCash")
        connection.commit()

        # Create temporary table of most recent allocation
        create_live_cash = """
			CREATE TEMPORARY TABLE liveCash AS
            SELECT t.date, t.accountId, t.liveEquity, t.timestamp FROM AccountValue AS t
            JOIN (SELECT accountId, date, MAX(timestamp) AS maxTs FROM AccountValue GROUP BY accountId, date) AS q
            ON t.accountId = q.accountId 
			AND q.maxTs = t.timestamp
        """
        cursor.execute(create_live_cash)
        connection.commit()

        # Fetch liveCash allocations associated with strategyId and current date
        query = """
            SELECT c.accountId, c.liveEquity, c.timestamp FROM liveCash AS c
			JOIN StrategyAccount AS s
			ON s.accountId = c.accountId
			WHERE s.strategyId = %s AND c.date = %s
		"""

        cursor.execute(query, [strategy_id, current_date])

        # Fetch all the results
        results = cursor.fetchall()

        # Print the retrieved data
        if results:
            for row in results:
                account_id, cash, ts = row
                ## db timestamps are always in UTC
                timestamp = convert_timestamp(ts).strftime('%Y%m%d-%H:%M:%S')
                logger.info(f"{strategy_id}: accountId: {account_id}, cash: {cash}, timestamp: {timestamp}")
        else:
            err = f"No accounts found for strategyId '{strategy_id}'."
            logger.critical(err)
            raise RuntimeError(err)

        # Close the cursor and connection
        cursor.close()
        connection.close()

        total_cash = 0
        alloc_nodes = []
        for row in results:
            account_id, cash, ts = row
            timestamp = convert_timestamp(ts).strftime('%Y%m%d-%H:%M:%S')
            alloc_node = AllocNode(account_id)
            alloc_node.cash = float(cash)
            alloc_node.timestamp = timestamp
            total_cash += float(cash)
            alloc_nodes.append(alloc_node)

        logger.info(f"{strategy_id}: trade_capital: {total_cash}")
        if total_cash <= 0:
            err = f"total_cash = {total_cash} for {strategy_id}"
            logger.critical(err)
            raise RuntimeError(err)

        return alloc_nodes, total_cash


    ## master method used to load universe and positions for trading.
    def initialize(self, strategy_id, universe_list):

        self.strategy_id = strategy_id
        self.universe = set(universe_list)
        ## give back a map of pos nodes, indexed by names,
        ## and the allocation breakdown for accounts
        ## and the sum of all allocations
        pos_map, alloc_nodes, total_allocation = self.read_positions_and_allocations()

        ## recover CURRENT day detail in the case of program restart
        self.position_detail, self.trades = self.recover_current_detail()

        opening_total = 0

        newbies= []
        for name in self.universe:
            pos_nodes = pos_map.get(name)
            if pos_nodes is None:
                ## new name to trade in the universe
                new_node = PosNode(name)
                self.positions.append(new_node)
                newbies.append(new_node)
            else:
                items = len(pos_nodes)
                ## add singular position definition
                if items == 1:
                    open_node = pos_nodes[0]
                    open_pos = open_node.position
                    ## clear past history for empty position on a new trading day
                    if open_pos == 0:
                        open_node.clear()
                    self.positions.append(open_node)
                    opening_total += abs(open_pos) 
                else:
                    ## map returned multiple pos nodes for a specific name
                    logger.warning(f'duplicate positions found for {name}.')
                    logger.warning(json.dumps(pos_nodes, ensure_ascii=False, indent =4 ))

        if len(newbies) > 0:
            logger.info(f'created following new position nodes:')
            nn = [ x.to_dict() for x in newbies ]
            logger.info(json.dumps(nn, ensure_ascii=False, indent=4))

        if len(self.positions) > 0:
            logger.info(f'current position nodes:')
            oo = [ x.to_dict() for x in self.positions ]
            logger.info(json.dumps(oo, ensure_ascii=False, indent=4))

        ## position names in position file - but not in current universe
        zombies = set(pos_map.keys()).difference(self.universe)

        if len(zombies) > 0:
            zz = []
            for name in zombies:
                zombie_nodes = pos_map.get(name)
                zz.extend([z.to_dict() for z in zombie_nodes])
            msg = f'universe loaded = {self.universe}\n'
            msg += 'zombie positions not in current universe found.\n'
            msg += json.dumps(zz, ensure_ascii=False, indent =4 )
            logger.warning(msg)

        ## check allocations
        if opening_total == 0:
            ## fetch new allocations if no open positions
            ## for a clean slate of zero positions
            alloc_nodes, self.total_allocation = self._fetch_cash_allocations(self.strategy_id)
        else:
            logger.info('using previous allocations on open positions')
            ## self.total_allocation = 0
            logger.info(json.dumps(alloc_nodes, ensure_ascii=False, indent=4))
            converted_allocs = []
            for alloc in alloc_nodes:
                aa= AllocNode(alloc['account_id'])
                converted_allocs.append(aa.from_dict(alloc))
            alloc_nodes = converted_allocs
            self.total_allocation = total_allocation

        self.allocations = alloc_nodes

        ## print out starting state 
        ## convert to a list of position dicts
        pos_msg = f'POSITIONS:\n'
        if self.positions:
            position_data = [ x.to_dict() for x in self.positions ]
            position_data = sorted( position_data, key=lambda x: x['name'])
            pos_msg += json.dumps(position_data, ensure_ascii=False, indent=4)
        else:
            pos_msg += "-- empty --"
            
        alloc_msg = f'ALLOCATIONS:\n'
        if self.allocations:
            alloc_data = [ x.to_dict() for x in self.allocations ]
            alloc_data = sorted( alloc_data, key=lambda x: x['cash'])
            alloc_msg += json.dumps(alloc_data, ensure_ascii=False, indent=4)
        else:
            alloc_msg += "-- empty --"

        def _to_dict(lst):
            f = lambda x: x if isinstance(x,dict) else x.to_dict()
            return [ f(x) for x in lst ]

        ## order_ledger in a dict of order dicts
        order_msg = f'ORDERS:\n'
        if self.order_ledger: 
            order_data = sorted( _to_dict(self.order_ledger.values()), key=lambda x: x['timestamp'])
            order_msg += json.dumps(order_data, ensure_ascii=False, indent=4) 
        else:
            order_msg += '-- empty --'

        init_msg = f'\n\n{pos_msg}\n\n{order_msg}\n\n{alloc_msg}\n'
        logger.critical(init_msg)


    def create_directory(self, directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    def get_order_ledger(self):
        ledger_copy = {}
        if self.order_ledger:
            for k, v in self.order_ledger.items():
                ledger_copy[k] = v.copy()
        return ledger_copy

    def write_orders(self, now):
        def _to_dict(lst):
            f = lambda x: x if isinstance(x,dict) else x.to_dict()
            return [ f(x) for x in lst ]

        ## sorts orders by timestamp

        sorted_orders = sorted( _to_dict(self.order_ledger.values()), key=lambda x: x['timestamp'])

        tdy = now.strftime("%Y%m%d")
        newdir =f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/trades/'
        self.create_directory(newdir)
        orders_file = f'{newdir}/{self.strategy_id}.orders.{tdy}.json'

        ## lock file to prevent race conditions between order send and order fill
        with open(orders_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            s = json.dumps(sorted_orders, ensure_ascii=False, indent =4 )
            f.write(s + '\n')
            fcntl.flock(f, fcntl.LOCK_UN)

        logger.info(f'{orders_file} updated')

    def write_unmatched(self, now):

        tdy = now.strftime("%Y%m%d")
        newdir =f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/trades/'
        self.create_directory(newdir)
        unmatched_file = f'{newdir}/{self.strategy_id}.unmatched.{tdy}.json'

        ## lock file to prevent race conditions between order send and order fill
        with open(unmatched_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            s = json.dumps(self.unmatched_orders, ensure_ascii=False, indent =4 )
            f.write(s + '\n')
            fcntl.flock(f, fcntl.LOCK_UN)

        logger.info(f'{unmatched_file} updated')

    def write_positions(self, now):
        def _to_dict(lst):
            f = lambda x: x if isinstance(x,dict) else x.to_dict()
            return [ f(x) for x in lst ]

        ## sorts current detail by name, then timestamp
        sorted_detail = sorted(self.position_detail, key=lambda x: (x['name'], x['timestamp']))

        #ts = now.strftime("%Y%m%d-%H%M%S")
        tdy = now.strftime("%Y%m%d")
        newdir =f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/positions/'
        self.create_directory(newdir)
        position_file = f'{newdir}/{self.strategy_id}.positions.{tdy}.json'
        pp = { 'strategy_id': self.strategy_id,
               'positions': _to_dict(self.positions),
               'position_layers': self.layer_mgr.to_dict(),
               'position_detail': sorted_detail,
               'allocations': _to_dict(self.allocations),
               'total_allocation': self.total_allocation
             }
        with open(position_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            s = json.dumps(pp, ensure_ascii=False, indent =4 )
            f.write(s + '\n')
            fcntl.flock(f, fcntl.LOCK_UN)

        logger.info(f'{position_file} updated')


    def write_trades(self, now):
        def _to_dict(lst):
            f = lambda x: x if isinstance(x,dict) else x.to_dict()
            return [ f(x) for x in lst ]

        ## sorts current by name, then timestamp
        sorted_trades = sorted( _to_dict(self.trades), key=lambda x: (x['asset'], x['timestamp']))

        tdy = now.strftime("%Y%m%d")
        newdir =f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/trades/'
        self.create_directory(newdir)
        trade_file = f'{newdir}/{self.strategy_id}.trades.{tdy}.json'
        tt = { 'strategy_id': self.strategy_id, 'trades': sorted_trades, 'allocations': _to_dict(self.allocations), 'total_allocation': self.total_allocation }
        with open(trade_file, 'w') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            s = json.dumps(tt, ensure_ascii=False, indent =4 )
            f.write(s + '\n')
            fcntl.flock(f, fcntl.LOCK_UN)

        logger.info(f'{trade_file} updated')


    ## hold a dictionary of open orders
    def register_order(self, order_info):
        """
        Record a submitted broker order in the order ledger.

        order_info must contain 'layer_id', which identifies the strategy
        layer that originated this order.  For single-layer strategies this
        will be the strategy_id.  For multi-layer strategies it will be the
        specific layer tag (e.g. 'LEX', 'SP1').

        layer_id on the Order object is what makes get_open_orders_for_layer()
        exact — every order belongs to exactly one layer, with no ambiguity.
        """
        order_id = order_info['order_id']
        order = Order()
        order.order_id = order_id
        order.symbol = order_info['symbol']
        order.qty = order_info['quantity']
        order.open_qty = order.qty
        order.side = order_info['side']
        order.status = OrderStatus.OPEN
        order.layer_id = order_info['layer_id']

        #optionals
        order.order_type = order_info.get('order_type')
        order.stop_price = order_info.get('stop_price')
        order.limit_price = order_info.get('limit_price')
        order.fa_group = order_info.get('fa_group')
        order.stamp_with_time()

        self.order_ledger[ order_id ] = order
        logger.info(f'writing order {order_id} to register')
        self.write_orders( datetime.now() )

        logger.info(f'order: {order_id} logged to order register')
        register_dump = json.dumps(list(self.order_ledger.keys()), indent=4)
        logger.info(f'current order_ledger:\n{register_dump}')

        logger.info(
            f'order {order_id} attributed to layer={order.layer_id}, '
            f'symbol={order.symbol}, side={order.side}, qty={order.qty}'
        )


    ## updates the status of a given list of orders
    ## this is used to modify the order ledger when cancels or rejects
    ## are returned from polling for order executions
    def update_order_ledger(self, fill_package):
        write_update = False
        unmatched_found = False

        misfits = {}
        misfits[OrderStatus.CANCELLED] = fill_package['cancels']
        misfits[OrderStatus.REJECTED] = fill_package['rejects']

        ## update cancelled and rejected orders
        for order_status, orders in misfits.items(): 
            for order in orders:
                order_id = order['order_id']
                try:
                    working_order = self.order_ledger[order_id]
                    if order_status != working_order.status:
                        working_order.status = order_status
                        update_msg = f'updating order {order_id} status= {order_status}.\n'
                        update_msg += f'{working_order}'
                        logger.info(update_msg)
                        write_update = True
                except KeyError:
                    if order_id not in self.unmatched_orders:    
                        self.unmatched_orders[order_id] = order
                        unmatched_found = True
                        logger.error(f'Update {order_id} status= {order_status} failed. Order not found.')

        ## update fill amount and avg price
        for order in fill_package['raw_orders']:
            order_id = order['order_id']
            try:
                filled_qty = order['filledQuantity']
                working_order = self.order_ledger[order_id]
                if working_order.filled_qty != filled_qty:
                    working_order.filled_qty = filled_qty
                    working_order.filled_avg_price = order['avgPrice']
                    write_update = True
            except KeyError:
                if order_id not in self.unmatched_orders:    
                    self.unmatched_orders[order_id] = order
                    unmatched_found = True
                    logger.error(f'Update {order_id} status= {order_status} failed. Order not found.')
       
        now = datetime.now()

        if write_update:
            self.write_orders( now )

        if unmatched_found:
            self.write_unmatched( now )


    def purge_order_groups(self):
        fa_confirm = 'removing fa_groups:\n'
        remove_groups = []
        for order in self.order_ledger.values():
            if order.status != OrderStatus.OPEN and order.fa_group:
                remove_groups.append(order.fa_group)

        for fa_group in remove_groups:
            try:
                deleted = IB.delete_allocation_group(fa_group)
                fa_confirm += f'{fa_group} delete response:\n{json.dumps(deleted, indent=4)}\n'
            except:
                fa_confirm += f'{fa_group} delete response: FAILED\n'
        logger.info(fa_confirm)


    def _calc_avg_price(self, curr_pos, curr_price, new_pos, new_price):
        x = abs(curr_pos)
        y = abs(new_pos)
        avp = (( x * curr_price ) + ( y* new_price)) / ( x+ y)
        return avp


    def update_positions(self, pos_node, trade_obj):

        self.layer_mgr.add(trade_obj)

        new_node = pos_node.copy()
        if pos_node.position > 0:
            if trade_obj.side == TradeSide.BUY:
                new_node.price = self._calc_avg_price(pos_node.position, pos_node.price, trade_obj.units, trade_obj.price)
                new_node.position += trade_obj.units
            elif trade_obj.side == TradeSide.SELL:
                new_node.position -= trade_obj.units
                if new_node.position < 0:
                    new_node.price = trade_obj.price

        ## manage short positions
        elif pos_node.position  < 0:
            if trade_obj.side == TradeSide.SELL:
                new_node.price = self._calc_avg_price(pos_node.position, pos_node.price, trade_obj.units, trade_obj.price)
                new_node.position -= trade_obj.units
            elif trade_obj.side == TradeSide.BUY:
                new_node.position += trade_obj.units
                if new_node.position > 0:
                    new_node.price = trade_obj.price

        ## start new position
        elif pos_node.position == 0:
            new_node.position = trade_obj.units if trade_obj.side == TradeSide.BUY else -(trade_obj.units)
            new_node.price = trade_obj.price

        ## update time of new position
        new_node.timestamp = trade_obj.timestamp

        ## clear out node if position == 0
        if new_node.position == 0:
            new_node.clear()

        ## update the new pos_node with the last affecting trade
        new_node.last_trade_id = trade_obj.trade_id

        ## record trade that affected the current position
        pos_detail = dict()
        pos_detail['name'] = new_node.name
        pos_detail['current_position'] = new_node.position
        pos_detail['avg_price'] = new_node.price
        pos_detail['prev_position'] = pos_node.position
        pos_detail['side'] = trade_obj.side
        pos_detail['trade_price'] = trade_obj.price
        pos_detail['units'] = trade_obj.units
        pos_detail['trade_id'] = trade_obj.trade_id
        pos_detail['layer_id'] = trade_obj.layer_id
        pos_detail['timestamp'] = trade_obj.timestamp

        return new_node, pos_detail

    ## update duration on open positions (AT EOD)
    def update_durations(self):
        for pos_node in self.positions:
            if pos_node.position != 0:
                pos_node.duration += 1
                d = pos_node.duration
                logger.info(f'updated duration: {pos_node.name} position:{pos_node.position} duration= {d}')

        ## self.allocations = list(AllocNodes)
        for alloc in self.allocations:
            alloc.update_durations()

        ## update top level durations
        self.layer_mgr.update_durations()

        now = datetime.now()
        self.write_positions(now)

    def write_pnl(self, pnls):
        ## pnls = [ pnl_dict1, pnl_dict2, ... ]
        if len(pnls) == 0:
            logger.info(f'no pnl events today. nothing to update.')
            return

        df_pnls = pandas.DataFrame(pnls)

        tdy = datetime.now().strftime("%Y%m%d")
        newdir = f'{PORTFOLIO_DIRECTORY}/{self.strategy_id}/trades/'
        self.create_directory(newdir)

        pnl_file = f'{newdir}/{self.strategy_id}.pnl.{tdy}.csv'
        if os.path.exists(pnl_file):
            existing_df = pandas.read_csv(pnl_file)
            df_pnls = pandas.concat([existing_df, df_pnls], ignore_index=True)

        df_pnls = df_pnls.sort_values(by='timestamp', ascending=True)
        df_pnls['realized_pnl'] = df_pnls['realized_pnl'].round(2)
        df_pnls['unrealized_pnl'] = df_pnls['unrealized_pnl'].round(2)

        df_pnls.to_csv(pnl_file, index=False)

        logger.info(f'{pnl_file} updated')

    ## the distribution algorithm based on indv account target amounts
    ## total_shares = total amount of shares filled
    ## requested shares = [tgt_for_acct1, tgt_for_acct2, ...]
    ## unwind control priority allocation for building and closing positions
    def _allocate_fill_amt(self, total_shares, requested_shares, unwind=False):
        ## need to force total_shares to int - for the range(remaining_shares)
        ## statement, range only deals in integers
        total_shares = int(total_shares)
        total_requested_shares = tsum = sum(requested_shares)
        if total_shares * total_requested_shares < 0:
            logger.critical(f'fill amount= {total_shares} OPPOSITE targeted shares= {tsum}:{requested_shares}.')
            raise ValueError

        ## fill amount matches sum of total allocation
        if total_shares == total_requested_shares:
            allocated_shares = requested_shares.copy()
            return allocated_shares

        ## distribute a zero amount
        if total_shares == 0:
            return [ 0 ] * len(requested_shares)

        ## convert all negative values to positives
        ## to do proper allocations - then revert back
        short = False
        if total_shares < 0:
            requested_shares = [ abs(x) for x in requested_shares ]
            total_requested_shares = sum(requested_shares)
            total_shares = abs(total_shares)
            short = True

        proportions = [share / total_requested_shares if total_requested_shares > 0 else 0 for share in requested_shares]
        allocated_shares = [max(1 if requested_shares[i] > 0 else 0, int(total_shares * proportion)) for i, proportion in enumerate(proportions)]

        remaining_shares = total_shares - sum(allocated_shares)

        ## handle largest proportion first when building a position or closing a position
        ## distribute the remaining shares one at a time to successive key_proportion_index

        _pick = min if unwind else max
        for _ in range(remaining_shares):
            key_proportion_index = proportions.index(_pick(proportions))
            allocated_shares[key_proportion_index] += 1
            proportions[key_proportion_index] = 0  # Exclude this account in the next iteration

        if short: allocated_shares = [ -x for x in allocated_shares ]
        return allocated_shares
 

    def _realized_position(self, curr_pos, prev_pos):
        if curr_pos * prev_pos >= 0:
            return prev_pos - curr_pos
        ## only portion realized is the amount
        ## before crossing ZERO.
        return prev_pos

    def _report_allocation_differences(self, symbol, account_ids, house_allocs, override_allocs):
        if None in override_allocs:
            logger.critical(f'None values in override allocations!:\n{override_allocs}')
            ## DO NOT use overrides
            return False

        logger.info(f'\nChecking differences btwn PosMgr allocs vs, override func allocs.')
        data = list( zip(account_ids, house_allocs, override_allocs) )
        allocs = [ dict(account=a, alloc=h, override=v) for a,h,v in data ]
        logger.info(f'\n{json.dumps(allocs, indent=4)}')
        diff = sum([ abs(h-v) for _, h, v in data])
        
        if diff != 0:
            report = json.dumps(dict(symbol=symbol, allocations=allocs), indent=4)
            logger.critical(f'share allocation difference:\n{report}')
        else:
            logger.info(f'\nPosMgr v override allocs = NO DIFFERENCES')
        
        ## use overrides
        return True


    def distribute_positions(self, symbol, curr_price, curr_pos, prev_pos, override_alloc_func):

        target_map = {}
        position_map = {}
        account_ids = []
        ## self.allocations = list(AllocNodes)
        for alloc in self.allocations:
            target_map.update( { alloc.account_id: alloc.get_target(symbol) })
            position_map.update( { alloc.account_id: alloc.get_position(symbol) })
            account_ids.append( alloc.account_id )

        target_dump = json.dumps(target_map, indent=4)
        logger.info(f'\ntargets for {symbol}:\n{target_dump}')
        position_dump = json.dumps(position_map, indent=4)
        logger.info(f'\npositions for {symbol}:\n{position_dump}')

        dist_map = list(target_map.values())
        dist_type = 'targets'
        dist_dump = target_dump

        diff = curr_pos - sum(dist_map)
        if curr_pos * diff > 0:
            ## use the distribution of the old positions.
            ## this always guarantees |sum(dist_map)| > |current_position|
            dist_map = list(position_map.values())
            dist_type = 'positions'
            dist_dump = position_dump

        ## allocate position based on if you are building a position
        ## or unwinding one
        diff = curr_pos - prev_pos
        unwind = True if diff * prev_pos < 0 else False

        logger.info(f'\ndistibution based on: {dist_type.upper()} map:\n{dist_dump}')
        fill_allocs = self._allocate_fill_amt( curr_pos, dist_map, unwind)

        ## override calculated allocations !!!
        if override_alloc_func is not None:
            logger.info(f'processing fill allocations using override= {override_alloc_func}')
            override_allocs = override_alloc_func( symbol, account_ids )
            apply_override = self._report_allocation_differences( symbol, account_ids, fill_allocs, override_allocs )
            if apply_override:
                logger.info(f'applying overrides.')
                fill_allocs = override_allocs

        ## realized amts for pnl calcs
        realized_amts_map = dict() 
        for curr, prev, account_id in zip(fill_allocs, list(position_map.values()), account_ids):  
            realized_amts_map.update( {account_id: self._realized_position(curr, prev)} ) 

        ## change in positions by account
        residuals = [ curr - prev for curr, prev in zip( fill_allocs, list(position_map.values()) ) ]

        fill_info = []
        for pos, res in zip(fill_allocs, residuals):
            fill_info.append( dict(new_position=pos, residual=res) )

        fill_map = dict(zip(account_ids, fill_info))
        fill_dump = json.dumps(fill_map, indent=4)

        msg = f'\nCURRENT POSITION for {symbol}: {curr_pos}, avg_price= {curr_price}'
        msg += f'\nallocated positions by account for {symbol}:\n{fill_dump}'
        logger.info(msg)

        return fill_map, realized_amts_map


    def reset_allocation_positions(self, symbol, pos_detail, alloc_by_account_dict ):
        for acct_id, fill_info in alloc_by_account_dict.items():
            for alloc in self.allocations:
                if alloc.account_id == acct_id:
                    alloc.positions[symbol] = fill_info['new_position'] 
                    trade_details = [ symbol, fill_info['residual'], pos_detail['trade_price'], pos_detail['layer_id'] ]
                    alloc.update_layer( *trade_details ) 

    ## takes the new position and distributes it across
    ## all account allocations (AcllocNode.positions) 
    ## also records pnl events triggered by position changes
    def update_allocations(self, prev_pos_node, new_pos_node, pos_detail, override_alloc_func=None):
        symbol = prev_pos_node.name
        prev_avg_price = prev_pos_node.price
        avg_price = new_pos_node.price
        prev_pos = pos_detail['prev_position']
        curr_pos = pos_detail['current_position']
        trade_amt = pos_detail['units']
        trade_price = pos_detail['trade_price']
        timestamp = pos_detail['timestamp']
        trade_id = pos_detail['trade_id']

        logger.info(f'allocating positions for {symbol}, trade_id= {trade_id}')
        alloc_by_account_dict, realized_by_account_dict =  self.distribute_positions(symbol, \
                avg_price, curr_pos, prev_pos, override_alloc_func)

        self.reset_allocation_positions(symbol, pos_detail, alloc_by_account_dict)

        diff = curr_pos - prev_pos
        ## make sure a pnl event occurred
        if diff * prev_pos < 0:
            account_ids = list(realized_by_account_dict.keys())
            realized_positions = list(realized_by_account_dict.values())
        
            delta = trade_price - prev_avg_price
            total_pnl = self._realized_position(curr_pos, prev_pos) * delta

            cols = ['timestamp', 'account_id', 'trade_id', 'symbol', 'realized_pnl', 'unrealized_pnl']
            tot_values = [timestamp, self.strategy_id, trade_id, symbol, total_pnl, 0]

            ## add a summary line for the strategy
            pnls = [ dict(zip(cols,tot_values)) ]

            ## create individual pnl lines per account
            realized_pnl = [ delta * x for x in realized_positions ]
            for acct_id, rpnl in zip(account_ids, realized_pnl):
                values = [timestamp, acct_id, trade_id, symbol, rpnl, 0]
                ## add indv account pnls per symbol
                indv_pnl = dict(zip(cols,values))
                pnls.append(indv_pnl)

            logger.info(f'\nPNL:\n{json.dumps(pnls, indent=4)}')

            self.write_pnl( pnls )

    ## take a new trade and update positions
    ## and update position and trade files
    def update_trades(self, trade_object, conversion_func=None):

        if conversion_func is None:
            raise RuntimeError('No trade parsing/conversion func defined!') 

        logger.info(f'trade fill recieved:\n{json.dumps(trade_object, indent=4)}')
        trade_obj = conversion_func(trade_object)
        ## fix trade_dump = json.dumps(trade_obj.__dict__, ensure_ascii=False, indent=4)
        trade_dump = json.dumps(trade_obj.to_dict(), ensure_ascii=False, indent=4)
        logger.info(f'converted fill: {trade_dump}')

        ## make sure you are not re-processing the same trade
        processed_trade_ids = [ x['trade_id'] for x in self.trades ]

        if trade_obj.trade_id in processed_trade_ids:
            logger.debug(f'ignoring trade already processed:\n {trade_dump}')
            return

        ##
        ## Interactive Brokers faGroup trade allocation
        ## this assigns all subaccount per IBRK rules 
        ## safeguards have been added to handle empty fetches
        ## it tries 3 times to get complete account info -
        ## if fails after 3 times - it returns the incomplete alloc array
        ## where processing is flagged and handled downstreen
        ##
        def _ibrk_fill_alloc_func( symbol, account_ids ):
            RETRY_LIMIT = sleep_timeout = 3
            ibrk_positions = [] 
            symbol_id = self.get_contract_id(symbol)

            for retry in range(1, RETRY_LIMIT+1):
                for account_id in account_ids:
                    cur_pos = IB.current_position(symbol_id, subaccount=account_id)
                    ibrk_positions.append( cur_pos.get("position") )

                if not (None in ibrk_positions):
                    return ibrk_positions

                m = f'incomplete IBRK allocation fetch:\n{zip(account_ids,ibrk_positions)}\n'
                m += f'retry = {retry} of {RETRY_LIMIT}'
                logger.critical(m)
                if retry < RETRY_LIMIT:
                    time.sleep(sleep_timeout)
                    ibrk_positions.clear()
                    sleep_timeout *= 1.5
            
            vv = [ dict(account_id=a, ibrk_alloc=b) for a,b in list(zip(account_ids, ibrk_positions)) ]
            logger.info(f'Fetched IBRK alloc overrides:\n{json.dumps(vv,indent=4)}' )

            return ibrk_positions


        self.trades.append(trade_obj.to_dict())
        logger.info(f'captured new trade: {trade_dump}')

        ## executed trades contain the original order_id submitted
        ## and a trade_id to identify the executed trade
        ## split fill happen when 2 or more trades with unique trade_ids
        ## belong to the same parent order_id
        order_id = trade_obj.order_id
        working_order = self.order_ledger.get(order_id)
        if working_order is not None:
            open_qty = working_order.open_qty
            fill_amt = trade_obj.units
            if fill_amt > open_qty:
                err_msg = f'order_id:{order_id}, fill_amt:{fill_amt} > open_qty:{open_qty}.\n'
                err_msg += f'order_id:{order_id}, open_qty SET TO ZERO.'
                logger.critical(err_msg)
                working_order.open_qty = 0
                working_order.status = OrderStatus.FILLED
            elif open_qty > 0:
                if fill_amt < open_qty:
                    logger.warning(f'partial fill: order_id:{order_id}, target_qty:{open_qty}, fiil_amt:{fill_amt}')
                working_order.open_qty -= fill_amt
                if working_order.open_qty == 0:
                    working_order.status = OrderStatus.FILLED
                    logger.info(f'order_id:{order_id}, status = {OrderStatus.FILLED}')
            logger.info(f'updating order {order_id} in order_ledger.\n{json.dumps(working_order.to_dict(), indent=4)}')
            self.write_orders( datetime.now() )
        else:
            logger.error(f'cannot find order_id:{order_id} in order_ledger.')\

        ## IMPORTANT: trade_obj.units is ALWAYS > 0

        for idx, pos_node in enumerate(self.positions):
            if pos_node.name == trade_obj.asset:
                new_node, new_detail = self.update_positions(pos_node, trade_obj)
                self.position_detail.append(new_detail)
                self.positions[idx] = new_node
                self.update_allocations(pos_node, new_node, new_detail)

                ## IMPORTANT - not using override_alloc_func presently because position updates
                ## are happing too slowly on the IBRK side.  therefore we are going to presently
                ## trust all our internal position accounting until another solution is found.

                ## self.update_allocations(pos_node, new_node, new_detail, 
                ##                            override_alloc_func=_ibrk_fill_alloc_func)

        now = datetime.now()
        self.write_positions(now)
        self.write_trades(now)


    def mark_to_market_open_positions(self, prices ):
        
        ptable = json.dumps(prices, indent=4)
        logger.info(f'\nMARK-TO-MARKET PRICES:\n{ptable}')

        cols = ['timestamp', 'account_id', 'trade_id', 'symbol', 'realized_pnl', 'unrealized_pnl']
        timestamp = datetime.now().strftime('%Y%m%d-%H:%M:%S')

        logger.info('marking positions to market.')

        try:
            pnls = []
            for i, pos_node in enumerate(self.positions):
                if pos_node.position == 0: continue
            
                last_trade_id = f'0000000000-{i+1:04d}'   # blank trade id.
                if pos_node.last_trade_id: last_trade_id = pos_node.last_trade_id

                symbol = pos_node.name
                bid, ask = prices[symbol]
                mark_price = bid if pos_node.position > 0 else ask
                pnl = (mark_price - pos_node.price) * pos_node.position

                
                values = [timestamp, self.strategy_id, f'{last_trade_id}', symbol, 0, pnl]
                pnls.append(dict(zip(cols,values)))

                for alloc in self.allocations:
                    account_id = alloc.account_id

                    ## get current position price from the aggregate position posted
                    alloc_pos = alloc.get_position(symbol)

                    pnl = (mark_price - pos_node.price) * alloc_pos
                    values = [timestamp, account_id, f'{last_trade_id}', symbol, 0, pnl]
                
                    pnls.append(dict(zip(cols,values)))
            
            self.write_pnl( pnls )

        except Exception as exc:
            logger.critical('\n'.join(['mark-to-market calculations failed.', str(exc)]))
            raise exc


if __name__ == "__main__":

    pmgr = PosMgr()
    pmgr.initialize('Strategy1', ['AAPL','SPY','QQQ'])

    logger.info(pmgr.positions)

    fake_trade = '12513, Strategy1, BUY, SPY, 50, 419.00'
    pmgr.update_trades(fake_trade)

    #fake_trade = '12511, Strategy1, SELL, SPY, 43, 461.66'
    #pmgr.update_trades(fake_trade)
    #fake_trade = '12511, Strategy1, BUY, SPY, 120, 470.66'
    #pmgr.update_trades(fake_trade)





