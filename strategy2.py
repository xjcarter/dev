
import json
import logging
import os
import functools
import ib_endpoints2 as IB
#import ib_simulator as IB
from posmgr2 import PosMgr, TradeSide, Trade, OrderType
from sec_master import SecMaster
from datetime import datetime
from collections import defaultdict
from clockutils import unix_time_to_string
import time

# Create a logger specific to __main__ module
logger = logging.getLogger(__name__)
"""
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
FORMAT = "%(asctime)s: %(levelname)8s [%(module)15s:%(lineno)3d - %(funcName)20s ] %(message)s"
formatter = logging.Formatter(FORMAT, datefmt='%a %Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
"""

#DATA_DIR = os.getenv('DATA_DIR', '/trading/data')
DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/ibrk/lex')
PORTFOLIO_DIRECTORY = os.environ.get('PORTFOLIO_DIRECTORY', '/home/jcarter/junk/portfolio/')

def _read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            file_contents = file.read()
            json_data = json.loads(file_contents)
        return json_data
    except json.JSONDecodeError as e:
        logger.critical(f"JSON decoding error for {filename}: {e}")
        logger.critical(f"Problematic JSON file contents: {file_contents}")

def _indent(v, j):
    q = " " * j
    return q + v.replace("\n", "\n"+q)

class Strategy():

    def __init__(self, strategy_id, configuration_file, kill_strategy=False):
        self.strategy_id = strategy_id
        self.pos_mgr = PosMgr()
        self.cfg = _read_json_file(configuration_file)
        self.universe = list(set(self.cfg['universe'])) 
        self.security_master = SecMaster(f'{DATA_DIR}/security_master.json')
        self.account_info = None

        ## kill switch -
        ## when this is set to True upon strategy reboot
        ## all outstanding fills will be captured and 
        ## all outstanding orders will be cancelled
        ## then strategy exits
        self.kill = kill_strategy 

        ## read in strategy context dictionary (if needed) 
        self.context = None

        ## populate sec_master with new symbol/contract_id pairs
        for symbol in self.universe:
            if symbol not in self.security_master.symbols():
                self.get_contract_id(symbol)

        logger.info(f'strategy_id = {self.strategy_id}')
        logger.info(f'strategy config file = {configuration_file}')
        logger.info(f'universe = {self.universe}')


    # general flatten all positions
    def calc_flatten_targets(self, alloc_node, prices_dict):
        ## unwind the positions created in calc_entry_targets
        targets = []
        for symbol, bid_ask in prices_dict.items():
            bid, ask = bid_ask

            position = self.get_position(symbol)
            if position == 0 or position is None: continue

            try:

                target_amt = 0

                logger.info(
                    f'calc_flatten_targets: symbol={symbol}, '
                    f'position={position}, '
                    f'target_amt={target_amt}'
                )

                ## define the type of order you want to execute with
                ## target amount
                tgt = { 'symbol': symbol,
                        'target_amt': target_amt,
                        'order_type': OrderType.MKT,
                        'stop_price': None,
                        'limit_price': None
                }
                targets.append( tgt )

            except (TypeError, KeyError):
                logger.critical(f'KILL: symbol: {symbol}, position={position} unwind failed')

        return targets


    def _load_context(self):
        return None 

    def _save_context(self):
        pass


    ## additonal initialization before trading
    def open_trading_book(self):

        self.context = self._load_context()

        self.pos_mgr.initialize(self.strategy_id, self.universe)
        logger.info('pos mgr intialized.')

        if self.kill:
            logger.critical('kill loop activated.')

            logger.info('KILL: checking for unprocessed fills.')
            TIMEOUT= 10 
            while TIMEOUT > 0:
                fills =  self.check_orders()
                for fill in fills:
                    self.process_fill(fill)
                time.sleep(2.0)
                TIMEOUT -= 1
            logger.info('KILL: fill check completed.')

            logger.info('KILL: cancelling open orders.')
            orders = self.get_open_orders()
            if orders:
                ## NOTE: you must call /iserver/accounts endpoint (IB.get_accounts)
                ## before attempting to cancel orders
                logger.info('querying accounts before cancels')
                IB.get_accounts()
                time.sleep(1.0)

                for order in orders:
                    ## only cancel opens that haven't been filled at all. 
                    if order.open_qty != 0: 
                        logger.info(f'cancelling: {order}')
                        IB.cancel_order(order.order_id, order.fa_group)
                logger.info('KILL: order cancels completed.')
            else:
                logger.info('KILL: no orders to cancel.')

            time.sleep(3.0)
            
            ## closing open positions
            logger.info('KILL: closing open positions.')

            flatten_target_map = self.get_targets( self.calc_flatten_targets )
            ts = datetime.now().strftime('%Y%m%d-%H:%M:%S')
            self.send_orders( flatten_target_map, order_notes=f'KILL Positions: TS={ts}')
            time.sleep(2.0)

            TIMEOUT= 10
            while TIMEOUT > 0:
                fills =  self.check_orders()
                for fill in fills:
                    self.process_fill(fill)
                time.sleep(2.0)
                TIMEOUT -= 1
            logger.info('KILL: open positions closed.')

            position_msg = '\nPOSITIONS:\n'
            for pos_node in self.pos_mgr.get_positions():
                position_msg += f'{pos_node.name}= {pos_node.position}\n'
            logger.info(position_msg)

            self.close_trading_book()

            logger.critical('strategy exit due to --kill flag activated.')
            exit(0)

    ## eod of day accounting
    def close_trading_book(self):
        ## mark each position with its current duration
        logger.info('grabbing EOD prices to CLOSE TRADING BOOK.')
        eod_prices = self.get_prices( self.universe )

        ptable = json.dumps(self.eod_prices, indent=4)
        prices_msg = f'EOD PRICES:\n{ptable}'
        logger.info(prices_msg)

        logger.info('marking open positions.')
        self.pos_mgr.mark_to_market_open_positions( eod_prices )
        logger.info('updating position durations.')
        self.pos_mgr.update_durations()

        ## remove order groups created
        logger.info('removing resolved fa_groups\n')
        self.pos_mgr.purge_order_groups()

        self._save_context()


    def ping_connection(self):
        logger.info('pinging IB server.')
        ping = IB.tickle()
        logger.info(json.dumps(ping, ensure_ascii=False, indent=4 ))
        return ping


    def get_contract_id(self, symbol):
        contract_id = None
        try:
            contract_id = self.security_master.get_sec_def(symbol)['contract_id']
        except:
            contract_id = IB.symbol_to_contract_id(symbol)
            if contract_id:
                self.security_master.add(symbol, contract_id)

        return contract_id


    def connect_to_market(self, symbol):
        contract_id = self.get_contract_id(symbol)
        logger.info(f'initialize market data connection for symbol= {symbol}, contract_id= {contract_id}')
        market_init = IB.market_connect(contract_id, retry=5)
        if not market_init:
            logger.critical(f'market data initialization failed. contract_id= {contract_id}')

    
    def get_account_info(self):
        if self.account_info is None:
            logger.info('fetching account information from IB')
            self.account_info = IB.account_summary()
            log_time= datetime.today().strftime('%Y%m%d')
            account_file =f"{PORTFOLIO_DIRECTORY}/{self.strategy_id}/account/{self.strategy_id}.account.{log_time}.json"
            with open(account_file, 'w') as f:
                acc_info = json.dumps(self.account_info, ensure_ascii=False, indent=4)
                f.write(acc_info)

        return self.account_info

    def logout(self):
        logged_out = IB.logout()
        logger.info(f'logged out = {logged_out}')

    def close_connection(self):
        IB.connection_cleanup()
        logger.info(f'Authentication token removed. Connection Closed.')

    def get_market_snapshot(self, contract_id):
        return IB.market_snapshot(self.contract_id)

    def get_bid_ask(self, symbol, raise_error=False):
        bid = ask = bid_size = ask_size = None
        try:
            con_id = self.get_contract_id( symbol)
            market_data = IB.market_snapshot(con_id)
            bid = market_data.get('bid')
            ask = market_data.get('ask')
            bid_size = market_data.get('bid_sz')
            ask_size = market_data.get('ask_sz')
            #symbol = market_data.get('symbol')

            logger.info(f'current bid/ask for symbol = {symbol}({con_id}): bid:{bid}|({bid_size}), ask:{ask}|({ask_size})')
            if not all([bid, ask, bid_size, ask_size]):
                logger.error(f'incomplete quote: {symbol}({con_id}): bid:{bid}|({bid_size}), ask:{ask}|({ask_size})')
        except Exception as bid_ask_exception:
            logger.critical(f'price data query failed for {symbol}, contract_id = {con_id}!\n' + str(bid_ask_exception))
            if raise_error:
                raise bid_ask_exception

        return bid, ask, bid_size, ask_size

    def get_prices(self, symbol_list):
        prices = {}
        for symbol in symbol_list:
            for _ in range(10):
                bid, ask, _, _ = self.get_bid_ask( symbol, raise_error=True )
                if all([bid, ask]): break
                time.sleep(0.5)
            else:
                raise ValueError('failed to fetch bid/ask for {symbol}')
            prices[symbol] = (bid, ask)

        return prices

    def get_order_ledger(self):
        return self.pos_mgr.get_order_ledger()

    def get_open_orders(self):
        return self.pos_mgr.get_open_orders()

    def get_position(self, symbol):
        return self.pos_mgr.get_position(symbol)

    def get_positions(self):
        return self.pos_mgr.positions

    def position_count(self):
        return self.pos_mgr.position_count()

    ## helper function to print out
    ## target calc functions
    def _func2str(self, func):
        if isinstance(func, functools.partial):
            return f'{func.func.__name__} {json.dumps(func.keywords)}'
        return func.__name__


    ## take a dictionary of the universe to be traded,
    ## along with current bid/ask spread per name,
    ## and determine target quantities for sending orders
    ## prices_dict = { symbol: tuple(bid, ask) }
    ## positions_dict = { symbol: account position }
    ## cash_alloc = total cash available to trade the strategy for a specific account

    ## determine trading targets
    ## requires a TARGET_CALC_FUNC that determines the individual targets
    ## per account, per symbol
    ## TARGET_CALC_FUNC has a signature of:
    ##     (the allocNode for the account, dict(prices) - with these 2 params 
    ##     you grab any context information needed to derive new trades
    ## returns a list of dictionary holding the aggregate target info for each symbol
    def get_targets(self, TARGET_CALC_FUNC):

        targets = []
        allocations = self.pos_mgr.get_allocations()
        per_account = defaultdict(list)
        target_totals = {}
        order_defs = {}
        msg = ''

        prices = self.get_prices( self.universe ) 
        ptable = json.dumps(prices, indent=4)

        try:
            ## do trading targets for each account
            logger.info('calculating trading targets')
            logger.info(f'using TARGET_CALC_FUNC: {self._func2str(TARGET_CALC_FUNC)}')
            logger.info(f'PRICES:\n{ptable}')
            for alloc in allocations:
                account_id = alloc.account_id
                msg += f'AllocNode:\n{json.dumps(alloc.to_dict(), indent=4)}\n'
                kk = f'AccountId( {account_id} ) Targets:'
                msg += f'{_indent(kk, 4)}\n'
      
                trade_targets = TARGET_CALC_FUNC( alloc, prices )

                ## iterate thru all calculated targets for this account
                ## target example 
                ## tgt = {'symbol': symbol,
                ##        'target_amt': target_amt,
                ##        'order_type': OrderType,MKT,
                ##        'stop_price': None,
                ##        'limit_price': None
                ## }

                for target in sorted( trade_targets, key=lambda x:x['symbol'] ):
                    tgt_dump = json.dumps(target, indent =4) 
                    logger.debug(f'account target:\naccount= {account_id}\n{tgt_dump}')
                    symbol = target.get('symbol')
                    pos = alloc.get_position(symbol)
                    target_qty = target.get('target_amt')
                    acc_dict = dict(account_id=account_id, target=target_qty, current=pos) 
                    per_account[symbol].append(acc_dict) 

                    ## assign target to account allocation
                    alloc.targets[symbol] = target_qty
                    target_totals[symbol] = target_qty + target_totals.get(symbol,0)

                    ## order type will be FOREVER consistent for a specific name across all accounts
                    ## just copying the tgt to order defs - we ignore symbol and target_amt items
                    ## and just use the order_type, stop_price, and limit_price we need
                    ## tgt = { 'symbol': symbol,
                    ##    'target_amt': target_amt,
                    ##    'order_type': OrderType.MKT,
                    ##    'stop_price': None,
                    ##    'limit_price': None
                    ## }

                    msg += f'{_indent(json.dumps(target,indent=4),4)}\n'
                    order_defs[symbol] = target


            tt = json.dumps(target_totals, indent=4)
            logger.critical(f'\nTRADING TARGET TOTALS:\n{tt}\n\n' + msg)
        except Exception as exc:
            logger.critical('\n'.join(['failed get_targets calc:', msg, str(exc)]))
            raise exc

        ## generate map of symbol: tuple(contract_id, total_qty, order_type) 
        ## that will be used to generate orders
        targets = dict()
        for symbol, tot_target in target_totals.items():
            ## show aggregates per symbol
            targets[symbol] = dict(contract_id=self.security_master[symbol], target=tot_target, order_def=order_defs[symbol]) 
            ## include detail that makes up the aggregate
            targets[symbol].update( dict(per_account=per_account[symbol]) ) 

        ## update positions file to include targets
        now = datetime.now()
        self.pos_mgr.write_positions(now)

        return targets


    def create_order(self, symbol, contract_id, side, amount, order_def, order_notes='', fa_group=None):

        logger.info('sending order.')

        order_type = order_def['order_type'].value
        stp_price, lmt_price = order_def['stop_price'], order_def['limit_price']
        order_info = IB.order_request(contract_id, order_type, side.value, amount,
                                        stp_price=stp_price, lmt_price=lmt_price, fa_group=fa_group)

        if order_info.get('reply_id') is not None:
            ## confirm to server that you want to send this order
            ## repeat flag forces all subsequent rder_replies to be resolved before returning
            order_info = IB.order_reply(order_info['reply_id'], repeat=True)

        order_id = order_info['order_id']
        logger.info(f'order_id: {order_id} successfully sent.')

        order_info = {
            'order_id': order_id,
            'symbol': symbol,
            'quantity': amount,
            'side': side.value,
            'order_type': order_type,
            'stop_price': stp_price,
            'limit_price': lmt_price,
            'fa_group': fa_group,
            'order_notes': order_notes 
        }

        order_info_dump = 'order_info:\n' + json.dumps(order_info, ensure_ascii=False, indent =4 )
        logger.info(order_info_dump)

        return order_info

    def create_fa_group(self, side, total, symbol, contract_id, account_allocs):
        account_list = []
        for account_dict in account_allocs:
            order_amt = abs(account_dict['target'] - account_dict['current'])
            account_list.append( dict(amount=order_amt, name=account_dict['account_id']) )

        now = datetime.now().strftime('%H%M%S')
        group_name = f'{symbol}_{side}_{int(total)}_{contract_id}_{now}'
        fa_group = {
                "name": group_name,
                "accounts": account_list,
                "default_method": "S"
        }

        IB.create_allocation_group(fa_group)
        logger.info('created fa_group:\n' + json.dumps(fa_group, indent=4))

        return group_name

    def generate_order(self, symbol, target_dict, order_notes=''):
        contract_id = target_dict['contract_id']
        target_amt = target_dict['target']
        order_def = target_dict['order_def']
        per_account = target_dict['per_account']

        pos_node = self.get_position(symbol)
        total_order_amt = target_amt - pos_node.position 

        side = TradeSide.BUY if total_order_amt > 0 else TradeSide.SELL

        ## IMPORTANT all amount passed for order submission are POSITIVE values
        total_order_amt = abs(total_order_amt)

        if total_order_amt == 0:
            msg = f'\nattempting to submit ZERO amount order for {symbol}'
            msg += f'\n{pos_node}, target_amt= {target_amt}'
            logger.critical(msg)
            return None 

        ## just a check to make sure sum(detail) == total_amount to trade
        item_total = sum([abs(x['target']-x['current']) for x in per_account])
        if item_total != total_order_amt:
            items_str = json.dumps(per_account, indent=4)
            err = f'{symbol} order mismatch:\ntotal={total_order_amt}, itemized_total={item_total},'
            err += f'\n{items_str}' 
            raise RuntimeError(err) 

        ibrk_fa_group = None
        if per_account:
            ibrk_fa_group = self.create_fa_group(side, total_order_amt, symbol, contract_id, per_account)
            #self.order_groups.append(ibrk_fa_group)

        return self.create_order(symbol, contract_id, side, total_order_amt, order_def, order_notes, ibrk_fa_group)
    
    def send_orders(self, target_map, order_notes=''):

        logger.info(
            f'send_orders: submitting {len(target_map)} order(s) '
            f'order_notes={order_notes}'
        )

        for symbol, target_dict in target_map.items():
            order_info = self.generate_order(symbol, target_dict, order_notes)
            if order_info:
                self.pos_mgr.register_order(order_info)


    def check_orders(self):
        return self.pos_mgr.check_orders()


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
            trd.order_notes = fill["order_notes"]

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


