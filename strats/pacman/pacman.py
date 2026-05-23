from datetime import datetime
from collections import defaultdict
import os, sys, json
import logging
import time
import argparse
import time, pandas
from datetime import datetime
import calendar_calcs
from clockutils import create_tripwire
import ib_endpoints2 as IB

STRATEGY="pacman"
PORTFOLIO_DIRECTORY = os.getenv('PORTFOLIO_DIRECTORY', '/portfolio/')

def get_time():
    return datetime.today().strftime('%Y%m%d')

## IMPORTANT - always label correct STRATEGY tag
DATA_DIR = os.getenv('DATA_DIR', '/trading/data/')

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

def _read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            file_contents = file.read()
            json_data = json.loads(file_contents)
        return json_data
    except json.JSONDecodeError as e:
        print(f"JSON decoding error for {filename}: {e}")
        print(f"Problematic JSON file contents: {file_contents}")


## Intraday Price Archiver 
class PriceGrabber():
    def __init__(self, configuration_file):

        self.cfg = _read_json_file(configuration_file)

        """
        dict of symbol, conid mappings
        {
            "APPL": 123456,
            "SPY": 9999
        }
        """
        def clean_symbol_map():
            deletes = []
            if self.symbol_map is not None:
                for symbol, conid in self.symbol_map.items():
                    if conid is None or (isinstance(conid,int) == False) or conid <= 0:
                        deletes.append(symbol)
            for d in deletes:
                del self.symbol_map[d]

        self.symbol_map = self.cfg.get('symbols')
        clean_symbol_map()

        self.intra_prices = defaultdict(list)
        self.datestamp = datetime.today().strftime("%Y%m%d")
        
        ## Load existing data on initialization in case of a restart
        self.load_existing_data()

    def load_existing_data(self):
        logger.info("Checking for existing intraday data files to reload...")
        for symbol in self.symbol_map.keys():
            filepath = f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/{symbol.upper()}.{self.datestamp}.csv'
            if os.path.exists(filepath):
                try:
                    df = pandas.read_csv(filepath)
                    # Convert dataframe back to a list of dicts to seamlessly append new data
                    self.intra_prices[symbol] = df.to_dict('records')
                    logger.info(f"Successfully loaded {len(self.intra_prices[symbol])} existing records for {symbol}.")
                except Exception as e:
                    logger.error(f"Failed to load existing data for {symbol} from {filepath}: {e}")
            else:
                logger.info(f"No existing data file found for {symbol} today. Starting fresh.")

    def connect_to_market(self, symbol, contract_id):
        logger.info(f'initialize market data connection for symbol= {symbol}, contract_id= {contract_id}')
        market_init = IB.market_connect(contract_id, retry=5)
        if not market_init:
            logger.critical(f'market data initialization failed. contract_id= {contract_id}')
 
    def get_market_snapshot(self, contract_id):
        return IB.market_snapshot(contract_id)

    def fetch_prices(self):
        errors = []
        for symbol, contract_id in self.symbol_map.items():
            try:
                market_data = self.get_market_snapshot(contract_id)
                ## ensure that a price quote was posted.
                ## sometime the first call only establishes a connection
                if market_data.get('last'):
                    self.intra_prices[symbol].append(market_data)
            except:
                errors.append(symbol)

        if len(errors) > 0:
            logger.critical(f'failed fetch: {errors}')


    def dump_intraday_prices(self):
        errors = []
        for symbol, data in self.intra_prices.items(): 
            try:
                df = pandas.DataFrame(data)
                df = df[['date','time','_updated','last','bid','ask','bid_sz','ask_sz','volume','symbol','conid']]
                filepath = f'{PORTFOLIO_DIRECTORY}/{STRATEGY}/data/{symbol.upper()}.{self.datestamp}.csv'
                df.to_csv(filepath, index=False)
            except:
                errors.append(symbol)

        logger.critical('save loop completed.')
        if len(errors) > 0:
            logger.error(f"couldn't write intraday data for: {errors}")

    ## report number of intraday data entries recorded per symbol
    def report(self):
        for symbol, _ in self.symbol_map.items():
            data = self.intra_prices.get(symbol)
            if data is None:
                logger.critical(f'{symbol} Count: NONE')
            else:
                logger.info(f'{symbol} Count: {len(data)}')
        

    def run(self):

        logger.info('starting intraday price archiver.')

        for symbol, conid in self.symbol_map.items():
            logger.info(f'connecting: symbol= {symbol}, contract_id= {conid}')
            self.connect_to_market(symbol, conid)

        ## trading operations schedule
        start_fetch = create_tripwire(self.cfg.get('start_fetch'))
        monitor_fetch = create_tripwire(self.cfg.get('monitor_fetch'))
        dump_data = create_tripwire(self.cfg.get('dump_data'))

        ## reporting TripWires
        yy = [start_fetch, dump_data]
        logger.info(f'\nTripWire setup:\n{yy}')

        logger.info('starting pacman loop.')

        while True:

            ## capturing 1 min price snapshots - all data 
            with start_fetch as fetch_open:
                if fetch_open:
                    self.fetch_prices()
            with monitor_fetch as monitor:
                if monitor:
                    self.report()
                    self.dump_intraday_prices()
            with dump_data as data_dump:
                if data_dump:
                    logger.critical('final dump.')
                    self.dump_intraday_prices()
                    logger.critical('terminating pacman loop.')
                    break

            time.sleep(6)

        logger.info('pacman finished.')


if __name__ == "__main__":
    parser =  argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    u = parser.parse_args()

    holidays = calendar_calcs.load_holidays()
    today = datetime.today().date()
    if today not in holidays:
        pacman = PriceGrabber(u.config)
        pacman.run()
    else:
        today_str = today.strftime("%Y-%m-%d")
        logger.critical(f'Today:{today_str} is a holiday. PACMan disabled.')
