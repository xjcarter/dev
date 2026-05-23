import ib_endpoints2 as IB
import pandas
import argparse
import json
import os
from datetime import datetime
import pytz
from sec_master import SecMaster

DATA_DIR = os.getenv('DATA_DIR','/trading/data/')

"""
ibrk_daily_data.py:
    Fetches historical data from IBRK given a list of symbols.
    Usage: python3 ibrk_daily_data.py --file=<symbol_file>
        All the symbols in the symbol file must be mapped to their corresponding conid 
        in /trading/data/security_master.json

    Example:
    ** symbol fetch file
       AAPL
       SPY
       
    ** /trading/data/security_master.json ** :
    {
    "AAPL": { 
                "conid": 265598,
                "name": "APPLE COMPUTER",
                "sectype": "STOCK"
            },
    "SPY":  {
                "conid": 756733,
                "name": "ISHARES SP500 ETF",
                "sectype": "ETF"
            }
    }

"""

def parse_arguments():
    parser = argparse.ArgumentParser(description="Historical Data download OHLC daily from IBRK")
    parser.add_argument("--file", required=True, help="IBRK data fetch list- by symbol")
    return parser.parse_args()

def read_fetch_list(filename):
    fetch_list = []
    try:
        with open(filename, 'r') as file:
            for line in file.readlines():
                v = line.strip()
                if len(v) > 0: fetch_list.append(v)
    except:
        print(f'Error: failure reading fetch_list: {filename}')

    return fetch_list


def convert_market_data(market_data):
    
    if len(market_data['data']) == 0:
        return

    df = pandas.DataFrame(market_data['data'])
    
    def _convert_timestamp(timestamp_ms):
        timestamp_s = timestamp_ms / 1000
        utc_datetime = datetime.fromtimestamp(timestamp_s, tz=pytz.utc)
        v = utc_datetime.astimezone(pytz.timezone('US/Eastern'))
        return f'{v:%Y-%m-%d}'

    df['t'] = df['t'].apply(_convert_timestamp)

    ## copy over adjusted close 
    df['ac'] = df['c']

    ## rearrange and re-label
    df = df['t,o,h,l,c,ac,v'.split(',')]
    df.columns='Date,Open,High,Low,Close,Adj Close,Volume'.split(',')

    return df


def get_current_data(symbol):
    filename = f'{DATA_DIR}/{symbol}.csv'
    try:
        df = pandas.read_csv(filename)
        return df
    except:
        print(f'No current history for {filename}')
    return None

def calc_fetch_period(current_df):
    
    ## fetch days only needed to update history
    ## NOTE some overlap will happen on weekends/holidays
    ## but that is taken care of with the 'drop_duplicates' when combining dfs
    if current_df is not None and not current_df.empty:
        last = current_df.iloc[-1]['Date']
        today = datetime.today()
        last_date = datetime.strptime(last,"%Y-%m-%d")
        delta = today - last_date
        return last, delta.days - 1
    
    ## get last 30 days of history if new fetch
    return None, 30


def fetch_data(fetch_list):

    security_master = SecMaster(f'{DATA_DIR}/security_master.json')
    for symbol in fetch_list:
        market_df = None

        print(f'reclaiming current data: {symbol}')
        current_df = get_current_data(symbol)

        print(f'fetching: {symbol}')
        try:
            conid = security_master.get_sec_def(symbol)['contract_id']
            last_date, fetch_period = calc_fetch_period(current_df)
            if fetch_period > 0:
                market_data = IB.market_data_history(int(conid), exchange='', period=f'{fetch_period}d', bar='1d', start_time='', outside_rth=False)
                #print(json.dumps(market_data, indent=4))
                market_df = convert_market_data(market_data)
            else:
                print(f'No fetch triggered: last date for {symbol} = {last_date}')
        except:
            print(f'Error: failed to fetch data for symbol= {symbol}')

        if market_df is not None and not market_df.empty:
            try:

                #current_df.set_index('Date', inplace=True)
                #market_df.set_index('Date', inplace=True)

                ## handle any overlap
                new_df = pandas.concat([current_df, market_df], axis=0).drop_duplicates(subset=['Date'], keep='last')
                outfile = f'{DATA_DIR}/{symbol}.csv'
                new_df.to_csv(outfile, index=False)
                print(f'Archived: {outfile}')
            except:
                print(f'Error: unable to write {outfile}')

if __name__ == '__main__':
    args = parse_arguments()
    fetch_list= read_fetch_list(args.file)
    fetch_data(fetch_list)
