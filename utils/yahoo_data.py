import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import argparse
import os
import json
import sys

## Grabs Daily Data from Yahoo Finance 
## Usage: python yahoo_data.py YAHOO_DOWNLOAD_KEY:<SYMBOL_ALIAS>
##   = the yahoo_download key is the tag used to fetch the data from yahoo
##   - the alias is optional and is to be used for naming the data file when the yahoo_key has a goofy format
##   - i.e. the yahoo_download_key contains symbols don't conform to typical filename conventions
##   - if no alias is given the yahoo_download_key is used as the name for the data file
## Usage: python yahoo_data.py ^DJI:DOW_INDEX ^RUT IBM GOOG

DATA_DIR = os.environ.get('DATA_DIR', '/home/jcarter/work/trading/data/')
START_DATE = '1999-12-31'

def convert_to_old_format(data_df):
    """
    Added - 02-28-2025

    the new format of the yahoo data is a MultIndex referenced DataFrame:
    >>> data = yf.download(['UPRO'], start="2024-01-01", end="2024-12-31")
    >>> data.head()
        Price           Close       High        Low       Open   Volume
        Ticker           UPRO       UPRO       UPRO       UPRO     UPRO
        Date                                                           
        2024-01-02  53.211758  53.587953  52.508868  53.092960  6554500
        2024-01-03  51.924778  52.736566  51.726780  52.479168  7324500
        2024-01-04  51.380287  52.588068  51.291188  51.706981  5516600
        2024-01-05  51.578278  52.380168  51.073386  51.400080  7851800
        2024-01-08  53.726551  53.795850  51.677280  51.706978  6631100

    - so this function 'flattens' single symbol fetches back to the legacy format
    """

    if 'Adj Close' in data_df.columns:
        ## this is an old fetched dataframe
        return data_df

    data_df.columns = [f'{price}' for price, ticker in data_df.columns]
    
    ## Adj Close is now automatically imbeded in the query - just copy it over
    ## as a place holder
    ## NOTE - I anticipate and issue with the raw-to-adjust conversion in backtesting

    data_df['Adj Close'] = data_df['Close']
    ## re-order columns
    data_df = data_df[['Open','High','Low','Close','Adj Close','Volume']]

    ## return dataframe indexed by 'Date'
    return data_df


def get_daily_data(symbols):
    databank = dict()
    COLON = ':'

    for key in symbols:
        key = key.upper()
        yahoo_key = sym = key

        ## handle filename alias for goofy yahoo_download keys
        if COLON in key:
            yahoo_key, sym = key.split(COLON)

        filename = f'{DATA_DIR}/{sym}.csv'

        current_data = None
        last_date = START_DATE
        path = Path(filename)

        if path.is_file():
            # get last record
            current_data = pd.read_csv(filename)
            last_row = current_data.shape[0] - 1
            last_date = current_data.iloc[last_row]['Date']
            # re-index Date to a DateTimeIndex (which is the original form of the Yahoo download)
            current_data['Date'] = pd.to_datetime(current_data['Date'], format='%Y-%m-%d')
            current_data.set_index('Date', inplace=True)

        databank[sym] = current_data

        today = datetime.today()
        last_date = datetime.strptime(last_date, "%Y-%m-%d")

        days_to_fetch = today - last_date
        if days_to_fetch.days > 0:
            start = last_date + timedelta(days=1)
            try:
                start, today = start.date(), today.date()
                print(f'Fetching: {sym}, {start}, {today}')

                # Fetch data with error handling
                new_data = yf.download(yahoo_key, start=start, end=today, progress=False)

                ## convert data from update yfinance module to legacy format
                new_data = convert_to_old_format(new_data)

                if new_data.empty:
                    print(f'No data fetched for: {sym}, {start}, {today}')
                    continue

                # Round the data to 5 decimal places
                for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                    if col in new_data.columns:
                        new_data[col] = new_data[col].round(decimals=5)

                # Merge new data with existing data
                if current_data is not None:
                    for ndate in new_data.index:
                        if ndate in current_data.index:
                            # Remove duplicate row in currently saved data -> it will be updated with the new data
                            current_data.drop(ndate, inplace=True)
                    new_data = pd.concat([current_data, new_data], axis=0)

                # Save the updated data to CSV
                new_data.to_csv(filename)
                databank[sym] = new_data
            except Exception as e:
                print(f'Error fetching or processing data for {sym}: {e}')
                continue

    return databank


def parse_symbols(sym_string, sym_file):
    symbols = []
    if sym_string is not None and len(sym_string) > 0:
        symbols = sym_string.split()

    file_symbols = []
    if sym_file is not None and len(sym_file) > 0:
        with open(sym_file, 'r') as f:
            file_symbols = f.readlines()

    ## clean whitespace
    v = [x.strip() for x in symbols + file_symbols]
    return v


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", help="command line comma separated list of symbols", type=str, default="")
    parser.add_argument("--file", help="single entry per line symbol file", type=str, default="")
    u = parser.parse_args()

    symbol_list = parse_symbols(u.list, u.file)
    get_daily_data(symbol_list)
