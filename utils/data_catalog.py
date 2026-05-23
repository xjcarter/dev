
import sys
import os
from datetime import datetime
import pandas
from prettytable import PrettyTable
from indicators import StDev, Median
import argparse

DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/trading/data/')

##
## data_table.py:  list the stats of the daily stock csv files 
## stored in the /data directory
##

def list_data(symbol_list):

    table_cols = "Count StartDt EndDt Symbol Close mdv20 StDev Pct".split()
    daily_table = PrettyTable(table_cols)

    for col in table_cols:
        daily_table.align[col] = "l"
        if col in ["Close", "StDev", "Pct"]:
            daily_table.float_format[col] = ".2"
            daily_table.align[col] = "r"

    COLON = ':'
    errors = []
    for key in symbol_list:
        key = key.replace('"','')
        key = key.upper()

        yahoo_key = symbol = key

        ## handle filename alias for goofy yahoo_down_load keys
        if COLON in key:
            yahoo_key, symbol = key.split(COLON)

        if len(symbol) == 0: continue
        try:
            stock_file = f'{DATA_DIR}/{symbol}.csv'
            stock_df = pandas.read_csv(stock_file)
            stock_df.set_index('Date', inplace=True)

            stdev = StDev(sample_size=50)
            mdv = Median(sample_size=20)

            count = stock_df.shape[0]
            start_dt = datetime.strptime(stock_df.index[0],"%Y-%m-%d").date()
            end_dt = datetime.strptime(stock_df.index[-1],"%Y-%m-%d").date()
            close_price = stock_df.loc[stock_df.index[-1]]['Close'] 

            for i in range(count-60, count):
                idate = stock_df.index[i]
                stock_bar = stock_df.loc[idate]
                close_price = stock_bar['Close']
                stdev.push(close_price)
                mdv.push(stock_bar['Volume'])

            q = stdev.valueAt(0)
            mm = int(mdv.valueAt(0))
            values = [count, start_dt, end_dt, symbol, close_price, mm, q, q/close_price]
            daily_table.add_row(values)
        except: 
           errors.append(f'Cant find: {stock_file}')  

    print(" ")
    print(daily_table)
    print(" ")
    for y in errors:
        print(y)

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
    parser =  argparse.ArgumentParser()
    parser.add_argument("--list", help="command line comma separated list of symbols", type=str, default="")
    parser.add_argument("--file", help="single entry per line symbol file", type=str, default="")
    u = parser.parse_args()

    symbol_list = parse_symbols(u.list, u.file) 
    list_data(symbol_list)
