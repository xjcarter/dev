
import sys
import os
from datetime import datetime
import pandas
from prettytable import PrettyTable
from indicators import Beta, Corr 
import argparse

DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/trading/data/')

##
## beta_map.py:  list the correlation and beta calcs for a list of symbols against a benchmark symbol 
## Usage: python beta_map.py <benchmark symbol> --file <symbol_file> --list <comma separated list> --sample_size <sample size>
##     Optional args: --corr_above <correlation value> --beta_above <beta value>
##                    --corr_below <correlation value> --beta_below <beta value>
##

def include_from_filters(corr_val, beta_val, corr_limits, beta_limits):

    ## set up correlation and beta filters
    corr_below, corr_above = corr_limits
    beta_below, beta_above = beta_limits

    if corr_below is not None and corr_val > corr_below: return False
    if corr_above is not None and corr_val < corr_above: return False
    if beta_below is not None and beta_val > beta_below: return False
    if beta_above is not None and beta_val < beta_above: return False 

    return True


def map_data(corr_symbol, symbol_list, sample_sz=50, corr_limits=(None,None), beta_limits=(None,None)):

    table_cols = "n Symbol Benchmark Close Benchmark_Close Corr Beta".split()
    daily_table = PrettyTable(table_cols)

    for col in table_cols:
        daily_table.align[col] = "l"
        if col in ["Close", "Benchmark_Close", "Corr", "Beta"]:
            daily_table.float_format[col] = ".2"
            daily_table.align[col] = "r"
    try:
        corr_file = f'{DATA_DIR}/{corr_symbol}.csv'
        corr_df = pandas.read_csv(corr_file)
    except:
        raise RuntimeError(f'Cannot find benchmark {corr_symbol} file: {corr_file}')

    ## need one additonal day of price history because we need to calc daily returns
    daysback = sample_sz + 1 
    corr_df.set_index('Date', inplace=True)

    vv = len(corr_df)
    if vv < daysback:
        raise RuntimeError(f'Not enoungh data points for benchmark {symbol}: len={vv}, daysback={daysback}')


    errors = []
    csv_data = []
    for symbol in symbol_list:
        symbol = symbol.replace('"','')
        if len(symbol) == 0: continue
        try:
            stock_file = f'{DATA_DIR}/{symbol}.csv'
            stock_df = pandas.read_csv(stock_file)
            stock_df.set_index('Date', inplace=True)

            corr = Corr(sample_size=sample_sz)
            beta = Beta(sample_size=sample_sz)

            ss = len(stock_df)
            if ss < daysback:
                errors.append(f'Not enoungh data points for {symbol}: len={ss}, daysback={daysback}')
                continue

            count = corr_df.shape[0]

            close_price = corr_price = None
            for i in range(count-daysback, count):
                idate = corr_df.index[i]
                corr_bar = corr_df.loc[idate]
                try:
                    stock_bar = stock_df.loc[idate]
                except:
                    errors.append(f'No Data for {symbol} on Date: {idate}')
                    continue

                close_price = stock_bar['Close'] 
                corr_price = corr_bar['Close'] 
                pair = (close_price, corr_price)

                corr.push(pair)
                beta.push(pair)

            cc = corr.valueAt(0)
            bb = beta.valueAt(0)

            if include_from_filters(cc, bb, corr_limits, beta_limits):
                values = [sample_sz, symbol, corr_symbol, close_price, corr_price, cc, bb]
                daily_table.add_row(values)
                csv_data.append(values)

        except Exception as e: 
           errors.append(f'Error: {stock_file}, {e}')  

    print(" ")
    print(daily_table)
    print(" ")
    for y in errors:
        print(y)

    try:
        df = pandas.DataFrame(data=csv_data, columns=table_cols)
        df.to_csv('betas.csv')
    except:
        raise RuntimeError('Could not create betas.csv file')
        

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

##     Optional args: --corr_above <correlation value> --beta_above <beta value>
##                    --corr_below <correlation value> --beta_below <beta value>

if __name__ == '__main__':
    parser =  argparse.ArgumentParser()
    parser.add_argument("benchmark", help="benchmark symbol for correlations and beta calcs")
    parser.add_argument("--list", help="command line comma separated list of symbols", type=str, default="")
    parser.add_argument("--file", help="single entry per line symbol file", type=str, default="")
    parser.add_argument("--sample_size", help="sample size of correlation and beta calcs", type=int, default=50)
    parser.add_argument("--corr_above", help="upside corr limit", type=float, default=None)
    parser.add_argument("--corr_below", help="downside corr limit", type=float, default=None)
    parser.add_argument("--beta_above", help="upside beta limit", type=float, default=None)
    parser.add_argument("--beta_below", help="downside beta limit", type=float, default=None)

    u = parser.parse_args()

    symbol_list = parse_symbols(u.list, u.file) 
    corr_limits = (u.corr_below, u.corr_above)
    beta_limits = (u.beta_below, u.beta_above)

    map_data(u.benchmark, symbol_list, u.sample_size, corr_limits, beta_limits)
