
import sys 
from datetime import datetime
import pandas
from prettytable import PrettyTable
from indicators import StDev, Median
import argparse

DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/trading/data/')

##
## view_symbol.py:  dumps a simple table of history for a specific symbol
## Usage: python view_symbol.py <SYMBOL> --daysback=<days back in history>
##

def list_history(symbol, daysback):

    table_cols = "Date Symbol Open High Low Close Adj_Close Volume mdv20 StDev Pct".split()
    daily_table = PrettyTable(table_cols)

    for col in table_cols:
        daily_table.align[col] = "l"
        dec_prices = "Open High Low Close Adj_Close StDev Pct".split()
        if col in dec_prices:
            daily_table.float_format[col] = ".2"
            daily_table.align[col] = "r"
            

    STDV_LEN, MDV_LEN = 50, 20

    try:
        stock_file = f'{DATA_DIR}/{symbol}.csv'
        stock_df = pandas.read_csv(stock_file)
        stock_df.rename(columns={'Adj Close':'Adj_Close'}, inplace=True)
        stock_df.set_index('Date', inplace=True)

        stdev = StDev(sample_size=STDV_LEN)
        mdv = Median(sample_size=MDV_LEN)

        count = len(stock_df)
        if daysback > 0 and count >= daysback: 
            first_index = count - daysback
            fdate = stock_df.index[first_index]
            first_dt = datetime.strptime(fdate,"%Y-%m-%d").date()

            ## grab the mininum amount data to do calcs
            full_window= (daysback + STDV_LEN + MDV_LEN)
            a, b = (0, count)
            if full_window < count: a, b = (count-full_window, count)
            for i in range(a, b):
                idate = stock_df.index[i]
                cur_dt = datetime.strptime(idate,"%Y-%m-%d").date()
                stock_bar = stock_df.loc[idate]
                close_price = stock_bar['Close']
                ss = stdev.push(close_price)
                mm = mdv.push(stock_bar['Volume'])

                if cur_dt >= first_dt:
                    stats = [int(mm), ss, ss/close_price]
                    price_bar = []
                    for v in ['Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']:
                        a =stock_bar[v]
                        if v == 'Volume': a = int(a)
                        price_bar.append(a) 
                    daily_table.add_row([idate, symbol] + price_bar + stats)
    
            print(daily_table)
                      
        else:
            raise Exception(f'{symbol}: data_count= {count}, cannot look {daysback} day back.')
    except Exception as e: 
        print(f'Error with {symbol}: {e}')  


if __name__ == '__main__':
    parser =  argparse.ArgumentParser()
    parser.add_argument("symbol", help="stock to trade based on the index")
    parser.add_argument("--daysback", help="history count", type=int, default=10)
    u = parser.parse_args()

    list_history(u.symbol, u.daysback)
