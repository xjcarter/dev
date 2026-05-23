
import argparse
import calendar_calcs
from datetime import datetime
import pandas
import os
from prettytable import PrettyTable  

WEEKDAYS = ['MON','TUE','WED','THU','FRI','SAT','SUN']

### THIS IS A CALENDAR TOOL TO IDENTIFY ACTION DAYS FOR THE MP STRATEGY
### mp (monday pullback) strategy
### simple long-only strategy trading a leveraged market etf (UPRO, UDOW) 
### the rules:
### buy on next open if close[1] is lower than low price established on the first day of the week
### sell on first day the trade is profitable or after days duration,
### sell if stopped out on close (using standard stop)

DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/trading/data/')

def adjust_prices(df):
    ## adjust the entire price bar to adjusted prices
    ## using the ratio of of Adj_Close/Close as multiplier
    data = []
    for i in range(df.shape[0]):
        bar = df.iloc[i]
        r = bar['Adj Close']/bar['Close']
        ah = bar['High'] * r
        al = bar['Low'] * r
        ao = bar['Open'] * r
        new_bar = [ao, ah, al, bar['Adj Close']]
        data.append([bar['Date']] + [round(x,6) for x in new_bar] + [bar['Volume']])

    nf = pandas.DataFrame(columns='Date Open High Low Close Volume'.split(), data=data)
    return nf, df


def show_data(stock, days_back, use_raw):
    cal_columns = f'Symbol Date Day Open High Low Close Volume'
    cal_table = PrettyTable(cal_columns.split())
    for col in 'Open High Low Close'.split():
        cal_table.float_format[col] = ".2"
        cal_table.align[col] = "r"
    cal_table.float_format['Volume'] = ".0"
    cal_table.align['Volume'] = "r"

    stock_file = f'{DATA_DIR}/{stock}.csv'
    print(f'Stock: {stock} -> {stock_file}')
    new_df, orig_df = adjust_prices(pandas.read_csv(stock_file))
    stock_df = new_df
    if use_raw: stock_df = orig_df
    stock_df.set_index('Date',inplace=True)

    gg = stock_df[-days_back:]
    for i in range(gg.shape[0]):
        idate = gg.index[i]
        cur_dt = datetime.strptime(idate, "%Y-%m-%d").date()
        stock_bar = gg.loc[idate]
        row = [stock, idate, WEEKDAYS[cur_dt.weekday()]]
        row += [stock_bar['Open'], stock_bar['High'], stock_bar['Low'], stock_bar['Close'], stock_bar['Volume']]
        cal_table.add_row(row) 
    
    print(" ")
    print(cal_table)
    print(" ")


if __name__ == '__main__':

    parser =  argparse.ArgumentParser()
    parser.add_argument("stock", help="stock to track")
    parser.add_argument("--history", type=int, help="days back fo history", default=20)
    parser.add_argument("--raw", help="show raw data", action='store_true')
    u = parser.parse_args()

    show_data(u.stock, u.history, u.raw)

