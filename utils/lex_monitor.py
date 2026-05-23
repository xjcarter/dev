from json2html import json2html
import json
import argparse
from indicators import MondayAnchor
import calendar_calcs
from datetime import datetime
import pandas
import os
from bs4 import BeautifulSoup
import email_lib



DATA_DIR = os.getenv('DATA_DIR', '/trading/data/')
#DATA_DIR = os.getenv('DATA_DIR', '/home/jcarter/work/trading/data/')
WEEKDAYS = ['MON','TUE','WED','THU','FRI','SAT','SUN']

### THIS IS A CALENDAR TOOL TO IDENTIFY ACTION DAYS FOR THE MP STRATEGY
### mp (monday pullback) strategy
### simple long-only strategy trading a leveraged market etf (UPRO, UDOW) 
### the rules: 
### buy on next open if close[1] is lower than low price established on the first day of the week
### sell on first day the trade is profitable or after days duration,
### sell if stopped out on close (using standard stop)

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


## separate the html that is created from the json
## into separate tables
def format_tables(input_html):
    soup = BeautifulSoup(input_html, 'html.parser')

    outer_table = soup.find('table')

    tables_with_names = []
    internal_tables = outer_table.find_all('table') if outer_table else []

    # Extract HTML content and names of each internal table
    for table in internal_tables:
        table_name = table.find_previous('th').text if table.find_previous('th') else "Unknown Table"

        for th in table.find_all('th'):
            th['style'] = 'text-align: center; padding: 5px; font-family: Monaco, monospace;'
        for td in table.find_all(['td', 'th']):
            td['style'] = 'text-align: center; padding: 5px; font-family: Monaco, monospace;'

        table_html = str(table)
        tables_with_names.append((table_name, table_html))

    # Create HTML content with names and add line breaks
    separated_tables_html = ''
    for name, html in tables_with_names:
        table_with_style = f'<div><strong style="font-size: 16px; font-family: Monaco, monospace;">{name}</strong><br>{html}</div>'
        separated_tables_html += f'{table_with_style}<br>'

    return separated_tables_html


def show_calendar(stock, days_back):
    holidays = calendar_calcs.load_holidays()
    cal_columns = 'Date Day Anchor Open Close LEX'.split()

    stock_file = f'{DATA_DIR}/{stock}.csv'
    today = datetime.today().date()
    action = ''
    if calendar_calcs.is_end_of_week(today, holidays): action = '= NO TRADE'
    new_df, orig_df = adjust_prices(pandas.read_csv(stock_file))
    stock_df = new_df
    stock_df.set_index('Date',inplace=True)

    anchor = MondayAnchor(derived_len=days_back)

    gg = stock_df[-days_back:]

    lex_list = []
    for i in range(gg.shape[0]):
        idate = gg.index[i]
        stock_bar = gg.loc[idate]
        cur_dt = datetime.strptime(idate,"%Y-%m-%d").date()
        
        tt = anchor.push((cur_dt, stock_bar))
        mpv = "" 
        anchor_low = "" 
        if tt is not None:
            anchor_bar, mp = tt
            if mp < 0: mpv = round(mp,2)
            anchor_low = anchor_bar['Low']

        v = dict(zip(cal_columns,[idate, WEEKDAYS[cur_dt.weekday()], anchor_low, stock_bar['Open'], stock_bar['Close'], mpv]))
        lex_list.append(v)

    lex_json = json.dumps(dict(lex_signals=lex_list))
    html_table = format_tables( json2html.convert(json = lex_json) )

    sub_heading = f'Today= {WEEKDAYS[today.weekday()]}: {today.strftime("%Y-%m-%d")} {action}'
    heading = f'LEX Monitor: {stock}<br>{sub_heading}'
    heading_line = f'<div><strong style="font-family: Monaco, monospace; font-size: 16px;">{heading}</strong></div>'
    html_table = heading_line + '<br>' + html_table 

    for recipient in ['xjcarter@gmail.com', 'ggregory@hannibalinvestments.com']:
        email_lib.send_html(recipient, "Daily LEX Monitor", html_table)



if __name__ == '__main__':

    parser =  argparse.ArgumentParser()
    parser.add_argument("stock", help="stock to track")
    parser.add_argument("--history", type=int, help="days back fo history", default=20)
    u = parser.parse_args()

    show_calendar(u.stock, u.history)

