
import logging
import json
from json2html import json2html
from datetime import datetime
import pytz
import argparse
import os
import sys
from enum import Enum
import pandas
import mysql.connector
from prettytable_utils import df_to_prettytable 
import ib_endpoints2 as IB
from bs4 import BeautifulSoup
import email_lib

MYSQL_HOSTNAME = os.environ.get('MYSQL_HOSTNAME', 'localhost')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'tarzan001')

"""
*** ace.py *** is the account management program for the automated strategies traded at IB
It has the following functionality:
1. upload from IB available eqity to trade from IB and allocate that cash to indivudal strategies.
2. will report available equity on demand
3. manage the allocation via allocation options: "pctEquity", "fixedEquity" through 
   config file ace.json
4. posts all daily alllocations to the DB to have an audit record of cash available, and cash allocated
Usage: python3 ace.py --alloc=ace_config.json, --report --strategy="strat1 strat2"
      alloc = mask of accounts to do bespoke allocatiions 
      report = report on all accounts by strategy
      *** default is --alloc where --alloc=None does all allocations as "passThru"
      *** you either run the app in --report mode or --alloc mode
      --strategy= space separeted strategies "strat1 strat2" to run --alloc functionality
"""

def get_time():
    return datetime.today().strftime('%Y%m%d')

#PORTFOLIO_DIRECTORY = os.getenv('PORTFOLIO_DIRECTORY', '/portfolio/')
PORTFOLIO_DIRECTORY = os.getenv('PORTFOLIO_DIRECTORY', '/home/jcarter/junk/admin')

# Create a FileHandler in 'append' mode
log_filename=f"{PORTFOLIO_DIRECTORY}/admin/logs/ace/ace.{get_time()}.log"
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

class AllocType(str, Enum):
    PCT_EQUITY = 'pctEquity'
    FIXED_EQUITY = 'fixedEquity'
    PASS_THRU = 'passThru'

def convert_timestamp( timestamp_utc ):
    timestamp_ny = timestamp_utc.astimezone(pytz.timezone('America/New_York'))
    return timestamp_ny

def _read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            file_contents = file.read()
            json_data = json.loads(file_contents)
        return json_data
    except json.JSONDecodeError as e:
        logger.critical(f"JSON decoding error for {filename}: {e}")
        logger.critical(f"Problematic JSON file contents: {file_contents}")


## separate the html that is created from the json
## into separate tables
def format_tables(input_html, column_formats):
    soup = BeautifulSoup(input_html, 'html.parser')

    ALIGNMENTS = {'c': 'center', 'l': 'left', 'r': 'right'}

    outer_table = soup.find('table')
    tables_with_names = []
    internal_tables = outer_table.find_all('table') if outer_table else []

    # Extract HTML content and names of each internal table
    for table in internal_tables:
        table_name = table.find_previous('th').text if table.find_previous('th') else "Unknown Table"

        # Extract column names from the table header
        column_names = [th.text.strip() for th in table.find('tr').find_all('th')]
        format_columns = [x['name'] for x in column_formats]

        # Apply column formatting styles
        for col_name in column_names:
            align = 'center'
            precision = None
            if col_name in format_columns:
                col_format = next((col for col in column_formats if col['name'] == col_name), None)
                if col_format:
                    align = ALIGNMENTS[col_format.get('align', 'c')]
                    precision = col_format.get('precision')
            style = f'text-align: {align}; padding: 5px; font-family: Monaco, monospace;'
            for tr in table.find_all('tr'):
                if tr.find('th'):
                    th = tr.find_all('th')[column_names.index(col_name)]
                    th['style'] = style
                elif tr.find('td'):
                    td = tr.find_all('td')[column_names.index(col_name)]
                    td['style'] = style
                    if precision is not None:
                        td.string = '{:,.{}f}'.format(float(td.text), precision)

        # Convert the modified table back to HTML
        table_html = str(table)
        tables_with_names.append((table_name, table_html))

    # Create HTML content with names and add line breaks
    separated_tables_html = ''
    for name, html in tables_with_names:
        table_with_style = f'<div><strong style="font-size: 16px; font-family: Monaco, monospace;">{name}</strong><br>{html}</div>'
        separated_tables_html += f'{table_with_style}<br>'

    return separated_tables_html


def email_report(title, table_data, column_formats):
    WEEKDAYS = ['MON','TUE','WED','THU','FRI','SAT','SUN']
    today = datetime.today().date()

    table_json = json.dumps(table_data)
    html_table = format_tables( json2html.convert(json = table_json) , column_formats)

    sub_heading = f'Today= {WEEKDAYS[today.weekday()]}: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
    heading = f'{title}<br>{sub_heading}'
    heading_line = f'<div><strong style="font-family: Monaco, monospace; font-size: 16px;">{heading}</strong></div>'
    html_table = heading_line + '<br>' + html_table

    for recipient in ['xjcarter@gmail.com']:
        email_lib.send_html(recipient, f"{title}", html_table)


def determine_allocation( account_dict, cash):
    """ account_dict example=
    { 
        "account": "XYZ",
        "allocType": "pctEquity",
        "allocValue": 10.5
    },
    """
    account = account_dict['account']
    alloc_type = account_dict['allocType']
    alloc_value = float(account_dict.get('allocValue', cash))

    if alloc_value < 0:
        logger.warning(f'trading alloc override:\naccount= {account}, allocValue= {alloc_value}')
        alloc_value = abs(alloc_value)
        logger.warning(f'defaulting to allocValue= {alloc_value}')

    portion = alloc_value
    if alloc_type == AllocType.PCT_EQUITY:
        portion = cash * alloc_value/100.0
    if portion > cash:
        logger.warning(f'trading alloc overflow:\n trading_alloc= {portion}, alloc_type= {alloc_type}, alloc_value= {alloc_value}')
        logger.warning(f'defaulting to trading_alloc = {cash}')

    return alloc_type, alloc_value, min(cash, portion)

def get_strategy_accounts():
    # Connect to the 'Operations' database
    connection = mysql.connector.connect(
        host=MYSQL_HOSTNAME,  # Replace with your MySQL server host
        user="root",  # Replace with your MySQL username
        password=MYSQL_PASSWORD,  # Replace with your MySQL password
        database="Operations"
    )

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    logger.info(f'fetching account-strategy map')

    # Perform the join query
    query = """
        SELECT accountId, strategyId 
        FROM StrategyAccount
    """
    cursor.execute(query)

    # Fetch all the results
    results = cursor.fetchall()

    # Print the retrieved data
    account_map = {}
    if results:
        for row in results:
            account_id, strategy_id= row
            account_map[account_id] = strategy_id
    else:
        err = f"No accounts found."
        logger.critical(err)
        raise RuntimeError(err)

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return account_map

## legacy sub_account parsing
def get_available_equity(sub_account):
    data_dicts = sub_account['data']
    name = sub_account['name']

    """ sample sub_account dict=
    {
            "data": [
                {
                    "value": "1003007.20",
                    "key": "NetLiquidation"
                },
                {
                    "value": "999951.45",
                    "key": "AvailableEquity"
                }
            ],
            "name": "DU9085813"
        },
    """

    for dd in data_dicts:
        if "AvailableEquity" in dd.values():
            return float(dd['value'])
    logger.warning(f'No \"AvailableEquity\" entry found for sub_account= {name}')
    return 0

## legacy sub_account data fetch
def get_sub_account_cash_old():
    sub_info = IB.get_subaccounts()
    account_cash = dict()
    import pdb; pdb.set_trace()
    sub_info_accounts = sub_info.get('accounts',{})
    logger.info(f'Accounts Visible:\n{json.dumps(sub_info_accounts, indent=4)}')
    for sub_account in sub_info_accounts: 
        account_cash[sub_account] = get_available_equity(sub_account)
    return account_cash

    """ OLD CRAP
    for sub_account in sub_info_accounts: 
        name = sub_account['name']
        account_cash[name] = get_available_equity(sub_account)
    return account_cash
    """

## current sub_account data fetch
def get_sub_account_cash_new():
    accounts_info = IB.get_accounts_info(fields=['availablefunds','settledcash'], currency='USD')
    import pdb; pdb.set_trace()
    account_cash = dict()
    logger.info(f'Accounts Visible:\n{json.dumps(accounts_info, indent=4)}')
    for account in accounts_info:
        value = 0
        try:
            value = float(account['availablefunds']) 
        except:
            pass
        account_cash[ account['account_id'] ] = value 

    return account_cash

## get current AvailableEquity
## fetch "AvailableEquity" information for each sub_account
## all sub_account AUTOMATICALLY fall under a parent fa_account for a SINGLE strategy
## where the parent fa Account is mapped to a SINGLE strategy.

def get_current_account_cash():
   
    """ 
    TESTING
    this check what account information is available to diagnosis account/account_value issues
    account_info = IB.get_account_catalog()
    acc_summary = IB.get_account_summary("DU9085815")
    print(f'Acc Detail:\n{json.dumps(acc_summary, indent=4)}')
    """

    return get_sub_account_cash_new()


def fetch_and_post( config_mask, strategy_list ):

    ## get account/strategy mapping
    strategy_accounts = get_strategy_accounts() 
    if strategy_list:
        reduced_accounts = {k:v for k,v in strategy_accounts.items() if v in strategy_list}
        strategy_accounts = reduced_accounts 

    ## fetch "AvailableEquity" information for each sub_account
    account_cash = get_current_account_cash()

    """ config_mask example=
    [
        {
            "strategy": "lex2",
            "accounts": [
                {
                    "account": "XYZ",
                    "allocType": "pctEquity",
                    "allocValue": 10.5
                },
                {
                    "account": "ABC",
                    "allocType": "fixedEquity",
                    "allocValue": 1000
                }
            ]
        },
    ]
    """

    dt = datetime.today().strftime("%Y-%m-%d")
    trading_allocs = []
    report_data = []
    for name, cash in account_cash.items():
        ## only update selected accounts 
        ## strategy_account starts out as the entire account/strategy universe
        ## it is only reduced if a fixed strategy list is given
        ## see get_strategy_accounts(strategy_list)
        if name not in strategy_accounts:
            continue

        strategy_id = strategy_accounts[name]

        alloc_type = AllocType.PASS_THRU.value 
        alloc_value = cash
        live_equity = cash

        ## handle specialized overrides from ace_config
        for alloc_config in config_mask:
            if alloc_config['strategy'] in strategy_accounts.values():
                for account_dict in alloc_config['accounts']:
                    if name == account_dict['account']:
                        alloc_type, alloc_value, live_equity= determine_allocation( account_dict, cash )
                        break

        v = dict(date=dt, accountId=name, availableEquity=cash)
        v.update( dict(allocType=alloc_type, allocValue=alloc_value, liveEquity=live_equity) )
        trading_allocs.append(v)

        # copy over data for reporting and add strategyId column
        u = dict(v)
        u.update( dict(strategyId=strategy_id) )
        report_data.append(u)

    # Connect to the 'Operations' database
    connection = mysql.connector.connect(
        host=MYSQL_HOSTNAME,  # Replace with your MySQL server host
        user="root",  # Replace with your MySQL username
        password=MYSQL_PASSWORD,  # Replace with your MySQL password
        database="Operations"
    )

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    cursor.execute("SET time_zone = 'America/New_York'")

    logger.info('alert: writing new trading alllocation amounts to AccountValue')

    for ta in trading_allocs:
        query = """
            INSERT INTO AccountValue (date, accountId, availableEquity, allocType, allocValue, liveEquity) \
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, list(ta.values()))

        # Commit the transaction
        connection.commit()

    # Close cursor and connection
    cursor.close()
    connection.close()

    if not report_data:
        logger.error("Match Failure: No accounts from IB matched the 'StrategyAccount' table in the DB.")
        logger.info(f"IB Accounts found: {list(account_cash.keys())}")
        return 

    # log live cash allocations for the day
    df = pandas.DataFrame(report_data)
    df = df.sort_values(by=['strategyId','accountId'], ascending=True)

    col_format = [
        {'name': 'accountId', 'align': 'l'},
        {'name': 'availableEquity', 'align': 'r', 'precision': 2},
        {'name': 'liveEquity', 'align': 'r', 'precision': 2},
        {'name': 'allocValue', 'align': 'r', 'precision': 2},
        {'name': 'allocType', 'align': 'l'},
        {'name': 'strategyId', 'align': 'l'}
    ]

    logger.info(f'\n\nCurrent Live Equity Catalog:\n{df_to_prettytable(df,col_format)}\n')
    
    email_report('Live Equity Report', dict(live_equity=report_data), col_format)


def run_allocations(alloc_config_file, strategy_list):
    config = [] 
    if alloc_config_file:
        config = _read_json_file(alloc_config_file)
        logger.info(f'trading_alloc override config:\n {json.dumps(config, indent=4)}')

    if strategy_list:
        logger.info(f'posting for ONLY strategy list= {strategy_list} accounts.')
    else:
        logger.info(f'posting for ALL accounts, ALL strategies.')

    fetch_and_post( config, strategy_list )
    
def get_last_equity_posted():

    connection = mysql.connector.connect(
        host=MYSQL_HOSTNAME,  # Replace with your MySQL server host
        user="root",  # Replace with your MySQL username
        password=MYSQL_PASSWORD,  # Replace with your MySQL password
        database="Operations"
    )

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    logger.info(f'fetching last posted AvailableEquity')
    query= """
        SELECT t.accountId, t.availableEquity, t.timestamp FROM AccountValue t \
        JOIN ( SELECT accountId, MAX(timestamp) as mst FROM AccountValue GROUP BY accountId ) maxt \
        ON t.accountId = maxt.accountId and maxt.mst = t.timestamp
    """ 

    cursor.execute(query)

    # Fetch all the results
    results = cursor.fetchall()

    # Print the retrieved data
    equity_map = {}
    if results:
        for row in results:
            account_id, prev_equity, timestamp = row
            ts = convert_timestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')
            equity_map[account_id] = (float(prev_equity), ts)
    else:
        err = f"No accounts found."
        logger.critical(err)
        raise RuntimeError(err)

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return equity_map

def run_report():
    ## dict mapping of dict[account] = strategy
    ## NOTE one unique account belongs to one unique strategy
    strategy_accounts = get_strategy_accounts()
    account_cash = get_current_account_cash()
    prev_equity_map = get_last_equity_posted()

    ts_now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    data = []
    for account, current_equity in account_cash.items():
        if account in strategy_accounts:
            strategy_id = strategy_accounts[account]
            prev_equity, prev_ts = prev_equity_map[account]
            delta = current_equity - prev_equity
            v = dict(strategyId=strategy_id, accountId=account, equity=current_equity)
            v.update( dict(prevEquity=prev_equity, delta=delta, prevTS=prev_ts, currTS=ts_now) )
            data.append(v)

    df = pandas.DataFrame(data)
    summed_df = df[['strategyId','equity','prevEquity']].groupby('strategyId').sum()
    summed_df = summed_df.reset_index()

    col_format = [
        {'name': 'accountId', 'align': 'l'},
        {'name': 'equity', 'align': 'r', 'precision': 2},
        {'name': 'prevEquity', 'align': 'r', 'precision': 2},
        {'name': 'delta', 'align': 'r', 'precision': 2},
        {'name': 'strategyId', 'align': 'l'}
    ]

    report = f'\n\nAccount Equity Report:\n{df_to_prettytable(df, col_format)}'
    report += f'\nEquity Report Totals:\n{df_to_prettytable(summed_df, col_format)}\n'
    logger.info(report)

    account_data = df.to_dict(orient='records')
    total_data = summed_df.to_dict(orient='records')
    email_report('Account Equity Report', dict(accounts=account_data,totals=total_data), col_format)


def run(alloc_config, report, strategies=None):
    strategy_list = []
    if strategies:
        ## space seperated strategy labels
        strategy_list = strategies.split()

    if report: 
        logger.info(f'REPORT START')
        run_report()
        logger.info(f'REPORT END')
    else:
        ## if alloc_config (json) is given - use that as allocation mask
        logger.info(f'ALLOCATIONS START')
        run_allocations(alloc_config, strategy_list)
        logger.info(f'ALLOCATIONS END')


if __name__ == "__main__":
    parser =  argparse.ArgumentParser()
    parser.add_argument("--alloc", help="run allocations", default=None)
    parser.add_argument("--report", help="report available equtiy", action='store_true')
    parser.add_argument("--strategy", help="run for specified strategies", default=None)
    u = parser.parse_args()

    run(u.alloc, u.report, u.strategy)
