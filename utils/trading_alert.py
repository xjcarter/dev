from json2html import json2html
import json
from bs4 import BeautifulSoup
import email_lib
import pyinotify
import os
import argparse
from datetime import datetime
import glob
import time
import socket

PORTFOLIO_DIRECTORY = os.environ.get('PORTFOLIO_DIRECTORY', '/home/jcarter/junk/portfolio/')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'jcarter@hannibalinvestments.com')


def read_json_file(file_path):
    try:
        with open(file_path, 'rb') as file:
            file_contents = file.read()
            json_data = json.loads(file_contents) 
        return json_data
    except json.JSONDecodeError as e:
        print(f"JSON decoding error for file {file_path}: {e}")
        print(f"Problematic JSON file contents: {file_contents}")
        send_message(file_path, f"Reading file: File '{file_path}' Error decoding JSON: {e}", [ADMIN_EMAIL])
    except FileNotFoundError:
        send_message(file_path, f"Reading file: File '{file_path}' not found.", [ADMIN_EMAIL])

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


def frame_positions_file_to_html( file_path ):

    json_data = read_json_file( file_path )

    ## remove allocations and total allocations table
    positions_dict = dict(json_data)
    del positions_dict['allocations']
    del positions_dict['total_allocation']

    positions_dict['position_detail'].reverse()

    json_data = json.dumps(positions_dict)

    return format_tables( json2html.convert(json = json_data) )


def frame_orders_file_to_html( file_path ):

    json_data = read_json_file( file_path )

    order_dict = dict(orders=list(json_data)[::-1])
    json_data = json.dumps(order_dict)
    
    return format_tables( json2html.convert(json = json_data) )


def send_html_tables(strategy, file_path, recipients, is_order_file):
    hostname = socket.gethostname()
    if is_order_file:
        subject = f'TRADING [{hostname}] Order Update: {strategy}'
        heading = f'Orders File: {file_path}'
        html_table = frame_orders_file_to_html(file_path)
    else:
        subject = f'TRADING [{hostname}] Position Update: {strategy}'
        heading = f'Positions File: {file_path}'
        html_table = frame_positions_file_to_html(file_path)

    heading_line = f'<div><strong style="font-family: Monaco, monospace; font-size: 16px;">{heading}</strong></div>'
    html_table = heading_line + '<br>' + html_table

    if recipients:
        for recipient in recipients:
            email_lib.send_html(recipient, subject, html_table)


## send email to recipient
def send_message(file_path, msg, recipients=None):
    basename = os.path.basename(file_path)
    subject= f'JSON Error: {basename}'
    email_body= f'file: {file_path}:<br>{msg}'
    if recipients:
        for recipient in recipients:
            email_lib.send_email(recipient, subject, email_body)

def file_modified_since(filepath, reference_datetime):
    try:
        # Get the last modification time of the file
        file_modification_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        # Compare with the reference datetime
        return file_modification_time > reference_datetime
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return False

def check_alert(trading_file_path, alert_dict):
    ## check alerts for files: <strategy>.<YYYYMMDD>.positions.json
    ## or <strategy>.<YYYYMMDD>.orders.json

    recipients = []
    strategy_name = None
    is_order_file, is_posn_file = [ tag in trading_file_path for tag in ['orders','positions']]
    if is_order_file or is_posn_file:
        basename = os.path.basename(trading_file_path)
        strategy_name = basename.split('.')[0]
        if strategy_name in alert_dict.keys():
            recipients = alert_dict[strategy_name].get('emails',[]) 
    return recipients, strategy_name, is_order_file, is_posn_file


def monitor_trading_logs(config_file):
    alert_dict = read_json_file(config_file)

    rpt_directories = []
    # Specify the directories to monitor
    for basename in alert_dict.keys():
        for path in ['positions', 'trades']:
            trading_dir = f'{PORTFOLIO_DIRECTORY}/{basename}/{path}'
            if os.path.exists(trading_dir):
                rpt_directories.append(trading_dir)

    # Initialize the inotify watcher
    wm = pyinotify.WatchManager()

    class EventHandler(pyinotify.ProcessEvent):

        def check_update(self, file_path, where):
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f'{now} checking: {file_path}, where={where}')
            recipients, strategy, is_order_alert, is_posn_alert = check_alert(file_path, alert_dict)
            print(f'{now} {recipients} {strategy}, is_order={is_order_alert}, is_posn={is_posn_alert}')
            if recipients:
                time.sleep(1)
                send_html_tables(strategy, file_path, recipients, is_order_alert)
            return is_order_alert or is_posn_alert 

        def process_IN_CREATE(self, event):
            if self.check_update(event.pathname, where='NEW'):
                # A new position or orders file was created, so add it to the watch list
                wm.add_watch(event.pathname, pyinotify.IN_MODIFY)

        def process_IN_MODIFY(self, event):
            self.check_update(event.pathname, where='MOD')

    # ref time when the trading_alert started up
    # this handles restarts so that you aren't getting spammed
    # with emails reflecting scans for tile that already exist
    TRADNG_ALERT_START = datetime.now()

    # Process existing log files that have been modified since TRADNG_ALERT_START,
    # and start monitoring new changes.
    # Also watch for new files that have been created.

    for rpt_directory in rpt_directories:
        wm.add_watch(rpt_directory, pyinotify.IN_CREATE)

        for root, dirs, files in os.walk(rpt_directory):
            for file in files:
                file_path = os.path.join(root, file)
                recipients, strategy, is_order_alert, is_posn_alert = check_alert(file_path, alert_dict)
                if recipients:
                    if file_modified_since(file_path, TRADNG_ALERT_START):
                        send_html_tables(strategy, file_path, recipients, is_order_alert)
                    # Start watching the file for modifications
                    wm.add_watch(file_path, pyinotify.IN_MODIFY)

    handler = EventHandler()
    notifier = pyinotify.Notifier(wm, handler)

    try:
        while True:
            notifier.process_events()
            if notifier.check_events():
                notifier.read_events()
    except KeyboardInterrupt:
        pass
    finally:
        notifier.stop()

"""
reads a json formatted config file in the format:
{
	"test": {
		  "emails": ["xjcarter@gmail.com"]
		},
	"lex": {
		  "emails": ["xjcarter@gmail.com", "hannibal@hannibalinvestments.com"]
		}

}
main key is the strategy name to monitor
'emails' is who to send emails to when new orders are placed, or trades executed
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    u = parser.parse_args()

    ## $ python log_alerts.py --config=/utils/log_alerts.json
    monitor_trading_logs(u.config)

