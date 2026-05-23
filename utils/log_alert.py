import pyinotify
import json
import os
import argparse
from datetime import datetime
from collections import deque
import tempfile
import glob
import email_lib
import time
import socket

"""
reads a json formatted config file in the format:
{
     "Lex": {
              "keywords": ["special"],
              "emails": [ "xjcarter@gmail.com" ],
      }
     "globals": ["bingo", "critical", "error"]
}
json file holds keywords to found in logs to alert via email
"""

PORTFOLIO_DIRECTORY = os.environ.get('PORTFOLIO_DIRECTORY', '/home/jcarter/junk/portfolio/')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'jcarter@hannibalinvestments.com')


def format_string(input_string):
    lines = input_string.split('\n')
    return '<br>'.join(lines)

## send email to recipient
def send_message(log_file_path, alert, label, msg, recipients=None, attachment_path=None, attachment_title=None):
    msg = format_string(msg)
    basename = os.path.basename(log_file_path)
    hostname = socket.gethostname()
    subject= f'LOG ALERT {alert.upper()} [{hostname}]: {label}'
    email_body= f'{msg}<br><br>file: {log_file_path}'
    if recipients:
        for recipient in recipients:
            email_lib.send_email(recipient, subject, email_body, attachment_path, attachment_title)

## reads a json formatted config file

def read_json_file(file_path):
    try:
        print(f'file_path = {file_path}')
        with open(file_path, 'r') as file:
            file_contents = file.read()
            json_data = json.loads(file_contents)
        return json_data
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        print(f"Problematic JSON file contents: {file_contents}")
    except FileNotFoundError:
        print(f"Reading file: File '{file_path}' not found.")


## normalize all words you are checking for against the text
def contains_word(text, word_list):
    alerts = [x.lower() for x in word_list]
    txt = text.lower()
    for alert in alerts:
        if alert in txt:
            return alert
    return None 

def get_log_filters(dir_path, alert_dict):
    strategy_dict = alert_dict.get(dir_path, {})
    label = strategy_dict.get('label', '') 
    recipients = strategy_dict.get('emails', None)
    strategy_alerts = strategy_dict.get('keywords', [])
    suffixes = strategy_dict.get('suffixes', ['.log'])
    return label, strategy_alerts, suffixes, recipients


def file_modified_since(filepath, reference_datetime):
    try:
        # Get the last modification time of the file
        file_modification_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        # Compare with the reference datetime
        return file_modification_time > reference_datetime
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return False


def create_tail_file(lines):
    if lines:
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_file.write('\n'.join(lines))
            temp_file_name = temp_file.name
        return temp_file_name

def monitor_log_alerts(config_file):
    alert_defs = read_json_file(config_file)
    alert_dict = alert_defs.get('logs',{})
    global_alerts = alert_defs.get('globals', ['critical', 'error', 'warning'])

    log_dict = {}
    
    log_directories = []
    # Specify the directories to monitor
    for log_dir in alert_dict.keys():
        if os.path.exists(log_dir):
            log_directories.append(log_dir)

    # Initialize the inotify watcher
    wm = pyinotify.WatchManager()

    def _endswith(string, suffixes):
        for suffix in suffixes:
            if string.endswith(suffix):
                return True
        return False

    def check_alerts(file_path, label, strategy_alerts, recipients, bundle):            
        ## give the file time to dump context
        time.sleep(1.5)

        text_block = None
        if bundle: text_block = []

        log_marker = marker = None
        last_alert = None
        alert_string_list = global_alerts + strategy_alerts
        with open(file_path, 'r') as file:
            last_lines = deque(maxlen=70)
            for line_no, line in enumerate(file):
                line = line.strip('\n')

                ## get the log line prefix
                if not log_marker: log_marker = line[:min(len(line),14)]

                formatted_line = f'{line_no:04d}: {line}'
                last_lines.append(formatted_line)
                alert = contains_word(line, alert_string_list)
                if alert is not None:
                    last_alert = alert
                    if bundle:
                        text_block.append(formatted_line)
                    else:
                        text_block = [formatted_line]
                    marker = log_marker
                else:
                    if marker:
                        if line.startswith(marker):
                            marker = None
                        else:
                            ## collect all text that doesn't begin with a log marker
                            text_block.append(formatted_line)

            if text_block and text_block[0] != log_dict.get(file_path, None):
                tail_file = create_tail_file(last_lines)
                send_message(file_path, last_alert, label, '<br>'.join(text_block), recipients, tail_file, 'log_tail.txt')
                log_dict[file_path] = text_block[0] 


    class EventHandler(pyinotify.ProcessEvent):
        def process_IN_CREATE(self, event):
            dirname = f'{os.path.dirname(event.pathname)}/' 
            label, strategy_alerts, suffixes, recipients = get_log_filters(dirname, alert_dict)
            if recipients and _endswith(event.pathname, suffixes):
                check_alerts(event.pathname, label, strategy_alerts, recipients, bundle=True)            
                # A new .log file was created, so add it to the watch list
                wm.add_watch(event.pathname, pyinotify.IN_MODIFY)

        def process_IN_MODIFY(self, event):
            dirname = f'{os.path.dirname(event.pathname)}/' 
            label, strategy_alerts, suffixes, recipients = get_log_filters(dirname, alert_dict)
            if recipients and _endswith(event.pathname, suffixes):
                check_alerts(event.pathname, label, strategy_alerts, recipients, bundle=False)            

    # ref time when the log_alert started up
    # this handles restarts so that you aren't getting spammed
    # with emails reflecting scans for tile that already exist
    LOG_ALERT_START = datetime.now()

    # Process existing log files that have been modified since LOG_ALERT_START,
    # and start monitoring new changes.
    # Also watch for new files that have been created.


    for log_directory in log_directories:
        wm.add_watch(log_directory, pyinotify.IN_CREATE)

        for root, dirs, files in os.walk(log_directory):
            for file in files:
                file_path = os.path.join(root, file)
                label, strategy_alerts, suffixes, recipients = get_log_filters(root, alert_dict)
                if recipients and _endswith(file, suffixes):
                    # send bundled change email for and current files that
                    # that changed since we restarted the log_alert
                    #if file_modified_since(file_path, LOG_ALERT_START):
                    if file_modified_since(file_path, LOG_ALERT_START):
                        check_alerts(file_path, label, strategy_alerts, recipients, bundle=True)            
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    u = parser.parse_args()

    ## $ python log_alerts.py --config=/utils/log_alerts.json
    monitor_log_alerts(u.config)

