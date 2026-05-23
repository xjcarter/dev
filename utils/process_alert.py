import json
import os
import argparse
import time
import subprocess
import socket
import email_lib

"""
reads a json formatted config file in the format:
dictionary keys are unique strings that exist in the 'ps' process status string
{
    "lex_new.py": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    },
    "log_alert.py": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    },
    "trading_alert.py": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    },
}
json file holds process names to monitor and current status 
and the email to send alerts to
"""

#ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'jcarter@hannibalinvestments.com')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'xjcarter@gmail.com')

## send email to recipient
def send_message(subject, msg, recipients=None):
    hostname = socket.gethostname()
    subject= f'PROCESS [{hostname}] {subject}'
    email_body= msg
    if recipients:
        for recipient in recipients:
            email_lib.send_email(recipient, subject, email_body)

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
        send_message(file_path, f"Reading file: File '{file_path}' Error decoding JSON: {e}", [ADMIN_EMAIL])
    except FileNotFoundError:
        send_message(file_path, f"Reading file: File '{file_path}' not found.", [ADMIN_EMAIL])


## poll_status() alerts of native processes being down or running - 
## in the cronjob to run processs the --rm flag is used to remove the process
## once it completes or is stopped.

def poll_status(process):
    #bash_command = f'pgrep -fn {process}' 
    bash_command = f"ps aux | grep {process} | grep -v grep"

    result = subprocess.run(bash_command, shell=True, capture_output=True, text=True)

    status = 'down' 
    if result.returncode != 0:
        subj = f'PROCESS CHECK for {process} failed.'
        msg = bash_command + " failed."
        msg += f'\n{result.stderr}\n'
        ## ignore error where the process does not exist (i.e. stderr='' )
        ## this allows the monitor to run constantly and not
        ## flag processs that we want to track but have been spun up yet.
        if result.stderr:
            msg += result.stderr
            send_message(subj, msg, [ADMIN_EMAIL])
    else:
        process_id = result.stdout.strip()
        if process_id:
            status = 'running'

    return result.returncode, status


def rewrite_config_file(process_dict, file_path):
    with open(file_path, 'w') as f:
        s = json.dumps(process_dict, ensure_ascii=False, indent =4 )
        f.write(s + '\n')

def monitor_processes(config_file):
    process_dict = read_json_file(config_file)

    while True:
        changed = False
        for process in process_dict.keys():
            status = process_dict[process].get('status','')
            emails = process_dict[process].get('emails',[])
            returncode, new_status = poll_status(process)

            if new_status != status: 
                subj = f'Status: {process} {new_status.upper()}'
                recipients = emails + [ADMIN_EMAIL]
                send_message(subj, subj, recipients)
                process_dict[process]['status'] = new_status
                changed = True

        if changed:
            rewrite_config_file(process_dict, config_file)
        time.sleep(30)
        
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    u = parser.parse_args()

    ## $ python process_alerts.py --config=/utils/process_alerts.json
    monitor_processes(u.config)

