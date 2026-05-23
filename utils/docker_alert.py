import json
import os
import argparse
import time
import subprocess
import email_lib
import socket

"""
reads a json formatted config file in the format:
{
    "ib_portal": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    },
    "mysql_db": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    },
    "phpmyadmin": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    },
    "lex_strategy": { 
	    "status": "exited",
	    "emails": ["jcarter@hannibalinvestments.com"]
    }
}
json file holds container names to monitor and current status 
and the email to send alerts to
"""

#ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'jcarter@hannibalinvestments.com')
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'xjcarter@gmail.com')

## send email to recipient
def send_message(subject, msg, recipients=None):
    hostname = socket.gethostname()
    subject= f'CONTAINER [{hostname}] {subject}'
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

## poll_status() alerts of docker processes being down or running - 
## in the cronjob to run containers the --rm flag is used to remove the container
## once it completes or is stopped.
## IMPORTANT! -- this monitor assumes containers are NOT reused, new contaiers are built
##   each trading day.

def poll_status(container):
    bash_command = "docker inspect -f '{{.State.Status}}' " + container

    result = subprocess.run(bash_command, shell=True, capture_output=True, text=True)

    status = 'down' 
    if result.returncode != 0:
        subj = f'docker inspect {container} failed.'
        msg = bash_command + " failed."
        msg += f'\n{result.stderr}\n'
        ## ignore error where the container does not exist
        ## this allows the monitor to run constantly and not
        ## flag containers that we want to track but have been spun up yet.
        if 'No such object' not in msg:
            send_message(subj, msg, [ADMIN_EMAIL])
    else:
        status = result.stdout.strip()

    #import pdb;pdb.set_trace()
    return result.returncode, status


def rewrite_config_file(container_dict, file_path):
    with open(file_path, 'w') as f:
        s = json.dumps(container_dict, ensure_ascii=False, indent =4 )
        f.write(s + '\n')

def monitor_docker_containers(config_file):
    container_dict = read_json_file(config_file)

    while True:
        changed = False
        for container in container_dict.keys():
            status = container_dict[container].get('status','')
            emails = container_dict[container].get('emails',[])
            returncode, new_status = poll_status(container)

            if new_status != status: 
                subj = f'Status: {container} {new_status.upper()}'
                recipients = emails + [ADMIN_EMAIL]
                send_message(subj, subj, recipients)
                container_dict[container]['status'] = new_status
                changed = True

        if changed:
            rewrite_config_file(container_dict, config_file)
        time.sleep(30)
        
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file", required=True)
    u = parser.parse_args()

    ## $ python log_alerts.py --config=/utils/log_alerts.json
    monitor_docker_containers(u.config)

