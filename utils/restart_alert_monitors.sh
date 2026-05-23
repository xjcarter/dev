#!/bin/bash

## kill and restart docker trading monitor - email alerts
pkill -f trading_alert.py
nohup /trading/utils/notify.sh "python3 /trading/utils/trading_alert.py --config=/trading/utils/trading_alert.json" &
## kill and restart docker container monitor - email alerts
pkill -f docker_alert.py
nohup /trading/utils/notify.sh "python3 /trading/utils/docker_alert.py --config=/trading/utils/docker_alert.json" &
## kill and restart process monitor - email alerts
pkill -f process_alert.py
nohup /trading/utils/notify.sh "python3 /trading/utils/process_alert.py --config=/trading/utils/process_alert.json" &
## kill and restart log monitor - email alerts
pkill -f log_alert.py
## start the new log_alert monitor for the next 24hrs...
nohup /trading/utils/notify.sh "python3 /trading/utils/log_alert.py --config=/trading/utils/log_alert.json" &
