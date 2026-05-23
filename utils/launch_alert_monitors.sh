#!/bin/bash

## 6:30 
nohup /trading/utils/notify.sh "python3 /trading/utils/docker_alert.py --config=/trading/utils/docker_alert.json" &
## 6:30 
nohup /trading/utils/notify.sh "python3 /trading/utils/process_alert.py --config=/trading/utils/process_alert.json" &
## 8:00
nohup /trading/utils/notify.sh "python3 /trading/utils/log_alert.py --config=/trading/utils/log_alert.json" &
## 9:00
nohup /trading/utils/notify.sh "python3 /trading/utils/trading_alert.py --config=/trading/utils/trading_alert.json" &
## check if everything is running
ps -ef | grep trading | awk '$8 == "python3"'
