
import ib_endpoints2 as IB
import json
from datetime import datetime
import time 

def poll_account():
    for i in range(1,11):
        subs = IB.get_subaccounts()
        accounts = subs['accounts']
        v = next((account for account in accounts if account['name'] == "DU9085813"), None)
        print(f'\n[{i:02d}] TS= {datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}')
        print(json.dumps(v, indent=4))
        time.sleep(600)

def get_all_accounts():
        subs = IB.get_subaccounts()
        accounts = subs['accounts']
        print(json.dumps(accounts, indent=4))

get_all_accounts()
#poll_account()
