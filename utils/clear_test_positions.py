import json
import random
import ib_endpoints2 as IB
from datetime import datetime
import time
import uuid

def get_account_dicts():
    jj = IB.get_subaccounts()
    sub_dicts = jj.get('accounts')
    return sub_dicts 


def get_account_list():
    sub_list = []
    for dd in get_account_dicts():
        sub_list.append(dd['name'])
    return sub_list 

def get_positions(accounts, symbol_id):
    positions = {}
    for account in accounts:
        cur_pos = IB.current_position(symbol_id, subaccount=account)
        positions[account] = cur_pos.get("position")
    return positions


def create_fa_group(fa_group, group_def):
    
    print(f'\ncreating group: {fa_group}')
    print(json.dumps(group_def, indent=4))

    print('\nposting group')
    ff = IB.create_allocation_group(group_def)
    print('reply')
    print(json.dumps(ff, indent=4))

    print('\nnew: allocation groups:')
    kk = IB.get_allocation_group(fa_group)
    print(json.dumps(kk, indent=4))




def send_order(contract_id, account_map, side):

    account_list = []
    total_shares = 0
    for account, shares in account_map.items():
        total_shares += abs(shares)
        account_list.append( dict(amount=abs(shares), name=account) )

    if total_shares == 0:
        print('error: total shares == 0, no order sent')
        return

    fa_group = f'FA_{uuid.uuid1()}'
    group_def = {
            "name": fa_group,
            "accounts": account_list,
            "default_method": "S"
    }

    create_fa_group(fa_group, group_def)
    
    
    time.sleep(3) 


    order_info = IB.order_request( contract_id, 'MKT', side, total_shares, fa_group=fa_group)
    if order_info.get('reply_id') is not None:
        ## confirm to server that you want to send this order
        ## repeat flag forces all subsequent rder_replies to be resolved before returning
        order_info = IB.order_reply(order_info['reply_id'], repeat=True)

    order_id = order_info['order_id']
    print(f'order_id: {order_id} successfully sent.', flush=True)


    time.sleep(3) 

    ## exit the program
    print(f'\nshutdown: {datetime.now().strftime("%H.%M.%S")}', flush=True)
    deleted = IB.delete_allocation_group(fa_group)
    print(f'\n{fa_group} delete response:\n {json.dumps(deleted, indent=4)}', flush=True)



if __name__ == "__main__":

    #AAPL = 265598
    UPRO = 61228752

    ## get list of sub_accounts
    accounts = get_account_list()

    ## returns a mapping account-to-position
    positions = get_positions(accounts, UPRO)
    print('old positions')
    print(positions)

    #send_order(UPRO, positions, 'SELL')

