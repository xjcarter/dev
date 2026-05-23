import json
import random
import ib_endpoints2 as IB
from clockutils import TripWire
from datetime import datetime
import time


def generate_random_array(total_sum, N):
    # Generate N-1 random integers between 1 and total_sum//N
    rand_nums = [random.randint(1, total_sum // N) for _ in range(N - 1)]
    # Calculate the last number to ensure sum equals total_sum
    rand_nums.append(total_sum - sum(rand_nums))
    # Shuffle the list to make it more random
    random.shuffle(rand_nums)
    return rand_nums


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


def run_order_cancel_with_fa_group():

    jj = IB.get_subaccounts()
    print('\nSub Accounts:')
    print(json.dumps(jj, indent=4))

    sub_list= jj.get('accounts')
    n = len(sub_list)

    account_list = []
    total_shares = 300 
    shares = generate_random_array(total_shares, n)
    for i, account_dict in enumerate(sub_list):
        account_list.append( dict(amount=shares[i], name=account_dict.get("name")) )


    fa_groups = []
    for i in range(2):
        now = datetime.now().strftime("%H%M%S")
        fa_group = f'FA_CANCEL_TEST_{now}_{i}'

        group_def = {
                "name": fa_group,
                "accounts": account_list,
                "default_method": "S"
        }

        create_fa_group(fa_group, group_def)
        fa_groups.append(fa_group)

    stage1 = TripWire("09:55")
    stage2 = TripWire("09:57")
    stage3 = TripWire("09:59")

    AAPL = 265598

    accounts = [ x.get('name') for x in account_list ]
    positions = None
    captured = {} 
    order_pairs = [] 

    while True:

        orders = IB.order_status()
        for order in orders:
            order_id = str(order['orderId'])
            if order_id not in list(captured.keys()): 
                print(f'\norders found: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                print(json.dumps(order, indent=4), flush=True)
                captured[order_id] = True 
            

        with stage1 as one:
            if one:
                for i, fa_group in enumerate(fa_groups):
                    order_info = IB.order_request( AAPL, 'LIMIT', 'BUY', total_shares, lmt_price=170-(i*10), fa_group=fa_group)
                    if order_info.get('reply_id') is not None:
                        ## confirm to server that you want to send this order
                        ## repeat flag forces all subsequent rder_replies to be resolved before returning
                        order_info = IB.order_reply(order_info['reply_id'], repeat=True)
   
                    j = str(order_info['order_id'])
                    v = dict(r=j, fa=fa_group) 
                    order_pairs.append( v )
                    print(f'order_id: {j} successfully sent.\n{order_info}', flush=True)


        with stage2 as two:
            if two:
                ## must call get_accounts() first
                print('\ncalling get_accounts()')
                accs = IB.get_accounts()
                print(f'accounts:\n{json.dumps(accs, indent=4)}')
                ## CANCEL orders

                for p in order_pairs:
                    order_id, fa_group = p['r'], p['fa']
                    print('\ncancelling order_id= {order_id}, fa_group= {fa_group}')
                    cancel_info = IB.cancel_order( order_id, fa_group )
                    print(f'order: {order_id} successfully cancelled.\n{json.dumps(cancel_info, indent=4)}', flush=True)
                    ## removed original placed order
                    ## so that it can re-appear in the next order_status() call, and we can make sure it's cancelled
                    del captured[order_id]

        with stage3 as three:
            if three:
                ## exit the program
                print(f'\nshutdown: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                for fa_group in fa_groups:
                    deleted = IB.delete_allocation_group(fa_group)
                    print(f'\n{fa_group} delete response:\n{json.dumps(deleted, indent=4)}', flush=True)
                    time.sleep(1)
                break

        time.sleep(3)


if __name__ == "__main__":
    run_order_cancel_with_fa_group()

