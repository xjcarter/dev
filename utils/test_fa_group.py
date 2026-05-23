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


def run_fa_group_test():

    jj = IB.get_subaccounts()
    print('\nSub Accounts:')
    print(json.dumps(jj, indent=4))

    sub_list= jj.get('accounts')
    n = len(sub_list)

    account_list = []
    total_shares = 50
    shares = generate_random_array(total_shares, n)
    for i, account_dict in enumerate(sub_list):
        account_list.append( dict(amount=shares[i], name=account_dict.get("name")) )

    fa_group = 'FA_GROUP_TEST'
    group_def = {
            "name": fa_group,
            "accounts": account_list,
            "default_method": "S"
    }

    create_fa_group(fa_group, group_def)

    stage1 = TripWire("9:40")
    stage2 = TripWire("9:45")
    stage3 = TripWire("9:50")

    AAPL = 265598
    print('\norder preview:')
    order_preview = IB.order_preview( AAPL, 'MKT', 'BUY', total_shares, fa_group=fa_group)
    print(json.dumps(order_preview, indent=4), flush=True)

    accounts = [ x.get('name') for x in account_list ]
    positions = None
    captured = []

    while True:

        orders = IB.order_status()
        for order in orders:
            order_id = order['orderId']
            if order_id not in captured: 
                print(f'\norders found: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                print(json.dumps(order, indent=4), flush=True)

                positions = get_positions(accounts, AAPL)
                print(f'\npositions:')
                print(json.dumps(positions, indent=4))

                captured.append(order_id)

        with stage1 as one:
            if one:
                order_info = IB.order_request( AAPL, 'MKT', 'SELL', total_shares, fa_group=fa_group)
                if order_info.get('reply_id') is not None:
                    ## confirm to server that you want to send this order
                    ## repeat flag forces all subsequent rder_replies to be resolved before returning
                    order_info = IB.order_reply(order_info['reply_id'], repeat=True)

                order_id = order_info['order_id']
                print(f'order_id: {order_id} successfully sent.', flush=True)


        with stage2 as two:
            if two:
                new_list = [ dict(amount=abs(v),name=k) for k,v in positions.items() ]
                new_shares = abs(sum(positions.values()))
                fa_group = 'FA_GROUP_TEST'
                group_def = {
                        "name": fa_group,
                        "accounts": new_list,
                        "default_method": "S"
                }
                create_fa_group(fa_group, group_def)

                ## UNWIND all positions
                if new_shares != 0:
                    order_info = IB.order_request( AAPL, 'MKT', 'BUY', new_shares, fa_group=fa_group)
                    if order_info.get('reply_id') is not None:
                        ## confirm to server that you want to send this order
                        ## repeat flag forces all subsequent rder_replies to be resolved before returning
                        order_info = IB.order_reply(order_info['reply_id'], repeat=True)

                    order_id = order_info['order_id']
                    print(f'order_id: {order_id} successfully sent.', flush=True)


        with stage3 as three:
            if three:
                ## exit the program
                print(f'\nshutdown: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                deleted = IB.delete_allocation_group(fa_group)
                print(f'\n{fa_group} delete response:\n {json.dumps(deleted, indent=4)}', flush=True)
                break

        time.sleep(3)


if __name__ == "__main__":
    run_fa_group_test()

