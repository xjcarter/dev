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

    now = datetime.now().strftime("%H%M%S")
    fa_group = f'FA_CANCEL_TEST_{now}'
    group_def = {
            "name": fa_group,
            "accounts": account_list,
            "default_method": "S"
    }

    create_fa_group(fa_group, group_def)

    stage1 = TripWire("09:40")
    stage2 = TripWire("09:42")
    stage3 = TripWire("09:44")

    AAPL = 265598

    accounts = [ x.get('name') for x in account_list ]
    positions = None
    captured = []
    order_id = None

    while True:

        orders = IB.order_status()
        for order in orders:
            order_id = order['orderId']
            if order_id not in captured: 
                print(f'\norders found: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                print(json.dumps(order, indent=4), flush=True)
                captured.append(order_id)
            

        with stage1 as one:
            if one:
                order_info = IB.order_request( AAPL, 'LIMIT', 'BUY', total_shares, lmt_price=170, fa_group=fa_group)
                if order_info.get('reply_id') is not None:
                    ## confirm to server that you want to send this order
                    ## repeat flag forces all subsequent rder_replies to be resolved before returning
                    order_info = IB.order_reply(order_info['reply_id'], repeat=True)

                order_id = order_info['order_id']
                print(f'order_id: {order_id} successfully sent.\n{order_info}', flush=True)


        with stage2 as two:
            if two:
                ## must call get_accounts() first
                print('\ncalling get_accounts()')
                accs = IB.get_accounts()
                print(f'accounts:\n{json.dumps(accs, indent=4)}')
                ## CANCEL order 
                print('\ncancelling {order_id}')
                cancel_info = IB.cancel_order( order_id, fa_group )
                print(f'order: {order_id} successfully cancelled.\n{json.dumps(cancel_info, indent=4)}', flush=True)
                
                time.sleep(5)
                print('checking cancel status')
                orders = IB.order_status()
                for order in orders:
                    ## find the cancelled order
                    ## orderIds coming back from order_status are INTs.
                    ## just compare ids as same type
                    if str(order_id) == str(order['orderId']): 
                        print(f'\nCXL order found: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                        print(json.dumps(order, indent=4), flush=True)


        with stage3 as three:
            if three:
                ## exit the program
                print(f'\nshutdown: {datetime.now().strftime("%H.%M.%S")}', flush=True)
                deleted = IB.delete_allocation_group(fa_group)
                print(f'\n{fa_group} delete response:\n{json.dumps(deleted, indent=4)}', flush=True)
                break

        time.sleep(3)


if __name__ == "__main__":
    run_order_cancel_with_fa_group()

