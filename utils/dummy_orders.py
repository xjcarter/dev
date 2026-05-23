
import json
import time
from datetime import datetime

def write_orders(order_list):
    f='/portfolio/test/trades/test.orders.20231209.json'
    with open(f, 'w') as file:
        vv = json.dumps(order_list, indent=4)
        file.write(vv)
        print(vv+'\n')

i = 0
orders_list = []
while i < 10:

    time.sleep(10)

    ts = datetime.now().strftime("%Y%m%d-%H:%M:%S")
    order = dict()
    order['order_id'] = f'{i:09d}'
    order['symbol'] = 'AAPL'
    order['qty'] = 37
    order['side'] = 'BUY'
    order['order_type'] = 'MKT'
    order['order_target'] = None
    order['timestamp'] = ts
    order['info'] = 'testing'

    orders_list.append(order)
    write_orders(orders_list)
    i += 1




