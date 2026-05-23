
import json
from trading_alert import read_json_file, send_html_tables

def frame( file_path ):
    json_data = read_json_file( file_path)
    order_dict = dict(orders=list(json_data))
    json_data = json.dumps(order_dict)

def check_BOM( file_path ):
    with open(file_path, 'rb') as file:
        first_three_bytes = file.read(3)

    if first_three_bytes == b'\xef\xbb\xbf':
        print("BOM detected (UTF-8 with BOM)")
    else:
        print("No BOM detected")

#def send_html_tables(strategy, file_path, recipients, is_order_file):

#s = '/portfolio/test/trades/test.orders.20231208.json'
recipients = ['xjcarter@gmail.com']
strategy='test'
is_order_file = False 
file_path = '/portfolio/test/positions/test.positions.20231208.json'
#file_path = '/portfolio/test/trades/test.orders.20231208.json'

send_html_tables(strategy, file_path, recipients, is_order_file)
