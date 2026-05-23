
import ib_endpoints2
import json

#ib_endpoints2.switch_to_account('DU7631004')
#ib_endpoints2.portfolio_accounts()
#ib_endpoints2.account_trades()
#ib_endpoints2.portfolio_for_account()

symbol='SPX'
contract_id = ib_endpoints2.symbol_to_contract_id(symbol)
print(f'contract_id = {contract_id}, symbol= {symbol}')

#symbol='ESTR'
#contract_info = ib_endpoints2.fetch_contract_info( [symbol], sec_type='futures' )
#print(json.dumps(contract_info, indent=4))

#contract_details = ib_endpoints2.fetch_contract_details( 620731015 )
#print(json.dumps(contract_details, indent=4))

#market_data = ib_endpoints2.market_data_history(666994090, exchange='CFE', period='1d', bar='1d', start_time='20240605-13:30:00', outside_rth=True)
#market_data = ib_endpoints2.market_data_history(666994090, exchange='CFE', period='3d', bar='1d', start_time='', outside_rth=False)
#print(json.dumps(market_data, indent=4))
#vv = ib_endpoints2.market_snapshot( contract_id )  
#print(json.dumps(vv, ensure_ascii=False, indent=4))

#account_info = ib_endpoints2.account_summary()
#print(json.dumps(account_info, ensure_ascii=False, indent=4))
#account_file = f'{strategy_id}.account_info.json'
#with open(account_file, 'w') as f:
#    acc_info = json.dumps(account_info, ensure_ascii=False, indent=4)
#    f.write(acc_info)

#pp = ib_endpoints2.current_position(265598)
#print(json.dumps(pp, ensure_ascii=False, indent=4))

#print('checking orders:')
#pp= ib_endpoints2.order_status()  
#print(json.dumps(pp, ensure_ascii=False, indent=4))

#print('\nsending stop order')
#ib_endpoints2.order_request( 265598, 'STP', 'SELL', 50, tgt_price=120 )  

#print('checking orders:')
#pp= ib_endpoints2.order_status()  
#print(json.dumps(pp, ensure_ascii=False, indent=4))

#ib_endpoints2.order_request( 265598, 'MKT', 'BUY', 100 )  
#ib_endpoints2.order_request( 265598, 'STP', 'SELL', 50, tgt_price=200 )  

#ib_endpoints2.market_connect( 265598 ) 
#market_init = ib_endpoints2.market_connect( 265598 , retry = 5)
#print(market_init)

##mm = ib_endpoints2.order_request( 265598, 'STP', 'SELL', 50, tgt_price=150 )  
##print(json.dumps(mm, ensure_ascii=False, indent=4))

#p = ib_endpoints2.status() 
#print(json.dumps(p, ensure_ascii=False, indent=4))

##UPRO =  61228752
##vv = ib_endpoints2.market_snapshot( 265598 )  
##vv = ib_endpoints2.market_snapshot( UPRO )  
##print(json.dumps(vv, ensure_ascii=False, indent=4))

## only can be called in production (live trading)
##p = ib_endpoints2.start_brokerage_session() 
##print(json.dumps(p, ensure_ascii=False, indent=4))

"""
jj = ib_endpoints2.get_subaccounts()
print('\nSub Accounts:')
print(json.dumps(jj, indent=4))

jj = ib_endpoints2.get_accounts()
print('\nAccounts:')
print(json.dumps(jj, indent=4))
"""

"""
UPRO =  61228752
AAPL = 265598
for sub in ["13","14","15","16","17"]:
    subaccount = f'DU90858{sub}'
    cur_pos = ib_endpoints2.current_position(UPRO, subaccount=subaccount)
    print(json.dumps(cur_pos, ensure_ascii=False, indent=4))

#p = ib_endpoints2.get_allocation_groups()
#print(json.dumps(p, ensure_ascii=False, indent=4))
"""

"""
order_info = ib_endpoints2.order_reply(reply_id='34d51a47-3307-44ec-81cf-06cb1a16ddd0',repeat=True)
print("-- order reply info--")
print(json.dumps(order_info, ensure_ascii=False, indent=4))
"""

"""
order_info = ib_endpoints2.order_request( 265598, 'MKT', 'BUY', 100 )  
print("-- order info--")
print(json.dumps(order_info, ensure_ascii=False, indent=4))
"""

##p = ib_endpoints2.tickle()  
##print(json.dumps(p, ensure_ascii=False, indent=4))

"""
IMPORTANT: replace the order_status() call within OrderMonitor.monitor_orders() with
           mock_order_status(), and make sure the snap_order*.txt are in current dir

order_monitor = ib_endpoints2.OrderMonitor()

i = 0
while i < 3:
    for fill in order_monitor.monitor_orders():
        print(fill)
    i += 1
"""
