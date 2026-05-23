import ib_endpoints2 as IB
import json

def get_positions(accounts, symbol_id):
    positions = {}
    for account in accounts:
        cur_pos = IB.current_position(symbol_id, subaccount=account)
        positions[account] = cur_pos.get("position")
    return positions

jj = IB.get_subaccounts()
print('\nSub Accounts:')
print(json.dumps(jj, indent=4))

sub_list= jj.get('accounts')
account_list = []
for account_dict in sub_list:
    account_list.append(account_dict.get("name"))

UPRO = 61228752
v = get_positions(account_list, UPRO)
print('\nPositions:')
print(json.dumps(v, indent=4))

"""
sample output=
Sub Accounts:
{
    "accounts": [
        {
            "data": [
                {
                    "value": "1003007.20",
                    "key": "NetLiquidation"
                },
                {
                    "value": "999951.45",
                    "key": "AvailableEquity"
                }
            ],
            "name": "DU9085813"
        },
        {
            "data": [
                {
                    "value": "1002872.68",
                    "key": "NetLiquidation"
                },
                {
                    "value": "999816.92",
                    "key": "AvailableEquity"
                }
            ],
            "name": "DU9085814"
        },
        {
            "data": [
                {
                    "value": "1003188.23",
                    "key": "NetLiquidation"
                },
                {
                    "value": "1000132.29",
                    "key": "AvailableEquity"
                }
            ],
            "name": "DU9085815"
        },
        {
            "data": [
                {
                    "value": "1003216.30",
                    "key": "NetLiquidation"
                },
                {
                    "value": "1000160.34",
                    "key": "AvailableEquity"
                }
            ],
            "name": "DU9085816"
        },
        {
            "data": [
                {
                    "value": "1003028.72",
                    "key": "NetLiquidation"
                },
                {
                    "value": "999972.86",
                    "key": "AvailableEquity"
                }
            ],
            "name": "DU9085817"
        }
    ]
}

Positions:
{
    "DU9085813": 118.0,
    "DU9085814": 190.0,
    "DU9085815": 9.0,
    "DU9085816": 47.0,
    "DU9085817": 317.0
}
"""
