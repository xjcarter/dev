import mysql.connector
import os
import json
from decimal import Decimal
from datetime import datetime
import argparse
import ib_endpoints2 as IB

#MYSQL_HOSTNAME = '97.107.130.78'
#MYSQL_PASSWORD = 'Eleph@ntTusk123$'
MYSQL_HOSTNAME = os.environ.get('MYSQL_HOSTNAME', 'localhost')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'tarzan001')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S')
        return super(DecimalEncoder, self).default(obj)

def get_available_equity(sub_account):
    data_dicts = sub_account['data']
    name = sub_account['name']

    """ sample sub_account dict=
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
    """

    for dd in data_dicts:
        if "AvailableEquity" in dd.values():
            return float(dd['value'])
    logger.warning(f'No \"AvailableEquity\" entry found for sub_account= {name}')
    return 0


def add_trading_account(new_account):

    dt = datetime.today().strftime("%Y-%m-%d")
    
    # Connect to the 'Operations' database
    # Connect to the 'Operations' database
    connection = mysql.connector.connect(
        #host="mysql_db",  # constainer MySQL server host named "mysql_db"
        host=MYSQL_HOSTNAME,  # constainer MySQL server host named "mysql_db"
        user="root",  # Replace with your MySQL username
        password=MYSQL_PASSWORD,  # Replace with your MySQL password
        database="Operations"
    )


    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    check = "SELECT * FROM AccountValue WHERE accountId = %s"
    cursor.execute(check, (new_account["accountId"],))

    # Fetch all the results
    results = cursor.fetchall()

    columns = "date accountId availableEquity allocType allocValue liveEquity timestamp" 
    if results:
        err = f'Error: accountId = {new_account["accountId"]} already exists.'
        print(err)
        for row in results:
            dd = dict(zip(columns.split(),row))
            print(f'{json.dumps(dd, indent=4, cls=DecimalEncoder)}')
        raise RuntimeError(err)


    query = "INSERT INTO AccountValue (date, accountId, availableEquity, \
                allocType, allocValue, liveEquity) \
            VALUES (%s, %s, %s, %s, %s, %s)"
    values = [dt] + list(new_account.values())
    cursor.execute(query, values)

    # Commit the changes
    connection.commit()

    # Close the cursor and the connection
    cursor.close()
    connection.close()
    print(f'Successfullly added account_id:\n{json.dumps(new_account, indent=4)}')



if __name__ == "__main__":
    parser =  argparse.ArgumentParser()
    parser.add_argument("--account_id", help="IB account id", required=True)
    u = parser.parse_args()

    ## NOTE ace.py is the manager of account postings after the account has been added
    ## every morning ace.py will pull available cash from IB and post that value to AccountValue

    jj = IB.get_subaccounts()

    """
    sample output=
    {
        "accounts": [
            {
                "data": [
                    {
                        "value": "1002258.71",
                        "key": "NetLiquidation"
                    },
                    {
                        "value": "1000398.49",
                        "key": "AvailableEquity"
                    }
                ],
                "name": "DU9085813"
            },
            {
                "data": [
                    {
                        "value": "1002258.71",
                        "key": "NetLiquidation"
                    },
                    {
                        "value": "1000398.49",
                        "key": "AvailableEquity"
                    }
                ],
                "name": "DU9085814"
            },
        ]
    }
    """

    sub_list= jj.get('accounts')
    for sub_account in sub_list:
        if u.account_id == sub_account["name"]:
            cash = get_available_equity(sub_account)
            new_account = dict(accountId=u.account_id, availableEquity=cash, allocType='passThru')
            new_account.update( dict(allocValue=cash, liveEquity=cash) )
            add_trading_account(new_account)
