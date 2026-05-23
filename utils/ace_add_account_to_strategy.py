import mysql.connector
import argparse
import os

MYSQL_HOSTNAME = os.environ.get('MYSQL_HOSTNAME', 'localhost')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'tarzan001')

def add_account_to_strategy(account_id, strategy_id):
    # Connect to the 'Operations' database
    connection = mysql.connector.connect(
        #host="mysql_db",  # constainer MySQL server host named "mysql_db"
        host=MYSQL_HOSTNAME,  # constainer MySQL server host named "mysql_db"
        user="root",  # Replace with your MySQL username
        password=MYSQL_PASSWORD,  # Replace with your MySQL password
        database="Operations"
    )

    #MYSQL_PASSWORD Create a cursor to execute SQL queries
    cursor = connection.cursor()

    # Use a parameterized query to insert data
    query = "INSERT INTO StrategyAccount (strategyId, accountId) VALUES (%s, %s)"

    # Loop through the data and execute the query for each tuple
    cursor.execute(query, (strategy_id, account_id))

    connection.commit()

    # Close the cursor and the connection
    cursor.close()
    connection.close()
    print(f"Successfully attached strategy_id: '{strategy_id}' to account: {account_id}")

if __name__ == "__main__":
    parser =  argparse.ArgumentParser()
    parser.add_argument("--account_id", help="cash account id", required=True)
    parser.add_argument("--strategy_id", help="strategy id", required=True)
    u = parser.parse_args()

    add_account_to_strategy(u.account_id, u.strategy_id)
