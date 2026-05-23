import mysql.connector

def test_connection(strategy_id):
    # Connect to the 'Operations' database
    connection = mysql.connector.connect(
        host="mysql_db",  # constainer MySQL server host named "mysql_db"
        user="root",  # Replace with your MySQL username
        password="tarzan001",  # Replace with your MySQL password
        database="Operations"
    )

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    print(f'alert: fetching new capital available for {strategy_id}')

    # Perform the join query
    ## FIX IT - add timestamp to AccountValue table - shows last update and MTM time.
    query = """
        SELECT sa.accountId, av.cash, av.timestamp
        FROM StrategyAccount AS sa
        JOIN AccountValue AS av ON sa.accountId = av.accountId
        WHERE sa.strategyId = %s
    """
    cursor.execute(query, (strategy_id,))

    # Fetch all the results
    results = cursor.fetchall()

    # Print the retrieved data
    if results:
        for row in results:
            account_id, cash, timestamp = row
            print(f"{strategy_id}: accountId: {account_id}, cash: {cash}, timestamp: {timestamp}")
    else:
        err = f"No accounts found for strategyId '{strategy_id}'."
        print(err)

if __name__ == "__main__":
    test_connection('Strategy2')
