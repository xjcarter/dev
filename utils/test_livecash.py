from datetime import datetime
import os
import pytz
import mysql.connector

MYSQL_HOSTNAME = os.environ.get('MYSQL_HOSTNAME', 'localhost')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'tarzan001')

def convert_timestamp( timestamp_utc ):
    timestamp_ny = timestamp_utc.astimezone(pytz.timezone('America/New_York'))
    return timestamp_ny

def test_live_cash_fetch(strategy_id):

    current_date = '2024-04-29' 

    # Connect to the 'Operations' database
    connection = mysql.connector.connect(
        host=MYSQL_HOSTNAME,  # Replace with your MySQL server host
        user="root",  # Replace with your MySQL username
        password=MYSQL_PASSWORD,  # Replace with your MySQL password
        database="Operations"
    )

    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

    print(f'alert: fetching new capital available for Strategy={strategy_id}')

    # Execute SQL statement to drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS liveCash")
    connection.commit()

    # Create temporary table of most recent allocation
    create_live_cash = """
        CREATE TEMPORARY TABLE liveCash AS
        SELECT t.date, t.accountId, t.liveEquity, t.timestamp FROM AccountValueNew AS t
        JOIN (SELECT accountId, date, MAX(timestamp) AS maxTs FROM AccountValueNew GROUP BY accountId, date) AS q
        ON t.accountId = q.accountId
        AND q.maxTs = t.timestamp
    """
    cursor.execute(create_live_cash)
    connection.commit()

    # Fetch liveCash allocations associated with strategyId and current date
    query = """
        SELECT c.accountId, c.liveEquity, c.timestamp FROM liveCash AS c
        JOIN StrategyAccount AS s
        ON s.accountId = c.accountId
        WHERE s.strategyId = %s AND c.date = %s
    """

    cursor.execute(query, [strategy_id, current_date])

    # Fetch all the results
    results = cursor.fetchall()

    # Print the retrieved data
    if results:
        for row in results:
            account_id, cash, ts = row
            timestamp = convert_timestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')
            print(f"{strategy_id}: accountId: {account_id}, cash: {cash}, timestamp: {timestamp}")
    else:
        err = f"No accounts found for strategyId '{strategy_id}'."
        print(err)
        raise RuntimeError(err)
      # Close the cursor and connection

    cursor.close()
    connection.close()


if __name__ == '__main__':
    test_live_cash_fetch('lex2')

