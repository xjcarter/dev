import mysql.connector

# Connect to the MySQL server
connection = mysql.connector.connect(
    host="mysql_db",  # Replace with your MySQL server host
    user="root",  # Replace with your MySQL username
    password="Eleph@ntTusk123$"  # Replace with your MySQL password
)

# Create the 'Operations' database
cursor = connection.cursor()
cursor.execute("CREATE DATABASE Operations")
print("Database 'Operations' created successfully.")

# Switch to the 'Operations' database
cursor.execute("USE Operations")

# Create the 'StrategyAccount' table
cursor.execute("CREATE TABLE StrategyAccount (strategyId VARCHAR(255) PRIMARY KEY, accountId INT)")
print("Table 'StrategyAccount' created successfully.")

# Create the 'AccountValue' table
cursor.execute("CREATE TABLE AccountValue (accountId INT PRIMARY KEY, cash DECIMAL(20, 2), equity DECIMAL(20, 2), timestamp VARCHAR(255))")
print("Table 'AccountValue' created successfully.")

# Close the cursor and connection
cursor.close()
connection.close()
