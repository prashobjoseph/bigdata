import mysql.connector
from mysql.connector import Error

try:
    # Establishing the connection
    connection = mysql.connector.connect(
        host="localhost",         # Host (e.g., 'localhost')
        user="root",              # Your MySQL username
        password="7098",          # Your MySQL password
        database="trial"          # Your database name
    )

    # Check if the connection is successful
    if connection.is_connected():
        print("Connection to the database was successful!")
        
        # Create a cursor object using the connection
        cursor = connection.cursor()

        # Define your SQL query
        query = "SELECT * FROM Departments"

        try:
            # Execute the query
            cursor.execute(query)

            # Fetch all rows from the executed query
            rows = cursor.fetchall()

            # Print each row
            print("Query executed successfully. Results:")
            for row in rows:
                print(row)
        
        except Error as query_error:
            print(f"Error executing query: {query_error}")
        
        finally:
            # Close the cursor
            cursor.close()

except Error as connection_error:
    print(f"Error connecting to the database: {connection_error}")

finally:
    # Ensure the connection is closed
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Database connection closed.")
