import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import OperationalError, DatabaseError

#Database Configuration (Your provided credentials)
DATABASE_TYPE = 'postgresql'
DB_DRIVER = 'psycopg2'
DB_USER = 'consultants'         # Replace with your PostgreSQL username
DB_PASSWORD = 'WelcomeItc%402022' # Replace with your PostgreSQL password, encode @ as %40
DB_HOST = '18.132.73.146'      # Corrected IP address
DB_PORT = '5432'               # Default PostgreSQL port
DB_NAME = 'testdb'             # Replace with your PostgreSQL database name
CSV_FILE_PATH = "C:/Users/prash/Downloads/customers-1000-dataset.csv"  # Replace with the path to your CSV file
TABLE_NAME = 'firsttablepr29'  # Replace with the desired table name

try:
    # Construct the database URL using the provided credentials
    #DATABASE_URL = f"{DATABASE_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    DATABASE_URL = 'postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb'


    # Create a SQLAlchemy engine for PostgreSQL
    engine = create_engine(DATABASE_URL)
    print("Database connection established successfully!")

    # Load the CSV into a Pandas DataFrame
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        print("CSV file loaded successfully!")
    except FileNotFoundError as e:
        print(f"Error: The file '{CSV_FILE_PATH}' was not found. Please check the path and try again.")
        raise

    # Load the DataFrame into the PostgreSQL table
    try:
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print(f"Data successfully loaded into the table '{TABLE_NAME}' in the PostgreSQL database.")
    except DatabaseError as e:
        print("Error: Failed to write the data to the PostgreSQL table. Please check the table schema and permissions.")
        raise

except OperationalError as e:
    print(f"Operational Error: Could not connect to the database. Please check the connection details.\n{e}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    if 'engine' in locals():
        # Dispose of the SQLAlchemy engine
        engine.dispose()
        print("Database connection closed.")

from flask import Flask, jsonify




# Flask Application Setup
app = Flask(__name__)

# Route to fetch data from the database and return it as JSON
@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    try:
        # Construct the database URL using the provided credentials
        #DATABASE_URL = f"{DATABASE_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        DATABASE_URL = 'postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb'
        # Create a SQLAlchemy engine for PostgreSQL
        engine = create_engine(DATABASE_URL)

        # SQL query to fetch all data from the table
        query = f"SELECT * FROM {TABLE_NAME};"

        # Execute the query and load the data into a Pandas DataFrame
        df = pd.read_sql(query, engine)

        # Close the database connection
        engine.dispose()

        # Convert the DataFrame to JSON format and return it
        return jsonify(df.to_dict(orient='records')), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run the Flask application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5039, debug=True)

#The API is accessible at the endpoint /fetch-data.
