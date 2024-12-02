import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import OperationalError, DatabaseError

# Database Configuration (Your provided credentials)
DATABASE_TYPE = 'postgresql'
DB_DRIVER = 'psycopg2'
DB_USER = 'consultants'         # Replace with your PostgreSQL username
DB_PASSWORD = 'WelcomeItc%402022' # Replace with your PostgreSQL password, encode @ as %40
DB_HOST = '18.132.73.146'      # Corrected IP address
DB_PORT = '5432'               # Default PostgreSQL port
DB_NAME = 'testdb'             # Replace with your PostgreSQL database name
CSV_FILE_PATH = "C:/Users/prash/Downloads/customers-1000-dataset.csv"  # Replace with the path to your CSV file
TABLE_NAME = 'firsttable'  # Replace with the desired table name

try:
    # Construct the database URL using the provided credentials
    DATABASE_URL = f"{DATABASE_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


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
