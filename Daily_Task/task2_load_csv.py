#importing libraries
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

try:
    # Step 1: Load the CSV file
    file_path = "C:/Users/prash/Downloads/customers-1000-dataset.csv"
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        raise Exception(f"File not found at: {file_path}. Please check the file path.")
    except pd.errors.EmptyDataError:
        raise Exception(f"File is empty or cannot be read: {file_path}.")
    except Exception as e:
        raise Exception(f"An error occurred while reading the file: {str(e)}")
    
    # Step 2: Transform the Data
    try:
        # Transformation 1: Rename columns to uppercase
        df.columns = df.columns.str.upper()

        # Transformation 2: Fill missing values in the 'COUNTRY' column
        if 'COUNTRY' not in df.columns:
            raise KeyError("Column 'COUNTRY' is missing in the dataset.")
        df['COUNTRY'] = df['COUNTRY'].fillna(df['COUNTRY'].mode()[0])

        # Transformation 3: Create a 'FULLNAME' column
        if 'FIRST NAME' not in df.columns or 'LAST NAME' not in df.columns:
            raise KeyError("Columns 'FIRST NAME' or 'LAST NAME' are missing in the dataset.")
        df['FULLNAME'] = df['FIRST NAME'] + " " + df['LAST NAME']

        # Transformation 4: Add a 'SALARY' column with random values
        df['SALARY'] = np.random.randint(30000, 100000, size=len(df))

        # Transformation 5: Filter rows where 'COUNTRY' equals "China"
        df = df[df['COUNTRY'] == "China"]
        if df.empty:
            raise ValueError("No rows meet the filter condition: COUNTRY = 'China'.")
    except KeyError as ke:
        raise Exception(f"Data transformation error: {str(ke)}")
    except Exception as e:
        raise Exception(f"An error occurred during data transformation: {str(e)}")
    
    # Step 3: Connect to the Database
    DATABASE_TYPE = 'mysql'  # 'mysql', 'sqlite', etc.
    DB_DRIVER = 'pymysql'    # Database driver
    DB_USER = 'root'
    DB_PASSWORD = '7098'
    DB_HOST = 'localhost'
    DB_PORT = '3306'
    DB_NAME = 'trial'

    DATABASE_URL = f"{DATABASE_TYPE}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    try:
        engine = create_engine(DATABASE_URL)
    except SQLAlchemyError as sqle:
        raise Exception(f"Database connection error: {str(sqle)}")
    except Exception as e:
        raise Exception(f"An error occurred while creating the database connection: {str(e)}")
    
    # Step 4: Load Transformed Data into the Database
    table_name = "transformed_data"  # Desired table name in the database
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)  # Overwrites table if exists
        print(f"\nData has been successfully loaded into the table '{table_name}' in the database.")
    except SQLAlchemyError as sqle:
        raise Exception(f"An error occurred while writing data to the database: {str(sqle)}")
    except Exception as e:
        raise Exception(f"An error occurred during the data loading process: {str(e)}")

except Exception as e:
    # General exception handling
    print(f"An error occurred: {str(e)}")
