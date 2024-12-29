from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit
import os
import pandas as pd

try:
    # Initialize Spark session
    spark = SparkSession.builder.appName("Fraud Detection - Next Batch Load").config("spark.jars", "/C:/Users/prash/Downloads/postgresql-42.7.4.jar").getOrCreate()
    print("Spark session created successfully.")

    # File path for CSV
    file_path = "C:/Users/prash/Downloads/fraudTest.csv"
    

    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError("The file at path {} does not exist.".format(file_path))

    # Read CSV with parsing dates
    df = spark.read.csv(file_path, header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss")
    print("CSV file loaded successfully.")

    # Ensure the data is sorted by date
    df = df.orderBy(col('trans_date_trans_time'))
    print("Data sorted by transaction date.")

    # Define database connection properties
    database_url = "jdbc:postgresql://18.132.73.146:5432/testdb"
    db_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver"
    }

    # Read the max transaction date from the database
    existing_data = spark.read \
        .jdbc(url=database_url,table="sop_credit_transaction", properties=db_properties)

    max_date_in_db = existing_data.select(spark_max("trans_date_trans_time")).first()[0]
    print("Max transaction date in database: {}".format(max_date_in_db))

    # Calculate the next 60 days range
    start_date = max_date_in_db
    end_date = start_date + pd.Timedelta(days=60)

    # Filter data for the next 60 days
    next_batch_data = df.filter((col('trans_date_trans_time') > lit(start_date)) & 
                                (col('trans_date_trans_time') <= lit(end_date)))
    print("Next batch data rows (60 days): {} rows.".format(next_batch_data.count()))

    # Write the next batch data to the database
    next_batch_data.write.jdbc(url=database_url, table="sop_credit_transaction", mode="append", properties=db_properties)
    print("Next batch data written to the database successfully.")

except FileNotFoundError as fnf_error:
    print("File not found: {}".format(fnf_error))
except Exception as e:
    print("An error occurred: {}".format(e))
