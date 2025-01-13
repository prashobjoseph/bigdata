from pyspark.sql import *
from pyspark.sql.functions import *

PG_TABLE_NAME= "sop_credit_transaction_test1"
HIVE_TABLE_NAME="sop_credit_trans_test1"

try:
    # Initialize SparkSession with Hive support
    spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

    # Fetch the maximum trans_date_trans_time from the Hive table
    max_time_df = spark.sql("SELECT max(trans_date_trans_time) as max_time FROM bigdata_nov_2024.{}".format(HIVE_TABLE_NAME))
    max_time = max_time_df.collect()[0]["max_time"]
    print("Maximum transaction date is {}".format(max_time))
    # Ensure max_time is not None; if None, initialize to a very old date
    if max_time is None:
        max_time = "1900-01-01 00:00:00"
    

    # Formulate the query to fetch new data from PostgreSQL
    query = "SELECT * FROM {} WHERE trans_date_trans_time > '{}'".format(PG_TABLE_NAME,max_time)

    print(query)

    # Read data from PostgreSQL using the query
    new_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()
    #new_data.show()
    # Check if there is new data to append
    if new_data.count() == 0:
        print("No new records to append.")
    else:
        # Transformation 1: Fill empty 'category' with "travel"
        new_data = new_data.fillna({"category": "travel"})

        # Transformation 2: Drop 'cc_num' column
        new_data = new_data.drop("cc_num")

        # Transformation 3: Rename columns
        new_data = new_data.withColumnRenamed("first", "first_name").withColumnRenamed("last", "last_name").withColumnRenamed("city_pop", "population")

        # Transformation 4: Add 'Age' column
        from pyspark.sql.types import DateType

        new_data = new_data.withColumn("dob", col("dob").cast(DateType()))

        #new_data = new_data.withColumn("dob", col("dob").cast(DataType()))  # Ensure it's a date type
        new_data = new_data.withColumn("Age", datediff(current_date(), col("dob")) / 365)  # Approximate age

        # Extract the date and create a new column
        new_data = new_data.withColumn("Date", to_date(col("trans_date_trans_time")))

        # Transformation 6: Rename the first column to "id"
        first_column = new_data.columns[0]  # Get the name of the first column
        new_data = new_data.withColumnRenamed(first_column, "id")  # Rename the first column to "id"

        new_data.show()
        # Append the new data to the Hive table
        new_data.write.mode("append").saveAsTable("bigdata_nov_2024.{}".format(HIVE_TABLE_NAME))
        print("Successfully loaded new records to Hive.")

    

except Exception as e:
    # Print the error message
    print("Error occurred:", str(e))