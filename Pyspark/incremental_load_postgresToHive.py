from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Create a SparkSession with Hive support
spark = SparkSession.builder \
    .master("local") \
    .appName("PrashobIncrementalLoad") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Get the maximum ID from the Hive table
# Check if the Hive table exists
try:
    max_timestamp = spark.sql("SELECT MAX(last_updated) FROM bigdata_nov_2024.prashtest").collect()[0][0]
    if max_timestamp is None:  # If the table is empty
        max_timestamp = 0
except:
    max_timestamp = 0  # If the table does not exist

#print(f"Max timestamp in Hive: {max_timestamp}")
print("Max timestamp in Hive: {}".format(max_timestamp))


# Step 3: Construct the query to fetch only new records from PostgreSQL
#query = f'SELECT * FROM prashtest WHERE last_updated > {max_timestamp}'
query = "SELECT * FROM prashtest WHERE last_updated > '{}'".format(max_timestamp)



# Step 4: Read the new data from PostgreSQL
#new_data = spark.read.format("jdbc") \
  #  .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb") \
  #  .option("driver", "org.postgresql.Driver") \
   # .option("user", "consultants") \
   # .option("password", "WelcomeItc@2022") \
   # .option("query", query) \
   # .load()

new_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "prashtest").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()

new_data.printSchema()
new_data.show(10)

# Step 5: Append the new data to the Hive table
if new_data.count() > 0:  # Only write if there are new records
    new_data.write.mode("append").saveAsTable("bigdata_nov_2024.prashtest")
    print("Successfully appended new data to Hive.")
else:
    print("No new data to load.")

# End of the script

spark.stop()
