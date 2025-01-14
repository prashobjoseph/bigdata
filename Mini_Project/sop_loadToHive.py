from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, to_date
from pyspark.sql.types import DateType

PG_TABLE_NAME= "sop_credit_transaction_test2"
HIVE_TABLE_NAME="sop_credit_trans_test2"

def initialize_spark():
    """Initialize the Spark session."""
    return SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

def load_data_from_postgres(spark):
    """Load data from PostgreSQL."""
    return spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", PG_TABLE_NAME).option("user", "consultants").option("password", "WelcomeItc@2022").load()
    
def transform_data(df):
    """Apply transformations to the dataframe."""
    # Transformation 1: Fill empty 'category' with 'travel'
    df = df.fillna({"category": "travel"})
    
    # Transformation 2: Drop 'cc_num' column
    df = df.drop("cc_num")
    
    # Transformation 3: Rename columns
    df = df.withColumnRenamed("first", "first_name").withColumnRenamed("last", "last_name").withColumnRenamed("city_pop", "population")
    
    # Transformation 4: Add 'Age' column (using 'dob')
    if "dob" in df.columns:
        df = df.withColumn("dob", col("dob").cast(DateType()))  # Ensure 'dob' is DateType
        df = df.withColumn("Age", (datediff(current_date(), col("dob")) / 365).cast("int"))  # Calculate approximate age
    
    # Transformation 5: Extract the date and create a new column
    if "trans_date_trans_time" in df.columns:
        df = df.withColumn("Date", to_date(col("trans_date_trans_time")))  # Assuming it's in timestamp format

    # Transformation 6: Rename the first column to "id"
    first_column = df.columns[0]  # Get the name of the first column
    df = df.withColumnRenamed(first_column, "id")  # Rename the first column to "id"
    
    return df

def save_to_hive(df):
    """Write the transformed dataframe to Hive."""
    df.write.mode("overwrite").saveAsTable("bigdata_nov_2024.()".format(HIVE_TABLE_NAME))
    print("Successfully Loaded to Hive")


if __name__ == "__main__":
    # Initialize Spark
    spark = initialize_spark()
    
    # Load data from PostgreSQL
    raw_df = load_data_from_postgres(spark)
    
    # Transform the data
    transformed_df = transform_data(raw_df)
    
    # Save the transformed data to Hive
    save_to_hive(transformed_df)

#cd Pyspark
#spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar sop_loadToHive.py