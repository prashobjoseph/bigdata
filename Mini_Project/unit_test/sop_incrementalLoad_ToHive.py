import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col

class TestMiniProj(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = SparkSession.builder.master("local[*]").appName("TestMiniProj").enableHiveSupport().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after testing."""
        cls.spark.stop()

    @patch("pyspark.sql.SparkSession.sql")
    @patch("pyspark.sql.DataFrame.write")
    @patch("pyspark.sql.DataFrame.count")
    @patch("pyspark.sql.DataFrame.withColumnRenamed")
    @patch("pyspark.sql.DataFrame.withColumn")
    @patch("pyspark.sql.DataFrame.fillna")
    @patch("pyspark.sql.DataFrame.drop")
    @patch("pyspark.sql.DataFrame.collect")
    def test_mini_proj(self, mock_collect, mock_drop, mock_fillna, mock_withColumn, mock_withColumnRenamed, mock_count, mock_write, mock_sql):
        """Test the entire process of fetching, transforming, and saving data."""

        PG_TABLE_NAME = "sop_credit_transaction_test1"
        HIVE_TABLE_NAME = "sop_credit_trans_test1"

        # Mock max transaction date from Hive
        max_time = "2023-01-01 00:00:00"
        mock_collect.return_value = [{"max_time": max_time}]

        # Mock new data from PostgreSQL
        schema = StructType([
            StructField("trans_date_trans_time", TimestampType(), True),
            StructField("category", StringType(), True),
            StructField("cc_num", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
            StructField("city_pop", IntegerType(), True),
            StructField("dob", StringType(), True)
        ])
        mock_data = self.spark.createDataFrame([
            ("2023-01-02 12:00:00", None, "1234567890123456", "John", "Doe", 1000, "1980-05-15"),
            ("2023-01-03 14:00:00", None, "9876543210987654", "Jane", "Smith", 2000, "1990-08-25")
        ], schema)
        
        # Mock Spark SQL query for Hive table
        mock_sql.return_value = mock_data

        # Mock transformations
        mock_fillna.return_value = mock_data.fillna({"category": "travel"})
        mock_drop.return_value = mock_data.drop("cc_num")
        mock_withColumnRenamed.return_value = (
            mock_data.withColumnRenamed("first", "first_name")
                     .withColumnRenamed("last", "last_name")
                     .withColumnRenamed("city_pop", "population")
        )
        mock_withColumn.return_value = (
            mock_data.withColumn("dob", col("dob").cast(DateType()))
                     .withColumn("Age", (2025 - 1980))  # Approximation for simplicity
                     .withColumn("Date", col("trans_date_trans_time").cast(DateType()))
        )
        mock_count.return_value = 2

        # Mock the write operation
        mock_write.mode.return_value = MagicMock()

        # Execute the tested logic
        try:
            # Fetch max date
            max_time_df = self.spark.sql(f"SELECT max(trans_date_trans_time) as max_time FROM bigdata_nov_2024.{HIVE_TABLE_NAME}")
            max_time = max_time_df.collect()[0]["max_time"]

            # Formulate query
            query = f"SELECT * FROM {PG_TABLE_NAME} WHERE trans_date_trans_time > '{max_time}'"
            
            # Fetch new data from PostgreSQL
            new_data = self.spark.read.format("jdbc") \
                .option("url", "jdbc:postgresql://mock-db:5432/testdb") \
                .option("driver", "org.postgresql.Driver") \
                .option("user", "mock_user") \
                .option("password", "mock_password") \
                .option("query", query).load()

            # Perform transformations
            new_data = new_data.fillna({"category": "travel"})
            new_data = new_data.drop("cc_num")
            new_data = new_data.withColumnRenamed("first", "first_name") \
                               .withColumnRenamed("last", "last_name") \
                               .withColumnRenamed("city_pop", "population")
            new_data = new_data.withColumn("dob", col("dob").cast(DateType())) \
                               .withColumn("Age", (2025 - 1980)) \
                               .withColumn("Date", col("trans_date_trans_time").cast(DateType()))
            
            # Write to Hive if there is new data
            if new_data.count() > 0:
                new_data.write.mode("append").saveAsTable(f"bigdata_nov_2024.{HIVE_TABLE_NAME}")
            
            print("Unit test passed.")

        except Exception as e:
            self.fail(f"Test failed due to unexpected exception: {str(e)}")

if __name__ == "__main__":
    unittest.main()
