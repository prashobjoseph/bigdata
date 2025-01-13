import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# Import functions from the script
from pyspark.sql.functions import col
from script_name import initialize_spark, load_data_from_postgres, transform_data, save_to_hive  # Replace 'script_name' with the actual name of your script file

class TestPySparkPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = SparkSession.builder.master("local[*]").appName("TestPySparkPipeline").enableHiveSupport().getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after testing."""
        cls.spark.stop()

    @patch("script_name.SparkSession")  # Mock SparkSession for `initialize_spark`
    def test_initialize_spark(self, mock_spark):
        """Test Spark session initialization."""
        mock_builder = mock_spark.builder.return_value
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_builder.getOrCreate.return_value = "mock_spark_session"

        result = initialize_spark()
        self.assertEqual(result, "mock_spark_session")

    @patch("script_name.SparkSession.read")
    def test_load_data_from_postgres(self, mock_read):
        """Test loading data from PostgreSQL."""
        # Create mock data
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("cc_num", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
            StructField("city_pop", IntegerType(), True),
            StructField("dob", StringType(), True),
            StructField("trans_date_trans_time", TimestampType(), True)
        ])
        mock_df = self.spark.createDataFrame([], schema)
        mock_read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df

        result = load_data_from_postgres(self.spark)
        self.assertEqual(result.schema, schema)

    def test_transform_data(self):
        """Test the data transformation logic."""
        # Create mock input data
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("cc_num", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
            StructField("city_pop", IntegerType(), True),
            StructField("dob", StringType(), True),
            StructField("trans_date_trans_time", TimestampType(), True)
        ])
        data = [
            ("1", None, "1234567890123456", "John", "Doe", 1000, "1980-05-15", "2023-01-01 12:00:00"),
            ("2", "shopping", "9876543210987654", "Jane", "Smith", 2000, "1990-08-25", "2023-01-02 14:00:00")
        ]
        input_df = self.spark.createDataFrame(data, schema)

        # Apply transformations
        result_df = transform_data(input_df)

        # Assert transformations
        self.assertIn("first_name", result_df.columns)
        self.assertIn("last_name", result_df.columns)
        self.assertIn("population", result_df.columns)
        self.assertIn("Age", result_df.columns)
        self.assertIn("Date", result_df.columns)
        self.assertNotIn("cc_num", result_df.columns)
        self.assertEqual(result_df.filter(col("category") == "travel").count(), 1)  # Check filled value
        self.assertEqual(result_df.filter(col("Age").isNotNull()).count(), 2)  # Check calculated age

    @patch("script_name.DataFrame.write")
    def test_save_to_hive(self, mock_write):
        """Test saving data to Hive."""
        # Create mock data
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("population", IntegerType(), True),
            StructField("dob", DateType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Date", DateType(), True)
        ])
        mock_df = self.spark.createDataFrame([], schema)

        # Call the function
        save_to_hive(mock_df)

        # Assert Hive write call
        mock_write.mode.assert_called_once_with("overwrite")
        mock_write.mode.return_value.saveAsTable.assert_called_once_with("bigdata_nov_2024.()".format(HIVE_TABLE_NAME))

if __name__ == "__main__":
    unittest.main()
