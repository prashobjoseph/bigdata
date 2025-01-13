import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import pandas as pd

class TestFraudDetection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = SparkSession.builder.master("local[*]").appName("TestFraudDetection").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after testing."""
        cls.spark.stop()

    @patch("os.path.exists")
    def test_file_existence(self, mock_exists):
        """Test file existence handling."""
        file_path = "C:/Users/prash/Downloads/fraudTest.csv"

        # Test when file exists
        mock_exists.return_value = True
        self.assertTrue(os.path.exists(file_path))

        # Test when file does not exist
        mock_exists.return_value = False
        self.assertFalse(os.path.exists(file_path))

    def test_csv_loading_and_sorting(self):
        """Test CSV loading and sorting by transaction date."""
        # Create mock data
        data = [("2023-01-01 00:00:00", 100.0), ("2023-01-02 00:00:00", 200.0)]
        columns = ["trans_date_trans_time", "amount"]
        df = self.spark.createDataFrame(data, columns)

        # Simulate sorting operation
        sorted_df = df.orderBy(col("trans_date_trans_time"))

        # Verify the data is sorted correctly
        sorted_dates = [row["trans_date_trans_time"] for row in sorted_df.collect()]
        self.assertEqual(sorted_dates, ["2023-01-01 00:00:00", "2023-01-02 00:00:00"])

    def test_date_range_filtering(self):
        """Test filtering data for the first 100 days."""
        # Create mock data
        data = [
            ("2023-01-01 00:00:00", 100.0),
            ("2023-03-01 00:00:00", 200.0),
            ("2023-06-01 00:00:00", 300.0),
        ]
        columns = ["trans_date_trans_time", "amount"]
        df = self.spark.createDataFrame(data, columns)

        # Define the start_date and end_date
        start_date = pd.Timestamp("2023-01-01 00:00:00")
        end_date = start_date + pd.Timedelta(days=100)

        # Filter the data
        filtered_df = df.filter((col("trans_date_trans_time") >= str(start_date)) & 
                                (col("trans_date_trans_time") < str(end_date)))

        # Verify only the expected rows are present
        filtered_dates = [row["trans_date_trans_time"] for row in filtered_df.collect()]
        self.assertEqual(filtered_dates, ["2023-01-01 00:00:00", "2023-03-01 00:00:00"])

    @patch("pyspark.sql.DataFrame.write")
    def test_database_write(self, mock_write):
        """Test writing filtered data to the database."""
        # Create mock data
        data = [("2023-01-01 00:00:00", 100.0)]
        columns = ["trans_date_trans_time", "amount"]
        df = self.spark.createDataFrame(data, columns)

        # Mock the `write.jdbc` behavior
        mock_write.jdbc = MagicMock()

        # Define mock database properties
        database_url = "jdbc:postgresql://mock-db:5432/testdb"
        db_properties = {
            "user": "mock_user",
            "password": "mock_password",
            "driver": "org.postgresql.Driver"
        }

        # Simulate the write operation
        df.write.jdbc(url=database_url, table="mock_table", mode="overwrite", properties=db_properties)

        # Verify the write was called with the correct parameters
        mock_write.jdbc.assert_called_once_with(
            url=database_url,
            table="mock_table",
            mode="overwrite",
            properties=db_properties
        )

    @patch("os.path.exists")
    def test_pipeline_integration(self, mock_exists):
        """Test the entire pipeline integration."""
        # Mock file existence
        mock_exists.return_value = True

        # Create mock data
        data = [("2023-01-01 00:00:00", 100.0), ("2023-01-02 00:00:00", 200.0)]
        columns = ["trans_date_trans_time", "amount"]
        df = self.spark.createDataFrame(data, columns)

        with patch.object(SparkSession, "read") as mock_read:
            mock_read.csv.return_value = df

            # Verify file existence and load data
            file_path = "mock_path.csv"
            self.assertTrue(os.path.exists(file_path))
            loaded_df = mock_read.csv(file_path, header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss")

            # Check sorting and filtering behavior
            sorted_df = loaded_df.orderBy(col("trans_date_trans_time"))
            start_date = pd.Timestamp("2023-01-01 00:00:00")
            end_date = start_date + pd.Timedelta(days=100)
            filtered_df = sorted_df.filter((col("trans_date_trans_time") >= str(start_date)) & 
                                           (col("trans_date_trans_time") < str(end_date)))

            # Verify filtered row count
            self.assertEqual(filtered_df.count(), 2)

if __name__ == "__main__":
    unittest.main()
