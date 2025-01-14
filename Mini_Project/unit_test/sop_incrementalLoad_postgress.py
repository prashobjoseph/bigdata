import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import pandas as pd

class TestNextBatchLoad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for testing."""
        cls.spark = SparkSession.builder.master("local[*]").appName("TestNextBatchLoad").getOrCreate()

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

    @patch("pyspark.sql.DataFrame.write")
    @patch("pyspark.sql.DataFrame.filter")
    @patch("pyspark.sql.DataFrame.select")
    @patch("pyspark.sql.DataFrame.orderBy")
    @patch("pyspark.sql.SparkSession.read")
    def test_next_batch_load(self, mock_read, mock_orderBy, mock_select, mock_filter, mock_write):
        """Test the entire next batch load process."""
        # Mock file path and database connection properties
        file_path = "C:/Users/prash/Downloads/fraudTest.csv"
        database_url = "jdbc:postgresql://mock-db:5432/testdb"
        db_properties = {"user": "mock_user", "password": "mock_password", "driver": "org.postgresql.Driver"}
        table_name = "sop_credit_transaction_test1"

        # Mock Spark DataFrame for CSV data
        csv_data = self.spark.createDataFrame(
            [("2023-01-01 00:00:00", 100.0), ("2023-03-01 00:00:00", 200.0)],
            ["trans_date_trans_time", "amount"]
        )
        mock_read.csv.return_value = csv_data

        # Mock Spark DataFrame for existing database data
        db_data = self.spark.createDataFrame(
            [("2023-01-01 00:00:00",)],
            ["trans_date_trans_time"]
        )
        mock_read.jdbc.return_value = db_data

        # Mock DataFrame operations
        mock_orderBy.return_value = csv_data
        max_date_in_db = "2023-01-01 00:00:00"
        mock_select.return_value.first.return_value = (max_date_in_db,)

        # Simulate date range filtering
        start_date = pd.Timestamp(max_date_in_db)
        end_date = start_date + pd.Timedelta(days=60)
        filtered_data = csv_data.filter((col('trans_date_trans_time') > str(start_date)) & 
                                        (col('trans_date_trans_time') <= str(end_date)))
        mock_filter.return_value = filtered_data

        # Simulate file existence check
        with patch("os.path.exists", return_value=True):
            self.assertTrue(os.path.exists(file_path))

            # Order by transaction date
            ordered_df = csv_data.orderBy(col("trans_date_trans_time"))
            mock_orderBy.return_value = ordered_df

            # Verify max_date_in_db calculation
            self.assertEqual(mock_select.return_value.first()[0], max_date_in_db)

            # Verify filtering for the next 60 days
            next_batch = ordered_df.filter((col("trans_date_trans_time") > str(start_date)) & 
                                           (col("trans_date_trans_time") <= str(end_date)))
            mock_filter.return_value = next_batch

            # Write to the database
            next_batch.write.jdbc(url=database_url, table=table_name, mode="append", properties=db_properties)
            mock_write.jdbc.assert_called_once_with(
                url=database_url,
                table=table_name,
                mode="append",
                properties=db_properties
            )

    def test_date_range_filtering(self):
        """Test filtering data for a specific date range."""
        # Create mock data
        data = [
            ("2023-01-01 00:00:00", 100.0),
            ("2023-01-15 00:00:00", 150.0),
            ("2023-03-01 00:00:00", 200.0),
        ]
        columns = ["trans_date_trans_time", "amount"]
        df = self.spark.createDataFrame(data, columns)

        # Define date range
        start_date = pd.Timestamp("2023-01-01 00:00:00")
        end_date = start_date + pd.Timedelta(days=60)

        # Filter data
        filtered_df = df.filter((col("trans_date_trans_time") > str(start_date)) & 
                                (col("trans_date_trans_time") <= str(end_date)))

        # Verify the filtered rows
        filtered_dates = [row["trans_date_trans_time"] for row in filtered_df.collect()]
        self.assertEqual(filtered_dates, ["2023-01-15 00:00:00", "2023-03-01 00:00:00"])


if __name__ == "__main__":
    unittest.main()
