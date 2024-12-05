import unittest
from unittest.mock import patch, MagicMock
from flask import Flask
from app import app, engine  # Assuming your script is named `app.py`

class TestFlaskApp(unittest.TestCase):

    def setUp(self):
        # Set up a test client for the Flask app
        self.app = app.test_client()
        self.app.testing = True  # Enable testing mode for better error reporting

    @patch("app.pd.read_sql")  # Mock the `pd.read_sql` function
    def test_get_data_success(self, mock_read_sql):
        # Mock the data returned by read_sql
        mock_data = [
            {"id": 1, "name": "Test Bank", "balance": 1000},
            {"id": 2, "name": "Sample Bank", "balance": 2000}
        ]
        mock_read_sql.return_value = pd.DataFrame(mock_data)

        # Call the endpoint
        response = self.app.get('/')

        # Assertions
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, mock_data)

    @patch("app.pd.read_sql")  # Mock the `pd.read_sql` function
    def test_get_data_failure(self, mock_read_sql):
        # Simulate an exception when reading from the database
        mock_read_sql.side_effect = Exception("Database error")

        # Call the endpoint
        response = self.app.get('/')

        # Assertions
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json, {"error": "Unable to fetch data from database"})

if __name__ == "__main__":
    unittest.main()
