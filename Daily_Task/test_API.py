import unittest
from API_creation import app, df
from unittest.mock import patch
from flask import Flask, jsonify

#Testing the API Endpoint
class TestGetDataEndpoint(unittest.TestCase):
    def setUp(self):
        # Sets up the Flask test client
        self.app = app.test_client()
        self.app.testing = True

    def test_get_data_success(self):
        # Test to verify /data endpoint returns correct data
        response = self.app.get('/data')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, df)


#Simulate data fetching mock       
class TestDatabaseConnection(unittest.TestCase):
    @patch('api2.df', new=[
        {
            "age": 30,
            "balance": 309,
            "campaign": 2,
            "contact": "unknown",
            "day": 7,
            "default": "no",
            "deposit": "yes",
            "duration": 1574,
            "education": "secondary",
            "housing": "yes",
            "id": 12,
            "job": "blue-collar",
            "loan": "no",
            "marital": "married",
            "month": "may",
            "pdays": -1,
            "poutcome": "unknown",
            "previous": 0
        }
    ])
    def test_get_data_mocked(self):  # No additional argument needed
        with app.test_client() as client:
            response = client.get('/data')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json, [
                {
                    "age": 30,
                    "balance": 309,
                    "campaign": 2,
                    "contact": "unknown",
                    "day": 7,
                    "default": "no",
                    "deposit": "yes",
                    "duration": 1574,
                    "education": "secondary",
                    "housing": "yes",
                    "id": 12,
                    "job": "blue-collar",
                    "loan": "no",
                    "marital": "married",
                    "month": "may",
                    "pdays": -1,
                    "poutcome": "unknown",
                    "previous": 0
                }
            ])


#ErrorHandling
class TestErrorHandling(unittest.TestCase):
    @patch('api2.df', None)  
    def test_get_data_error(self):  
        with app.test_client() as client:
            response = client.get('/data')
            self.assertEqual(response.status_code, 500)
            self.assertEqual(response.json, {"error": "Unable to fetch data from database"})




if __name__ == "__main__":
    unittest.main()