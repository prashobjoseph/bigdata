import pytest
from API_creation import app  # Import your Flask app

@pytest.fixture
def client():
    """Set up the test client for the Flask app."""
    app.config['TESTING'] = True  # Enable testing mode
    with app.test_client() as client:
        yield client

def test_get_data(client):
    """Test the GET request for the '/' endpoint."""
    response = client.get('/')  # Make a GET request to the '/' route
    assert response.status_code == 200  # Check if the response status code is 200 (OK)
    data = response.get_json()  # Parse the JSON response
    assert isinstance(data, list)  # Check if the response is a list
    assert len(data) > 0  # Ensure there's at least one record
    assert "marital" in data[0]  # Replace '' with an actual field from your table

    
