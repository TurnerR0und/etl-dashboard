import pytest
from fastapi.testclient import TestClient
import os

# This fixture will be used by all tests in this module
@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    Provides a TestClient instance for the FastAPI app after setting up
    a dedicated test database environment.
    """
    # 1. Set the environment variable to point to a test-specific database
    # This must be done BEFORE importing the app
    os.environ["DATABASE_URL"] = "sqlite:///./test_house_prices.db"
    
    # 2. Now we can safely import the app.
    # The app's top-level code will run, including initialize_database(),
    # which will create and populate our test_house_prices.db
    from api import app
    
    # 3. Yield the TestClient for the tests to use
    with TestClient(app) as c:
        yield c
        
    # 4. Teardown: after all tests in the module run, remove the test database
    os.remove("./test_house_prices.db")

# --- API Endpoint Tests (These remain mostly the same) ---

def test_read_index(client: TestClient):
    """
    Tests the root endpoint ('/') to ensure it serves the frontend.
    """
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers['content-type']
    assert b"UK House Price Index Dashboard" in response.content

def test_get_regions_successful(client: TestClient):
    """
    Tests the /regions endpoint for a successful response.
    """
    response = client.get("/regions")
    assert response.status_code == 200
    
    data = response.json()
    assert "regions" in data
    assert isinstance(data["regions"], list)
    
    # The initialize_database() function should have populated the test DB
    assert len(data["regions"]) > 0
    assert "London" in data["regions"]

def test_get_data_for_region_successful(client: TestClient):
    """
    Tests fetching data for a valid, existing region.
    """
    region_name = "London"
    response = client.get(f"/data/{region_name}")
    assert response.status_code == 200
    
    data = response.json()
    assert "region" in data
    assert data["region"] == region_name
    assert "data" in data
    assert isinstance(data["data"], list)

    # Check that data was returned and has the correct structure
    assert len(data["data"]) > 0
    first_item = data["data"][0]
    assert "date" in first_item
    assert "average_price" in first_item
    assert "index" in first_item

def test_get_data_for_nonexistent_region(client: TestClient):
    """
    Tests fetching data for a region that does not exist.
    """
    region_name = "Atlantis" # A region we know is not in the dataset
    response = client.get(f"/data/{region_name}")
    assert response.status_code == 200
    
    data = response.json()
    assert data["region"] == region_name
    # The API should gracefully return an empty list for the data
    assert data["data"] == []