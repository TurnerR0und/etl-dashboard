import pytest
from fastapi.testclient import TestClient
from api import app # Import your FastAPI app instance

# --- Test Fixtures ---

@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    Provides a TestClient instance for the FastAPI app.
    This fixture has a 'module' scope, meaning it will be created
    once per test module run.
    """
    with TestClient(app) as c:
        yield c

# --- API Endpoint Tests ---

def test_read_index(client: TestClient):
    """
    Tests the root endpoint ('/') to ensure it serves the frontend.
    """
    response = client.get("/")
    # Assert that the request was successful
    assert response.status_code == 200
    # Assert that the response content type is HTML
    assert "text/html" in response.headers['content-type']
    # Assert that some key text from your index.html is present
    assert b"UK House Price Index Dashboard" in response.content

def test_get_regions_successful(client: TestClient):
    """
    Tests the /regions endpoint for a successful response.
    """
    response = client.get("/regions")
    assert response.status_code == 200
    
    # Check the structure of the JSON response
    data = response.json()
    assert "regions" in data
    assert isinstance(data["regions"], list)
    
    # Check if a known region is in the list (if the test db is populated)
    # This might fail if the test environment doesn't have a prepopulated DB,
    # but it's a good example of a more specific test.
    if data["regions"]:
         assert "London" in data["regions"]

def test_get_data_for_region_successful(client: TestClient):
    """
    Tests fetching data for a valid, existing region.
    """
    region_name = "London"
    response = client.get(f"/data/{region_name}")
    assert response.status_code == 200
    
    # Check the structure of the JSON response
    data = response.json()
    assert "region" in data
    assert data["region"] == region_name
    assert "data" in data
    assert isinstance(data["data"], list)

    # If data is returned, check the structure of the first item
    if data["data"]:
        first_item = data["data"][0]
        assert "date" in first_item
        assert "average_price" in first_item
        assert "index" in first_item

def test_get_data_for_nonexistent_region(client: TestClient):
    """
    Tests fetching data for a region that does not exist.
    The current API will return an empty list, which is a valid response.
    """
    region_name = "Atlantis" # A region we know is not in the dataset
    response = client.get(f"/data/{region_name}")
    assert response.status_code == 200
    
    data = response.json()
    assert data["region"] == region_name
    # The API should gracefully return an empty list for the data
    assert data["data"] == []