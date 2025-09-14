# In tests/test_api.py

import pytest
from fastapi.testclient import TestClient
import os
import asyncio

# This fixture will be used by all tests in this module
@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    Provides a TestClient for the API after setting up and tearing down
    a dedicated test database.
    """
    os.environ["DATABASE_URL"] = "sqlite:///./test_database.db"
    
    # --- SETUP: Run the data pipeline to create the test DB ---
    # We directly import and run the pipeline's main function.
    # This is more reliable than using a subprocess.
    from data_pipeline import main as run_pipeline
    
    # Run the async main function from the pipeline
    asyncio.run(run_pipeline())
    
    # Now that the DB is created, we can import the app
    from api import app
    
    # Yield the TestClient for the tests to use
    with TestClient(app) as c:
        yield c
        
    # --- TEARDOWN: Clean up the test database after tests are done ---
    os.remove("./test_database.db")

# --- API Endpoint Tests (These require a small update for the new data) ---

def test_read_index(client: TestClient):
    """Tests the root endpoint ('/')."""
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers['content-type']

def test_get_regions_successful(client: TestClient):
    """Tests the /regions endpoint for a successful response."""
    response = client.get("/regions")
    assert response.status_code == 200
    data = response.json()
    assert "regions" in data
    assert isinstance(data["regions"], list)
    assert "London" in data["regions"]

def test_get_data_for_region_successful(client: TestClient):
    """Tests fetching data for a valid region, checking for new data fields."""
    response = client.get("/data/London")
    assert response.status_code == 200
    
    data = response.json()
    assert data["region"] == "London"
    assert "data" in data
    assert len(data["data"]) > 0

    # Check that our new affordability data is present in the response
    first_item = data["data"][0]
    assert "average_price" in first_item
    assert "average_annual_salary" in first_item
    assert "affordability_ratio" in first_item

def test_get_data_for_nonexistent_region(client: TestClient):
    """Tests fetching data for a region that does not exist."""
    response = client.get("/data/Atlantis")
    assert response.status_code == 200
    data = response.json()
    assert data["region"] == "Atlantis"
    assert data["data"] == [] # Should return an empty list