# In tests/test_api.py

import pytest
from fastapi.testclient import TestClient
import os
import asyncio

# This fixture will be used by all tests in this module
@pytest.fixture(scope="module")
@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    Provides a TestClient for the API after setting up a dedicated test database.
    Includes error handling to expose pipeline failures during test setup.
    """
    db_path = "./test_api_database.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    
    # --- SETUP: Run the data pipeline with explicit error catching ---
    try:
        from data_pipeline import main as run_pipeline
        log.info("Test setup: Running data pipeline to create test database...")
        asyncio.run(run_pipeline())
        log.info("Test setup: Data pipeline finished successfully.")
    except Exception as e:
        # If the pipeline fails for any reason, fail the test suite with a clear error
        pytest.fail(f"Data pipeline failed during test setup: {e}", pytrace=True)

    # Now that the DB is created, we can import the app
    from api import app
    
    with TestClient(app) as c:
        yield c
        
    # --- TEARDOWN: Clean up the test database ---
    os.remove(db_path)

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