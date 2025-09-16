import pytest
from fastapi.testclient import TestClient
import os
from logger_config import log
import pandas as pd
import io

# --- Test Data Fixtures ---
# These provide consistent, local data for our tests, removing the need for internet access.

@pytest.fixture(scope="module")
def raw_hpi_content() -> bytes:
    """Provides sample raw HPI data as bytes, simulating a file download."""
    csv_data = """Date,RegionName,AveragePrice,Index
01/01/2025,London,500000,120.5
01/02/2025,North West,200000,110.2
"""
    return csv_data.encode('utf-8')

@pytest.fixture(scope="module")
def raw_salary_content() -> bytes:
    """Provides sample raw Salary data as bytes, simulating an Excel file download."""
    data = {
        'Region': ['London', 'North West'],
        '2025': [1000, 800] # Weekly pay
    }
    df = pd.DataFrame(data)
    output = io.BytesIO()
    # Simulate the multi-row header found in the real Excel file
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        dummy_header = pd.DataFrame(['Title of Document'])
        dummy_header.to_excel(writer, sheet_name='All', index=False, header=False)
        df.to_excel(writer, sheet_name='All', index=False, startrow=5)
    output.seek(0)
    return output.getvalue()


# --- The Main Test Client Fixture ---

@pytest.fixture(scope="module")
def client(mocker, raw_hpi_content, raw_salary_content) -> TestClient:
    """
    Provides a TestClient for the API after mocking the data fetching process.
    This fixture ensures a clean, predictable test database is created for each test run.
    """
    # 1. Mock the function responsible for downloading data from the internet.
    async def mock_fetch_data(session, url, data_name):
        log.info(f"MOCK FETCH: Intercepted request for {data_name} at {url}")
        if "house-price-index" in url:
            return raw_hpi_content
        elif "earn05" in url:
            return raw_salary_content
        return None
        
    mocker.patch('data_pipeline.fetch_data', new=mock_fetch_data)

    # 2. Set up a temporary database for the tests.
    db_path = "./test_api_database.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    
    # 3. Import the app and create the TestClient.
    # The 'with' statement correctly triggers the app's startup event,
    # which now runs our mocked data pipeline.
    from api import app
    
    with TestClient(app) as c:
        yield c
        
    # 4. Clean up the temporary database after tests are done.
    log.info(f"Test teardown: Removing test database at {db_path}")
    if os.path.exists(db_path):
        os.remove(db_path)

# --- API Endpoint Tests (These can remain exactly the same) ---

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
    assert len(data["regions"]) > 0
    # Based on our mock data, London should be a region
    assert "London" in data["regions"]

def test_get_data_for_region_successful(client: TestClient):
    """Tests fetching data for a valid region, checking for new affordability data."""
    response = client.get("/data/London")
    assert response.status_code == 200
    
    data = response.json()
    assert data["region"] == "London"
    assert "data" in data
    assert len(data["data"]) > 0

    first_item = data["data"][0]
    assert "average_price" in first_item
    assert "average_annual_salary" in first_item
    assert "affordability_ratio" in first_item
    # Check a calculation based on our mock data (1000 weekly * 52)
    assert first_item["average_annual_salary"] == 52000

def test_get_data_for_nonexistent_region(client: TestClient):
    """Tests fetching data for a region that does not exist."""
    response = client.get("/data/Atlantis")
    assert response.status_code == 200
    data = response.json()
    assert data["region"] == "Atlantis"
    assert data["data"] == []