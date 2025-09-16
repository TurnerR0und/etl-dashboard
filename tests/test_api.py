import pytest
from fastapi.testclient import TestClient
import os
from logger_config import log
import pandas as pd
import io
from unittest.mock import patch

# --- Test Data Fixtures ---

@pytest.fixture(scope="module")
def raw_hpi_content() -> bytes:
    """Provides sample raw HPI data with parent and granular region names."""
    csv_data = """Date,RegionName,OfficialName,AveragePrice,Index
01/01/2025,London,London,500000,120.5
01/01/2025,North West,Manchester,200000,110.2
"""
    return csv_data.encode('utf-8')

@pytest.fixture(scope="module")
def raw_salary_content() -> bytes:
    """Provides sample raw Salary data."""
    data = {
        'Region': ['London', 'North West'],
        '2025': [1000, 800] # Weekly pay
    }
    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame(["Official Statistics"]).to_excel(writer, sheet_name="DataSheet", index=False, header=False)
        df.to_excel(writer, sheet_name="DataSheet", index=False, startrow=5)
    output.seek(0)
    return output.getvalue()

# --- The Main Test Client Fixture ---

@pytest.fixture(scope="module")
def client(raw_hpi_content, raw_salary_content) -> TestClient:
    """Provides a TestClient for the API after mocking the data fetching process."""
    async def mock_fetch_data(session, url, data_name):
        if "house-price-index" in url:
            return raw_hpi_content
        elif "earn05" in url:
            return raw_salary_content
        return None
        
    db_path = "./test_api_database.db"

    with patch('data_pipeline.fetch_data', new=mock_fetch_data):
        if os.path.exists(db_path):
            os.remove(db_path)
        os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"

        from api import app
        with TestClient(app) as c:
            yield c

    if os.path.exists(db_path):
        os.remove(db_path)
    os.environ.pop("DATABASE_URL", None)


# --- API Endpoint Tests ---

def test_read_index(client: TestClient):
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers['content-type']

def test_get_regions_successful(client: TestClient):
    response = client.get("/regions")
    assert response.status_code == 200
    data = response.json()
    assert "regions" in data
    assert "London" in data["regions"]
    assert "Manchester" in data["regions"]

def test_get_data_for_region_successful(client: TestClient):
    response = client.get("/data/London")
    assert response.status_code == 200
    data = response.json()
    assert data["region"] == "London"
    assert len(data["data"]) > 0
    first_item = data["data"][0]
    assert first_item["average_annual_salary"] == 52000
    assert first_item["affordability_ratio"] == (500000 / 52000)

def test_get_data_for_nonexistent_region(client: TestClient):
    response = client.get("/data/Atlantis")
    assert response.status_code == 200
    data = response.json()
    assert data["data"] == []
