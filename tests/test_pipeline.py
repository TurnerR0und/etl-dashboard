import pandas as pd
import pytest
import io
from data_pipeline import clean_house_price_data, clean_salary_data

pytest.importorskip("openpyxl")

# --- Test Data Fixtures ---

@pytest.fixture
def raw_hpi_content() -> bytes:
    """Provides sample raw HPI data as bytes, simulating a file download."""
    csv_data = """Date,RegionName,AveragePrice,Index,ExtraColumn
01/01/2025,London,500000,120.5,A
01/02/2025,North West,200000,110.2,B
bad-date,Wales,150000,105.1,C
,Scotland,180000,108.0,D
"""
    return csv_data.encode('utf-8')

@pytest.fixture
def raw_salary_content() -> bytes:
    """Provides sample raw Salary data as bytes, simulating an Excel file download."""
    # Create a DataFrame with the structure of the real data
    data = {
        'Region': ['London', 'North West', 'East of England'],
        '2025': [1000, 800, 'bad-data']
    }
    df = pd.DataFrame(data)

    # Simulate an in-memory Excel file
    output = io.BytesIO()
    
    # --- START OF THE FIX ---
    # Use pd.ExcelWriter and explicitly specify the 'openpyxl' engine for writing.
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Write some dummy header content to simulate the real file's structure
        # This ensures our cleaning function's header=5 logic is tested correctly.
        dummy_header = pd.DataFrame(['Title of Document'])
        dummy_header.to_excel(writer, sheet_name='All', index=False, header=False)
        
        # Write the actual data, starting at row 5
        df.to_excel(writer, sheet_name='All', index=False, startrow=5)
    # --- END OF THE FIX ---
    
    output.seek(0)
    return output.getvalue()


# --- Tests for clean_house_price_data function ---

def test_clean_hpi_data_renames_columns(raw_hpi_content):
    """Tests if the HPI columns are correctly renamed."""
    cleaned_df = clean_house_price_data(raw_hpi_content)
    expected = ['date', 'region_name', 'average_price', 'index', 'year']
    assert all(col in cleaned_df.columns for col in expected)

def test_clean_hpi_data_handles_bad_dates(raw_hpi_content):
    """Tests that rows with unparseable dates are dropped."""
    cleaned_df = clean_house_price_data(raw_hpi_content)
    assert len(cleaned_df) == 2

def test_clean_hpi_data_adds_year_column(raw_hpi_content):
    """Tests that the 'year' column is correctly extracted."""
    cleaned_df = clean_house_price_data(raw_hpi_content)
    assert 'year' in cleaned_df.columns
    assert cleaned_df['year'].iloc[0] == 2025

# --- Tests for clean_salary_data function ---

def test_clean_salary_data_skips_header_and_renames(raw_salary_content):
    """Tests that salary data cleaning skips header rows and renames columns."""
    cleaned_df = clean_salary_data(raw_salary_content)
    expected = ['year', 'region_name', 'average_annual_salary']
    assert all(col in cleaned_df.columns for col in expected)

def test_clean_salary_data_calculates_annual_salary(raw_salary_content):
    """Tests the annual salary calculation and data typing."""
    cleaned_df = clean_salary_data(raw_salary_content)
    assert len(cleaned_df) == 2  # Skips the 'bad-data' row
    assert cleaned_df['average_annual_salary'].iloc[0] == 52000  # 1000 * 52
    assert cleaned_df['year'].iloc[0] == 2025
