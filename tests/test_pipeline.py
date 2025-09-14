import pandas as pd
import pytest
import io
from data_pipeline import clean_house_price_data, clean_salary_data # <-- Updated import

# --- Test Data Fixtures ---

@pytest.fixture
def raw_hpi_dataframe() -> pd.DataFrame:
    """Provides a sample raw DataFrame for HPI data testing."""
    data = {
        'Date': ['01/01/2025', '01/02/2025', 'bad-date', None],
        'RegionName': ['London', 'North West', 'Wales', 'Scotland'],
        'AveragePrice': [500000, 200000, 150000, 180000],
        'Index': [120.5, 110.2, 105.1, 108.0],
        'ExtraColumn': ['A', 'B', 'C', 'D']
    }
    return pd.DataFrame(data)

@pytest.fixture
def raw_salary_dataframe() -> pd.DataFrame:
    """Provides a sample raw DataFrame for Salary data testing."""
    # Mimics the structure of the Excel file after skipping initial rows
    data = {
        'Unnamed: 0': ['2025', '2025', '2025'],
        'Unnamed: 1': ['London', 'North West', 'East of England'],
        'Unnamed: 2': ['1000', '800', 'bad-data'],
        'Unnamed: 3': ['[note 1]', '[note 1]', '[note 1]']
    }
    # Create a dataframe with 7 dummy rows at the top to simulate the real file
    header = pd.DataFrame([[''] * 4] * 7)
    body = pd.DataFrame(data)
    full_df = pd.concat([header, body]).reset_index(drop=True)
    # Set the column names to match what pandas would read before we clean them
    full_df.columns = ['Title', 'Region', '2025', 'Notes']
    return full_df


# --- Tests for clean_house_price_data function ---

def test_clean_hpi_data_renames_columns(raw_hpi_dataframe):
    """Tests if the HPI columns are correctly renamed."""
    cleaned_df = clean_house_price_data(raw_hpi_dataframe)
    expected_columns = ['date', 'region_name', 'average_price', 'index', 'year']
    assert all(col in cleaned_df.columns for col in expected_columns)

def test_clean_hpi_data_handles_bad_dates(raw_hpi_dataframe):
    """Tests that rows with unparseable dates are dropped."""
    cleaned_df = clean_house_price_data(raw_hpi_dataframe)
    assert len(cleaned_df) == 2 # bad-date and None should be dropped

def test_clean_hpi_data_adds_year_column(raw_hpi_dataframe):
    """Tests that the 'year' column is correctly extracted."""
    cleaned_df = clean_house_price_data(raw_hpi_dataframe)
    assert 'year' in cleaned_df.columns
    assert cleaned_df['year'].iloc[0] == 2025

# --- NEW: Tests for clean_salary_data function ---

def test_clean_salary_data_skips_header_and_renames(raw_salary_dataframe):
    """Tests that the salary data cleaning skips header rows and renames columns."""
    # We pass the raw dataframe content as bytes to simulate a file download
    output = io.BytesIO()
    raw_salary_dataframe.to_excel(output, index=False, sheet_name='All')
    output.seek(0)
    
    cleaned_df = clean_salary_data(output.getvalue())
    
    expected_columns = ['year', 'region_name', 'average_annual_salary']
    assert all(col in cleaned_df.columns for col in expected_columns)

def test_clean_salary_data_calculates_annual_salary(raw_salary_dataframe):
    """Tests the annual salary calculation and data typing."""
    output = io.BytesIO()
    raw_salary_dataframe.to_excel(output, index=False, sheet_name='All')
    output.seek(0)

    cleaned_df = clean_salary_data(output.getvalue())
    
    # It should drop the 'bad-data' row
    assert len(cleaned_df) == 2
    # Check the calculation for London: 1000 * 52
    assert cleaned_df['average_annual_salary'].iloc[0] == 52000
    assert cleaned_df['year'].iloc[0] == 2025

# We need this import for the new salary tests
import io