import pandas as pd
import pytest
from datetime import datetime
from data_pipeline import clean_data

# --- Test Data Fixtures ---

@pytest.fixture
def raw_dataframe() -> pd.DataFrame:
    """Provides a sample raw DataFrame for testing."""
    data = {
        'Date': ['01/01/2025', '01/02/2025', 'bad-date', None],
        'RegionName': ['London', 'North West', 'Wales', 'Scotland'],
        'AveragePrice': [500000, 200000, 150000, 180000],
        'Index': [120.5, 110.2, 105.1, 108.0],
        'ExtraColumn': ['A', 'B', 'C', 'D'] # This column should be dropped
    }
    return pd.DataFrame(data)

@pytest.fixture
def dataframe_with_nulls() -> pd.DataFrame:
    """Provides a sample raw DataFrame with missing critical values."""
    data = {
        'Date': ['01/01/2025', '01/02/2025'],
        'RegionName': ['London', None], # Missing RegionName
        'AveragePrice': [500000, 200000],
        'Index': [120.5, None] # Missing Index
    }
    return pd.DataFrame(data)


# --- Tests for clean_data function ---

def test_clean_data_renames_columns(raw_dataframe):
    """
    Tests if the columns are correctly renamed.
    """
    cleaned_df = clean_data(raw_dataframe)
    expected_columns = ['date', 'region_name', 'average_price', 'index']
    assert all(col in cleaned_df.columns for col in expected_columns)
    assert 'RegionName' not in cleaned_df.columns # Check old name is gone

def test_clean_data_drops_extra_columns(raw_dataframe):
    """
    Tests if columns not in the rename mapping are dropped.
    """
    cleaned_df = clean_data(raw_dataframe)
    assert 'ExtraColumn' not in cleaned_df.columns

def test_clean_data_converts_date_type(raw_dataframe):
    """
    Tests if the 'date' column is converted to datetime objects.
    """
    cleaned_df = clean_data(raw_dataframe)
    # The first row should be a valid datetime object
    assert isinstance(cleaned_df['date'].iloc[0], pd.Timestamp)

def test_clean_data_handles_bad_dates(raw_dataframe):
    """
    Tests that rows with unparseable dates are dropped.
    """
    cleaned_df = clean_data(raw_dataframe)
    # The original dataframe had 4 rows, but one date was bad and another was None
    # Both should be dropped by the cleaning process (dropna + coerce)
    assert len(cleaned_df) == 2

def test_clean_data_drops_rows_with_nulls(dataframe_with_nulls):
    """
    Tests that rows with nulls in critical columns are dropped.
    """
    cleaned_df = clean_data(dataframe_with_nulls)
    # The original dataframe had 2 rows, but the second had nulls
    assert len(cleaned_df) == 1
    assert cleaned_df['region_name'].iloc[0] == 'London'
