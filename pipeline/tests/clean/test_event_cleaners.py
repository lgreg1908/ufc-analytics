import pandas as pd
import numpy as np
import pytest
from datetime import datetime

from pipeline.src.clean.cleaners import EventsCleaner
from pipeline.src.clean.utils import extract_location_parts

# Sample fixture for a DataFrame with multiple rows.
@pytest.fixture
def sample_events_df():
    data = {
        "date": ["2021-01-01", "invalid", "2022-12-31"],
        "location": [
            "Sydney, New South Wales, Australia",  # Three parts
            "Macau, China",                         # Two parts
            "Unknown"                               # One part
        ]
    }
    return pd.DataFrame(data)

def test_extract_location_parts_three_parts():
    """Test extraction when location has three parts."""
    loc_series = pd.Series(["Sydney, New South Wales, Australia"])
    parts = extract_location_parts(loc_series)
    assert parts.loc[0, "city"] == "Sydney"
    assert parts.loc[0, "state"] == "New South Wales"
    assert parts.loc[0, "country"] == "Australia"

def test_extract_location_parts_two_parts():
    """Test extraction when location has two parts."""
    loc_series = pd.Series(["Macau, China"])
    parts = extract_location_parts(loc_series)
    assert parts.loc[0, "city"] == "Macau"
    assert pd.isna(parts.loc[0, "state"])
    assert parts.loc[0, "country"] == "China"

def test_extract_location_parts_one_part():
    """Test extraction when location has only one part."""
    loc_series = pd.Series(["Unknown"])
    parts = extract_location_parts(loc_series)
    assert parts.loc[0, "city"] == "Unknown"
    assert pd.isna(parts.loc[0, "state"])
    assert pd.isna(parts.loc[0, "country"])

def test_events_cleaner_date_conversion(sample_events_df):
    """Test that the EventsCleaner converts 'date' to datetime correctly."""
    cleaner = EventsCleaner(sample_events_df)
    cleaned_df = cleaner.clean()
    
    # The valid date should be converted
    expected_date = pd.to_datetime("2021-01-01")
    assert cleaned_df.loc[0, "date"] == expected_date
    
    # The invalid date should be converted to NaT
    assert pd.isna(cleaned_df.loc[1, "date"])

def test_events_cleaner_location_extraction(sample_events_df):
    """Test that the EventsCleaner extracts location parts correctly."""
    cleaner = EventsCleaner(sample_events_df)
    cleaned_df = cleaner.clean()
    
    # Row 0: Three parts ("Sydney, New South Wales, Australia")
    assert cleaned_df.loc[0, "city"] == "Sydney"
    assert cleaned_df.loc[0, "state"] == "New South Wales"
    assert cleaned_df.loc[0, "country"] == "Australia"
    
    # Row 1: Two parts ("Macau, China")
    assert cleaned_df.loc[1, "city"] == "Macau"
    assert pd.isna(cleaned_df.loc[1, "state"])
    assert cleaned_df.loc[1, "country"] == "China"
    
    # Row 2: One part ("Unknown")
    assert cleaned_df.loc[2, "city"] == "Unknown"
    assert pd.isna(cleaned_df.loc[2, "state"])
    assert pd.isna(cleaned_df.loc[2, "country"])
