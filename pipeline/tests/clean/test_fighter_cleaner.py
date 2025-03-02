import pandas as pd
import numpy as np
import pytest
import datetime
from pytest import approx

# Adjust the import path as needed.
from pipeline.src.clean.cleaners import FighterCleaner

# Sample test for valid and invalid data
def test_fighter_cleaner_valid():
    # Prepare sample data
    data = {
        # Valid date with expected format; also include a non-breaking space in row 1.
        "date_of_birth": ["Jan 01, 2000", "Feb\xa028, 1990", None],
        # Valid heights for row 0 and 1; row 2 has an invalid height.
        "height": ["5' 10\"", "6' 2\"", "invalid"],
        # Valid reaches for row 0 and 1; row 2 has an invalid reach.
        "reach": ["71\"", "74\"", "invalid"]
    }
    df = pd.DataFrame(data)
    
    # Instantiate FighterCleaner and set its DataFrame.
    cleaner = FighterCleaner(df)
    
    # Run the cleaning process.
    result = cleaner.clean()
    
    # --- Test date_of_birth conversion ---
    # Row 0: "Jan 01, 2000" should be parsed as January 1, 2000.
    expected_date0 = pd.Timestamp("2000-01-01")
    # Row 1: "Feb\xa028, 1990" should become "Feb 28, 1990".
    expected_date1 = pd.Timestamp("1990-02-28")
    # Row 2: None should result in NaT.
    assert result.loc[0, "date_of_birth"] == expected_date0, \
        f"Expected {expected_date0} but got {result.loc[0, 'date_of_birth']}"
    assert result.loc[1, "date_of_birth"] == expected_date1, \
        f"Expected {expected_date1} but got {result.loc[1, 'date_of_birth']}"
    assert pd.isna(result.loc[2, "date_of_birth"]), "Expected NaT for invalid/missing date_of_birth"
    
    # --- Test height_cm conversion ---
    # Row 0: "5' 10\"" should convert to 5*30.48 + 10*2.54.
    expected_height0 = 5 * 30.48 + 10 * 2.54  # ≈ 177.8
    # Row 1: "6' 2\"" should convert to 6*30.48 + 2*2.54.
    expected_height1 = 6 * 30.48 + 2 * 2.54  # ≈ 187.96
    # Row 2: "invalid" should yield NaN.
    assert result.loc[0, "height_cm"] == approx(expected_height0, rel=1e-2), \
        f"Expected {expected_height0} cm but got {result.loc[0, 'height_cm']}"
    assert result.loc[1, "height_cm"] == approx(expected_height1, rel=1e-2), \
        f"Expected {expected_height1} cm but got {result.loc[1, 'height_cm']}"
    assert pd.isna(result.loc[2, "height_cm"]), "Expected NaN for invalid height"
    
    # --- Test reach_cm conversion ---
    # Row 0: "71\"" should convert to 71 * 2.54.
    expected_reach0 = 71 * 2.54  # ≈ 180.34
    # Row 1: "74\"" should convert to 74 * 2.54.
    expected_reach1 = 74 * 2.54  # ≈ 187.96
    # Row 2: "invalid" should yield NaN.
    assert result.loc[0, "reach_cm"] == approx(expected_reach0, rel=1e-2), \
        f"Expected {expected_reach0} cm but got {result.loc[0, 'reach_cm']}"
    assert result.loc[1, "reach_cm"] == approx(expected_reach1, rel=1e-2), \
        f"Expected {expected_reach1} cm but got {result.loc[1, 'reach_cm']}"
    assert pd.isna(result.loc[2, "reach_cm"]), "Expected NaN for invalid reach"

def test_fighter_cleaner_preserves_columns():
    # Create a minimal valid DataFrame.
    data = {
        "date_of_birth": ["Mar 15, 1985"],
        "height": ["5' 8\""],
        "reach": ["70\""]
    }
    df = pd.DataFrame(data)
    cleaner = FighterCleaner(df)
    result = cleaner.clean()
    
    # Expected columns: original ones plus the new ones.
    expected_columns = {"date_of_birth", "height", "reach", "height_cm", "reach_cm"}
    assert set(result.columns) >= expected_columns, \
        f"Expected columns to include {expected_columns} but got {set(result.columns)}"

