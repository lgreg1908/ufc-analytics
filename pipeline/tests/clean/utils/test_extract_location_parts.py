import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import extract_location_parts

@pytest.mark.parametrize("input_loc, expected", [
    # Three parts: city, state, country.
    ("New York, NY, USA", {"city": "New York", "state": "NY", "country": "USA"}),
    # Two parts: city and country; state should be NaN.
    ("Paris, France", {"city": "Paris", "state": np.nan, "country": "France"}),
    # One part: only city is available; state and country should be NaN.
    ("Tokyo", {"city": "Tokyo", "state": np.nan, "country": np.nan}),
    # Empty string: no parts, all values should be NaN.
    ("", {"city": np.nan, "state": np.nan, "country": np.nan}),
    # Extra empty parts: should be filtered out.
    ("Los Angeles, , USA", {"city": "Los Angeles", "state": np.nan, "country": "USA"}),
    # More than three parts: use first, second and last parts.
    ("Sydney, New South Wales, Australia, Extra", {"city": "Sydney", "state": "New South Wales", "country": "Extra"}),
    # String with only whitespace: should return NaN for all.
    ("   ", {"city": np.nan, "state": np.nan, "country": np.nan}),
])
def test_extract_location_parts_single(input_loc, expected):
    """
    Test individual location strings by converting them into a single-row Series.
    """
    series = pd.Series([input_loc])
    result = extract_location_parts(series)
    # Get the first (and only) row.
    res = result.iloc[0]
    
    # Compare each expected field.
    for key, exp in expected.items():
        if pd.isna(exp):
            assert pd.isna(res[key]), f"Expected {key} to be NaN for input '{input_loc}' but got {res[key]}"
        else:
            assert res[key] == exp, f"Expected {key}='{exp}' for input '{input_loc}' but got {res[key]}"

def test_extract_location_parts_multiple():
    """
    Test a Series containing multiple location strings at once.
    """
    locations = pd.Series([
        "New York, NY, USA",
        "Paris, France",
        "Tokyo",
        "Los Angeles, , USA",
        "Sydney, New South Wales, Australia, Extra"
    ])
    expected_df = pd.DataFrame({
        "city": ["New York", "Paris", "Tokyo", "Los Angeles", "Sydney"],
        "state": ["NY", np.nan, np.nan, np.nan, "New South Wales"],
        "country": ["USA", "France", np.nan, "USA", "Extra"]
    })
    
    result = extract_location_parts(locations)
    
    # Reset index for a consistent comparison.
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected_df)

def test_extract_location_parts_edge_whitespace():
    """
    Test a case with extra spaces and empty segments between commas.
    """
    series = pd.Series(["  London  ,  ,  UK  "])
    result = extract_location_parts(series)
    
    # After stripping, the parts should be: ["London", "UK"]
    # So expected: city = "London", state = NaN, country = "UK".
    res = result.iloc[0]
    assert res["city"] == "London", f"Expected city 'London' but got '{res['city']}'"
    assert pd.isna(res["state"]), f"Expected state to be NaN but got '{res['state']}'"
    assert res["country"] == "UK", f"Expected country 'UK' but got '{res['country']}'"
