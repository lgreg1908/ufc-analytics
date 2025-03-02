import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import (
    extract_location_parts,
    convert_height_to_cm,
    convert_reach_to_cm,
    convert_time_to_seconds,
    split_method,
    parse_percentage
    )

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

@pytest.mark.parametrize("input_height, expected_cm", [
    # Typical case: "5' 10\"" should yield 5*30.48 + 10*2.54
    ("5' 10\"", 5 * 30.48 + 10 * 2.54),
    # Another valid height
    ("6' 0\"", 6 * 30.48 + 0 * 2.54),
    # No space between feet and inches should also work.
    ("5'10\"", 5 * 30.48 + 10 * 2.54),
    # Extra whitespace around the string
    (" 5' 10\" ", 5 * 30.48 + 10 * 2.54),
    # A different valid value
    ("4' 11\"", 4 * 30.48 + 11 * 2.54),
    # Empty string should return NaN
    ("", np.nan),
    # Non-parsable string returns NaN
    ("invalid", np.nan),
    # None should be treated as missing data and return NaN
    (None, np.nan)
])
def test_convert_height_to_cm_single(input_height, expected_cm):
    """
    Test individual height strings conversion to centimeters.
    """
    series = pd.Series([input_height])
    result = convert_height_to_cm(series)
    
    if pd.isna(expected_cm):
        assert pd.isna(result.iloc[0]), f"Expected NaN for input {input_height} but got {result.iloc[0]}"
    else:
        assert result.iloc[0] == pytest.approx(expected_cm), (
            f"Expected {expected_cm} cm for input {input_height} but got {result.iloc[0]}"
        )

def test_convert_height_to_cm_vectorized():
    """
    Test conversion on a vectorized Series of multiple height strings.
    """
    inputs = ["5' 10\"", "6' 0\"", "5'10\"", "invalid", "", None]
    expected = [
        5 * 30.48 + 10 * 2.54,
        6 * 30.48 + 0 * 2.54,
        5 * 30.48 + 10 * 2.54,
        np.nan,
        np.nan,
        np.nan
    ]
    series = pd.Series(inputs)
    result = convert_height_to_cm(series)
    
    for idx, (res, exp_val) in enumerate(zip(result, expected)):
        if pd.isna(exp_val):
            assert pd.isna(res), f"Expected NaN at index {idx} for input {inputs[idx]} but got {res}"
        else:
            assert res == pytest.approx(exp_val), (
                f"Expected {exp_val} cm at index {idx} for input {inputs[idx]} but got {res}"
            )
@pytest.mark.parametrize("input_reach, expected_cm", [
    # A valid reach string with quotes.
    ('71"', 71 * 2.54),
    # A valid reach string with extra whitespace.
    (' 71" ', 71 * 2.54),
    # A valid reach string without quotes.
    ('71', 71 * 2.54),
    # A non-numeric string should return NaN.
    ('abc', np.nan),
    # An empty string should return NaN.
    ('', np.nan),
    # None value should return NaN.
    (None, np.nan)
])
def test_convert_reach_to_cm_single(input_reach, expected_cm):
    """
    Test the conversion for individual reach strings.
    """
    series = pd.Series([input_reach])
    result = convert_reach_to_cm(series)
    # Check if expected value is NaN
    if pd.isna(expected_cm):
        assert pd.isna(result.iloc[0]), f"Expected NaN for input {input_reach} but got {result.iloc[0]}"
    else:
        assert result.iloc[0] == pytest.approx(expected_cm), (
            f"Expected {expected_cm} for input {input_reach} but got {result.iloc[0]}"
        )

def test_convert_reach_to_cm_vectorized():
    """
    Test the conversion on a Series of multiple reach values.
    """
    inputs = ['71"', '68"', 'abc', '', None]
    expected = [71 * 2.54, 68 * 2.54, np.nan, np.nan, np.nan]
    series = pd.Series(inputs)
    result = convert_reach_to_cm(series)
    
    for idx, (res, exp_val) in enumerate(zip(result, expected)):
        if pd.isna(exp_val):
            assert pd.isna(res), f"At index {idx}, expected NaN for input {inputs[idx]} but got {res}"
        else:
            assert res == pytest.approx(exp_val), (
                f"At index {idx}, expected {exp_val} for input {inputs[idx]} but got {res}"
            )

@pytest.mark.parametrize("input_str, expected", [
    # Test minutes and seconds.
    ("0:30", 30.0),
    ("1:00", 60.0),
    ("10:00", 600.0),
    ("5:05", 5*60 + 5),
    # Test hours, minutes, and seconds.
    ("1:23:45", 1*3600 + 23*60 + 45),  # 5025 seconds
    ("0:00:00", 0.0),
    # Leading/trailing whitespace should be handled.
    ("  2:30  ", 2*60 + 30),
])
def test_valid_time_formats(input_str, expected):
    result = convert_time_to_seconds(input_str)
    assert result == pytest.approx(expected), f"For input '{input_str}', expected {expected} but got {result}"

@pytest.mark.parametrize("input_str", [
    "",          # empty string
    "   ",       # whitespace only
    None,        # non-string input
    "abc",       # non-numeric string
    "1:2:3:4",   # too many parts
    "1",         # single number
    "1::",       # malformed with empty parts
    "1:xx",      # invalid numeric values
])
def test_invalid_time_formats(input_str):
    result = convert_time_to_seconds(input_str)
    assert np.isnan(result), f"For input '{input_str}', expected NaN but got {result}"

@pytest.mark.parametrize("input_method, expected", [
    # Case with a method string containing multiple newlines and extra whitespace.
    (
        "SUB\n\n      \n\n        Guillotine Choke",
        pd.Series(["SUB", "Guillotine Choke"])
    ),
    # Case with no newline: should return the string and NaN.
    (
        "KO/TKO",
        pd.Series(["KO/TKO", np.nan])
    ),
    # Case with an empty string: should return NaN for both parts.
    (
        "",
        pd.Series([np.nan, np.nan])
    ),
    # Case with a string containing only whitespace: should return NaN for both parts.
    (
        "   ",
        pd.Series([np.nan, np.nan])
    ),
    # Non-string input: should return NaN for both parts.
    (
        None,
        pd.Series([np.nan, np.nan])
    ),
    # Case with a trailing newline that results in only one non-empty part.
    (
        "SUB\n",
        pd.Series(["SUB", np.nan])
    ),
    # Case with more than two non-empty parts: should only take the first two.
    (
        "SUB\nDetail\nExtra",
        pd.Series(["SUB", "Detail"])
    )
])
def test_split_method(input_method, expected):
    result = split_method(input_method)
    pd.testing.assert_series_equal(result, expected, check_names=False)

