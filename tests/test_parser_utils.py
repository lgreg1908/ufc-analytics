import pytest
import pandas as pd
from src.scrape.parsers.utils import normalize_headers, combine_dicts

def test_normalize_headers_with_valid_data():
    """
    Test normalize_headers with valid input headers.
    
    Ensure that headers are correctly normalized: 
    - Lowercase conversion
    - Removal of periods ('.')
    - Spaces replaced by underscores ('_')
    - Percentage signs replaced by 'pct'
    """
    headers = ["Column Name", "Total%", "Value.Value", "Other Column"]
    expected = ["column_name", "totalpct", "valuevalue", "other_column"]
    
    result = normalize_headers(headers)
    
    assert result == expected

def test_normalize_headers_with_empty_list():
    """
    Test normalize_headers with an empty list.
    
    Ensure that an empty list is returned when the input is empty.
    """
    headers = []
    result = normalize_headers(headers)
    
    assert result == []

def test_normalize_headers_with_special_characters():
    """
    Test normalize_headers with special characters in headers.
    
    Ensure that special characters are handled correctly, and only the specified characters 
    (period, space, percentage) are transformed.
    """
    headers = ["Header% Name", "Special.Header."]
    expected = ["headerpct_name", "specialheader"]
    
    result = normalize_headers(headers)
    
    assert result == expected


def test_combine_dicts_with_common_keys():
    """
    Test combine_dicts when both dictionaries have common keys.
    
    Ensure that the combined DataFrame contains the correct merged values with an outer join.
    """
    dict1 = {"id": [1, 2, 3], "value1": [10, 20, 30]}
    dict2 = {"id": [2, 3, 4], "value2": [200, 300, 400]}
    
    result = combine_dicts(dict1, dict2)
    
    # Expected DataFrame
    expected_df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "value1": [10, 20, 30, None],
        "value2": [None, 200, 300, 400]
    })
    
    pd.testing.assert_frame_equal(result, expected_df)

def test_combine_dicts_with_common_keys_outer():
    """
    Test combine_dicts with common keys using the default 'outer' merge strategy.
    
    Ensure that the combined DataFrame contains the correct merged values with an outer join.
    """
    dict1 = {"id": [1, 2, 3], "value1": [10, 20, 30]}
    dict2 = {"id": [2, 3, 4], "value2": [200, 300, 400]}
    
    result = combine_dicts(dict1, dict2, how='outer')
    
    expected_df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "value1": [10, 20, 30, None],
        "value2": [None, 200, 300, 400]
    })
    
    pd.testing.assert_frame_equal(result, expected_df)


def test_combine_dicts_with_common_keys_inner():
    """
    Test combine_dicts with common keys using the 'inner' merge strategy.
    
    Ensure that the combined DataFrame contains only the intersection of both dictionaries.
    """
    dict1 = {"id": [1, 2, 3], "value1": [10, 20, 30]}
    dict2 = {"id": [2, 3, 4], "value2": [200, 300, 400]}
    
    result = combine_dicts(dict1, dict2, how='inner')
    
    expected_df = pd.DataFrame({
        "id": [2, 3],
        "value1": [20, 30],
        "value2": [200, 300]
    })
    
    pd.testing.assert_frame_equal(result, expected_df)


def test_combine_dicts_with_empty_dicts():
    """
    Test combine_dicts when both dictionaries are empty.
    
    Ensure that an empty DataFrame is returned.
    """
    dict1 = {}
    dict2 = {}
    
    result = combine_dicts(dict1, dict2)
    
    expected_df = pd.DataFrame()
    
    pd.testing.assert_frame_equal(result, expected_df)


def test_combine_dicts_invalid_merge_strategy():
    """
    Test combine_dicts with an invalid merge strategy.
    
    Ensure that the function raises a ValueError when an invalid merge strategy is passed.
    """
    dict1 = {"id": [1, 2, 3], "value1": [10, 20, 30]}
    dict2 = {"id": [2, 3, 4], "value2": [200, 300, 400]}
    
    with pytest.raises(ValueError, match="Invalid merge strategy"):
        combine_dicts(dict1, dict2, how='invalid_strategy')


def test_combine_dicts_invalid_inputs():
    """
    Test combine_dicts with invalid inputs (non-dictionary).
    
    Ensure that the function raises a ValueError for invalid inputs.
    """
    dict2 = {"id": [2, 3, 4], "value2": [200, 300, 400]}

    # Invalid input, not a dictionary
    with pytest.raises(ValueError, match="Both inputs must be dictionaries"):
        combine_dicts("invalid_input", dict2)


def test_combine_dicts_with_different_lengths():
    """
    Test combine_dicts when the lists in the dictionaries have different lengths.
    
    Ensure that the combined DataFrame contains NaN for missing values.
    """
    dict1 = {"id": [1, 2], "value1": [10, 20]}
    dict2 = {"id": [2, 3, 4], "value2": [200, 300, 400]}
    
    result = combine_dicts(dict1, dict2, how='outer')
    
    expected_df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "value1": [10, 20, None, None],
        "value2": [None, 200, 300, 400]
    })
    
    pd.testing.assert_frame_equal(result, expected_df)