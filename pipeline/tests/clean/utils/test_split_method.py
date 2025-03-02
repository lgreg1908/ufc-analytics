import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import split_method

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

