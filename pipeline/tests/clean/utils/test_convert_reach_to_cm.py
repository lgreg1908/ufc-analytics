import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import convert_reach_to_cm

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
