import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import convert_height_to_cm

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
