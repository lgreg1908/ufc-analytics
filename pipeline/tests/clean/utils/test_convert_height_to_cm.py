import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import convert_height_to_cm

def test_convert_height_to_cm():
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
