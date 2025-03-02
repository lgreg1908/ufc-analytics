import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import convert_reach_to_cm

def test_convert_reach_to_cm():
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
