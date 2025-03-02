import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import parse_percentage

def test_parse_percentage():
    """
    Test conversion on a Series containing multiple percentage strings.
    """
    inputs = ["14%", "0%", "100%", "---", "42%", "", " 7%", "abc%"]
    expected = [14.0, 0.0, 100.0, np.nan, 42.0, np.nan, 7.0, np.nan]
    series = pd.Series(inputs)
    result = parse_percentage(series)
    
    for idx, (res, exp_val) in enumerate(zip(result, expected)):
        if pd.isna(exp_val):
            assert pd.isna(res), f"At index {idx}, expected NaN for input '{inputs[idx]}' but got {res}"
        else:
            assert res == pytest.approx(exp_val), (
                f"At index {idx}, expected {exp_val} for input '{inputs[idx]}' but got {res}"
            )