import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import parse_fraction

def test_parse_fraction_vectorized():
    """
    Test the conversion on a Series containing multiple fraction strings at once.
    """
    inputs = ["4 of 27", "10 of 15", "invalid", "", "8 of 20", "  3 of 9"]
    expected_df = pd.DataFrame({
        "landed": [4, 10, np.nan, np.nan, 8, 3],
        "attempted": [27, 15, np.nan, np.nan, 20, 9]
    })
    series = pd.Series(inputs)
    result = parse_fraction(series)
    
    pd.testing.assert_frame_equal(result, expected_df)