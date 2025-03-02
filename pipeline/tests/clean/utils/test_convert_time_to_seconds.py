import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import convert_time_to_seconds

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
