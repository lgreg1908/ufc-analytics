import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import assign_method_type

@pytest.mark.parametrize("method_short, expected", [
    # Cases where method_short should be interpreted as a decision.
    ("U-DEC", "Decision"),
    ("S-DEC", "Decision"),
    ("M-DEC", "Decision"),
    # Cases with specific returns.
    ("KO/TKO", "Knockout"),
    ("SUB", "Submission"),
    # Any other string should return 'Other'.
    ("RND", "Other"),
    ("", "Other"),
    ("SomeMethod", "Other"),
    # Even if not a string (None), the function will return 'Other'
    (None, "Other")
])
def test_assign_method_type(method_short, expected):
    result = assign_method_type(method_short)
    assert result == expected, f"For input '{method_short}', expected '{expected}' but got '{result}'"