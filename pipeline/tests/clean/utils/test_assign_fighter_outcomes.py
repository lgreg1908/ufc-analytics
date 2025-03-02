import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.utils import assign_fighter_outcomes

@pytest.mark.parametrize("input_result, expected", [
    # If the result is "Win", fighter1 wins and fighter2 loses.
    ("Win", ("Win", "Loss")),
    # For draws, both outcomes should be the same.
    ("Draw", ("Draw", "Draw")),
    # For "NC" (no contest) both outcomes are the same.
    ("NC", ("NC", "NC")),
    # For any other outcome, both fighters get the same label.
    ("Loss", ("Loss", "Loss")),
    ("Other", ("Other", "Other")),
    # Test case sensitivity: "win" is not equal to "Win", so both will be "win".
    ("win", ("win", "win")),
    # An empty string should simply return an empty tuple.
    ("", ("", "")),
])
def test_assign_fighter_outcomes(input_result, expected):
    result = assign_fighter_outcomes(input_result)
    assert result == expected, f"For input '{input_result}', expected {expected} but got {result}"