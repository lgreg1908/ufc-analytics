import pandas as pd
import pytest
from pandas.testing import assert_series_equal, assert_frame_equal

# Assuming the following functions are imported from your module:
from src.transform.utils import add_all_cumsum_columns

def test_dummy_only():
    # Create a DataFrame with a dummy (categorical) column.
    df = pd.DataFrame({
        "fighter_url": ["A", "A", "A", "B", "B"],
        "result": ["Win", "Loss", "Win", "Loss", "win"]
    })
    # Apply the function with only dummy columns.
    result_df = add_all_cumsum_columns(
        df.copy(), dummy_cols=["result"], numerical_cols=[], group_col="fighter_url"
    )
    
    # Expected cumulative sums after cleaning:
    # For group A: ["win", "loss", "win"] -> total_win: [1, 1, 2] and total_loss: [0, 1, 1]
    # For group B: ["loss", "win"] -> total_win: [0, 1] and total_loss: [1, 1]
    expected_total_win = pd.Series([1, 1, 2, 0, 1], name="total_win")
    expected_total_loss = pd.Series([0, 1, 1, 1, 1], name="total_loss")
    expected_row_count = pd.Series([1, 2, 3, 1, 2], name="row_count")
    
    assert_series_equal(result_df["total_win"], expected_total_win)
    assert_series_equal(result_df["total_loss"], expected_total_loss)
    assert_series_equal(result_df["row_count"], expected_row_count)
    
    # Check that intermediate dummy columns are dropped.
    assert "win" not in result_df.columns
    assert "loss" not in result_df.columns

def test_numerical_only():
    # Create a DataFrame with a numerical column (with non-numeric values included).
    df = pd.DataFrame({
        "fighter_url": ["A", "A", "B", "B"],
        "score": [1, 2, "3", "bad"]
    })
    # Apply the function with only numerical columns.
    result_df = add_all_cumsum_columns(
        df.copy(), dummy_cols=[], numerical_cols=["score"], group_col="fighter_url"
    )
    
    # 'bad' coerced to NaN then replaced with 0.
    # For group A: cumulative score: [1, 1+2=3]
    # For group B: cumulative score: [3, 3+0=3]
    expected_total_score = pd.Series([1., 3., 3., 3.], name="total_score")
    expected_row_count = pd.Series([1, 2, 1, 2], name="row_count")
    expected_score = pd.Series([1.0, 2.0, 3.0, 0.0], name="score")
    
    assert_series_equal(result_df["total_score"], expected_total_score)
    assert_series_equal(result_df["row_count"], expected_row_count)
    # Verify that the original 'score' column is numeric.
    assert_series_equal(result_df["score"], expected_score)

def test_both_dummy_numerical():
    # Create a DataFrame with both a dummy column and a numerical column.
    df = pd.DataFrame({
        "fighter_url": ["A", "A", "B", "B", "B"],
        "result": ["Win", "Loss", "Win", "Loss", "Loss"],
        "score": [1, None, 2, 3, 4]
    })
    result_df = add_all_cumsum_columns(
        df.copy(), dummy_cols=["result"], numerical_cols=["score"], group_col="fighter_url"
    )
    
    # For group A:
    #   Row 0: "win"   -> cumulative: total_win=1, total_loss=0, total_score=1
    #   Row 1: "loss"  -> cumulative: total_win=1, total_loss=1, total_score=1 (None becomes 0)
    # For group B:
    #   Row 2: "win"   -> cumulative: total_win=1, total_loss=0, total_score=2
    #   Row 3: "loss"  -> cumulative: total_win=1, total_loss=1, total_score=2+3=5
    #   Row 4: "loss"  -> cumulative: total_win=1, total_loss=2, total_score=5+4=9
    expected_total_win = pd.Series([1, 1, 1, 1, 1], name="total_win")
    expected_total_loss = pd.Series([0, 1, 0, 1, 2], name="total_loss")
    expected_total_score = pd.Series([1., 1., 2., 5., 9.], name="total_score")
    expected_row_count = pd.Series([1, 2, 1, 2, 3], name="row_count")
    expected_score = pd.Series([1.0, 0.0, 2.0, 3.0, 4.0], name="score")
    
    assert_series_equal(result_df["total_win"], expected_total_win)
    assert_series_equal(result_df["total_loss"], expected_total_loss)
    assert_series_equal(result_df["total_score"], expected_total_score)
    assert_series_equal(result_df["row_count"], expected_row_count)
    assert_series_equal(result_df["score"], expected_score)
    
    # Check that intermediate dummy columns are dropped.
    assert "win" not in result_df.columns
    assert "loss" not in result_df.columns

def test_custom_prefix_and_row_count():
    # Create a DataFrame and specify custom prefix and row count column name.
    df = pd.DataFrame({
        "group": ["X", "X", "Y"],
        "result": ["Yes", "No", "Yes"],
        "value": [5, 10, 20]
    })
    result_df = add_all_cumsum_columns(
        df.copy(),
        dummy_cols=["result"],
        numerical_cols=["value"],
        group_col="group",
        prefix="cumsum_",
        row_count_col="count"
    )
    
    # For group X:
    #   Row 0: "yes" -> cumulative: cumsum_yes=1, cumsum_no=0, cumsum_value=5, count=1
    #   Row 1: "no"  -> cumulative: cumsum_yes=1, cumsum_no=1, cumsum_value=15, count=2
    # For group Y:
    #   Row 2: "yes" -> cumulative: cumsum_yes=1, cumsum_no=0, cumsum_value=20, count=1
    expected_cumsum_yes = pd.Series([1, 1, 1], name="cumsum_yes")
    expected_cumsum_no = pd.Series([0, 1, 0], name="cumsum_no")
    expected_cumsum_value = pd.Series([5, 15, 20], name="cumsum_value")
    expected_count = pd.Series([1, 2, 1], name="count")
    
    assert_series_equal(result_df["cumsum_yes"], expected_cumsum_yes)
    assert_series_equal(result_df["cumsum_no"], expected_cumsum_no)
    assert_series_equal(result_df["cumsum_value"], expected_cumsum_value)
    assert_series_equal(result_df["count"], expected_count)
    
    # Check that intermediate dummy columns are dropped.
    # After cleaning, the dummy values become lowercase so check for "yes" and "no"
    assert "yes" not in result_df.columns
    assert "no" not in result_df.columns

def test_non_string_dummy_column():
    # Test when the dummy column is not of string dtype.
    df = pd.DataFrame({
        "group": ["G1", "G1", "G2", "G2"],
        "category": [1, 2, 1, 2]
    })
    result_df = add_all_cumsum_columns(
        df.copy(), dummy_cols=["category"], numerical_cols=[], group_col="group"
    )
    
    # For group G1:
    #   Row 0: category 1 -> cumulative: total_1=1, total_2=0; row_count=1
    #   Row 1: category 2 -> cumulative: total_1=1, total_2=1; row_count=2
    # For group G2:
    #   Row 2: category 1 -> cumulative: total_1=1, total_2=0; row_count=1
    #   Row 3: category 2 -> cumulative: total_1=1, total_2=1; row_count=2
    expected_total_1 = pd.Series([1, 1, 1, 1], name="total_1")
    expected_total_2 = pd.Series([0, 1, 0, 1], name="total_2")
    expected_row_count = pd.Series([1, 2, 1, 2], name="row_count")
    
    assert_series_equal(result_df["total_1"], expected_total_1)
    assert_series_equal(result_df["total_2"], expected_total_2)
    assert_series_equal(result_df["row_count"], expected_row_count)
    
    # Check that intermediate dummy columns are dropped.
    # For non-string dummy columns, the intermediate columns (e.g., 1 and 2) should not be present.
    assert 1 not in result_df.columns
    assert 2 not in result_df.columns

def test_empty_dataframe():
    # Test behavior on an empty DataFrame.
    df = pd.DataFrame(columns=["group", "dummy", "num"])
    result_df = add_all_cumsum_columns(
        df.copy(), dummy_cols=["dummy"], numerical_cols=["num"], group_col="group"
    )
    # The result should be an empty DataFrame.
    assert result_df.empty

from src.transform.utils import subset_most_recent_fight

def test_basic():
    # Basic case: multiple fighters with valid dates.
    data = {
        "fighter": ["A", "A", "B", "B", "C"],
        "fight_date": ["2021-01-01", "2021-02-01", "2020-12-31", "2021-03-15", "2021-05-05"],
        "score": [10, 20, 15, 30, 25]
    }
    df = pd.DataFrame(data)
    result = subset_most_recent_fight(df, "fighter", "fight_date")
    
    # For fighter A, most recent is 2021-02-01; fighter B, 2021-03-15; fighter C, 2021-05-05.
    expected = pd.DataFrame({
        "fighter": ["A", "B", "C"],
        "fight_date": pd.to_datetime(["2021-02-01", "2021-03-15", "2021-05-05"]),
        "score": [20, 30, 25]
    }).reset_index(drop=True)
    
    assert_frame_equal(result, expected)

def test_invalid_dates():
    # Some rows contain invalid dates; those rows should be dropped.
    data = {
        "fighter": ["A", "A", "B", "B"],
        "fight_date": ["2021-01-01", "invalid_date", "2021-02-01", "2021-03-01"],
        "score": [10, 20, 15, 30]
    }
    df = pd.DataFrame(data)
    result = subset_most_recent_fight(df, "fighter", "fight_date")
    
    # For fighter A, only the valid "2021-01-01" remains; for fighter B, "2021-03-01" is the most recent.
    expected = pd.DataFrame({
        "fighter": ["A", "B"],
        "fight_date": pd.to_datetime(["2021-01-01", "2021-03-01"]),
        "score": [10, 30]
    }).reset_index(drop=True)
    
    assert_frame_equal(result, expected)

def test_all_invalid_dates():
    # When all date entries are invalid, the result should be an empty DataFrame.
    data = {
        "fighter": ["A", "B"],
        "fight_date": ["not_a_date", "another_bad_date"],
        "score": [10, 20]
    }
    df = pd.DataFrame(data)
    result = subset_most_recent_fight(df, "fighter", "fight_date")
    
    # The expected DataFrame is empty but retains the original columns.
    expected = pd.DataFrame(columns=["fighter", "fight_date", "score"])
    
    assert result.empty
    assert list(result.columns) == list(expected.columns)

def test_same_date():
    # When a fighter has multiple fights on the same date, idxmax() returns the first occurrence.
    data = {
        "fighter": ["A", "A", "B"],
        "fight_date": ["2021-06-01", "2021-06-01", "2021-07-01"],
        "score": [10, 20, 30]
    }
    df = pd.DataFrame(data)
    result = subset_most_recent_fight(df, "fighter", "fight_date")
    
    # For fighter A, the first occurrence (index 0) is returned.
    expected = pd.DataFrame({
        "fighter": ["A", "B"],
        "fight_date": pd.to_datetime(["2021-06-01", "2021-07-01"]),
        "score": [10, 30]
    }).reset_index(drop=True)
    
    assert_frame_equal(result, expected)

def test_empty_dataframe():
    # An empty DataFrame should return an empty DataFrame with the same columns.
    df = pd.DataFrame(columns=["fighter", "fight_date", "score"])
    result = subset_most_recent_fight(df, "fighter", "fight_date")
    
    assert result.empty
    assert list(result.columns) == ["fighter", "fight_date", "score"]

def test_missing_date_column():
    # If the date column is missing, a KeyError should be raised.
    df = pd.DataFrame({
        "fighter": ["A", "B"],
        "score": [10, 20]
    })
    with pytest.raises(KeyError):
        subset_most_recent_fight(df, "fighter", "fight_date")

def test_missing_fighter_column():
    # If the fighter column is missing, a KeyError should be raised.
    df = pd.DataFrame({
        "fight_date": ["2021-01-01", "2021-02-01"],
        "score": [10, 20]
    })
    with pytest.raises(KeyError):
        subset_most_recent_fight(df, "fighter", "fight_date")
