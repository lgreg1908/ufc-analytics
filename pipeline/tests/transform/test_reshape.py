import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pipeline.src.transform.fighter_fight.components.reshape import wide_to_long_results, subset_most_recent_fight

def test_standard_wide_to_long():
    """Test the function with a standard multi-row DataFrame."""
    data = {
        'fight_url': ['fight1', 'fight2'],
        'event_url': ['event1', 'event2'],
        'weight_class': ['lightweight', 'heavyweight'],
        'fighter1_url': ['fighter1_A', 'fighter1_B'],
        'fighter2_url': ['fighter2_A', 'fighter2_B'],
        'fighter1_result': ['win', 'loss'],
        'fighter2_result': ['loss', 'win']
    }
    df_wide = pd.DataFrame(data)
    
    # Expected output after transformation and sorting.
    expected_data = {
        'fight_url': ['fight1', 'fight1', 'fight2', 'fight2'],
        'event_url': ['event1', 'event1', 'event2', 'event2'],
        'weight_class': ['lightweight', 'lightweight', 'heavyweight', 'heavyweight'],
        'fighter_url': ['fighter1_A', 'fighter2_A', 'fighter1_B', 'fighter2_B'],
        'result': ['win', 'loss', 'loss', 'win'],
        'role': ['fighter1', 'fighter2', 'fighter1', 'fighter2'],
        'opp_url': ['fighter2_A', 'fighter1_A', 'fighter2_B', 'fighter1_B']
    }
    expected_df = pd.DataFrame(expected_data)
    
    result_df = wide_to_long_results(df_wide)
    
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_single_row():
    """Test the function with a single-row DataFrame."""
    data = {
        'fight_url': ['fight_single'],
        'event_url': ['event_single'],
        'weight_class': ['middleweight'],
        'fighter1_url': ['fighter1_single'],
        'fighter2_url': ['fighter2_single'],
        'fighter1_result': ['win'],
        'fighter2_result': ['loss']
    }
    df_wide = pd.DataFrame(data)
    
    expected_data = {
        'fight_url': ['fight_single', 'fight_single'],
        'event_url': ['event_single', 'event_single'],
        'weight_class': ['middleweight', 'middleweight'],
        'fighter_url': ['fighter1_single', 'fighter2_single'],
        'result': ['win', 'loss'],
        'role': ['fighter1', 'fighter2'],
        'opp_url': ['fighter2_single', 'fighter1_single']
    }
    expected_df = pd.DataFrame(expected_data)
    
    result_df = wide_to_long_results(df_wide)
    
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_missing_required_column():
    """Test that a KeyError is raised if a required column is missing."""
    # Create a DataFrame missing 'fighter2_url'
    data = {
        'fight_url': ['fight1'],
        'event_url': ['event1'],
        'weight_class': ['lightweight'],
        'fighter1_url': ['fighter1_missing'],
        # 'fighter2_url' is missing
        'fighter1_result': ['win'],
        'fighter2_result': ['loss']
    }
    df_wide = pd.DataFrame(data)
    
    with pytest.raises(KeyError):
        # This should raise a KeyError when trying to access df['fighter2_url']
        wide_to_long_results(df_wide)

def test_complex_wide_to_long():
    """
    Test the transformation on a complex, unsorted wide DataFrame that includes:
      - Multiple fights from different events
      - Extra columns (date, venue, round) of different types
      - Unsorted input to ensure the final DataFrame is correctly sorted.
    """
    # Create a complex unsorted wide-format DataFrame using a list of dictionaries
    input_data = [
        {
            'fight_url': 'F2',
            'event_url': 'E2',
            'weight_class': 'Light',
            'fighter1_url': 'f1',
            'fighter2_url': 'f2',
            'fighter1_result': 'win',
            'fighter2_result': 'loss',
            'date': '2025-01-01',
            'venue': 'City A',
            'round': 3
        },
        {
            'fight_url': 'F1',
            'event_url': 'E1',
            'weight_class': 'Heavy',
            'fighter1_url': 'f3',
            'fighter2_url': 'f4',
            'fighter1_result': 'loss',
            'fighter2_result': 'win',
            'date': '2025-01-02',
            'venue': 'City B',
            'round': 5
        },
        {
            'fight_url': 'F3',
            'event_url': 'E1',
            'weight_class': 'Middle',
            'fighter1_url': 'f5',
            'fighter2_url': 'f6',
            'fighter1_result': 'draw',
            'fighter2_result': 'draw',
            'date': '2025-01-03',
            'venue': 'City C',
            'round': 2
        }
    ]
    df_wide = pd.DataFrame(input_data)

    # Expected DataFrame after conversion
    # The function sorts by event_url, fight_url, fighter_url.
    expected_data = [
        # Fight from event E1, fight F1 (Heavy)
        {
            'fight_url': 'F1',
            'event_url': 'E1',
            'weight_class': 'Heavy',
            'date': '2025-01-02',
            'venue': 'City B',
            'round': 5,
            'fighter_url': 'f3',
            'result': 'loss',
            'role': 'fighter1',
            'opp_url': 'f4'
        },
        {
            'fight_url': 'F1',
            'event_url': 'E1',
            'weight_class': 'Heavy',
            'date': '2025-01-02',
            'venue': 'City B',
            'round': 5,
            'fighter_url': 'f4',
            'result': 'win',
            'role': 'fighter2',
            'opp_url': 'f3'
        },
        # Fight from event E1, fight F3 (Middle)
        {
            'fight_url': 'F3',
            'event_url': 'E1',
            'weight_class': 'Middle',
            'date': '2025-01-03',
            'venue': 'City C',
            'round': 2,
            'fighter_url': 'f5',
            'result': 'draw',
            'role': 'fighter1',
            'opp_url': 'f6'
        },
        {
            'fight_url': 'F3',
            'event_url': 'E1',
            'weight_class': 'Middle',
            'date': '2025-01-03',
            'venue': 'City C',
            'round': 2,
            'fighter_url': 'f6',
            'result': 'draw',
            'role': 'fighter2',
            'opp_url': 'f5'
        },
        # Fight from event E2, fight F2 (Light)
        {
            'fight_url': 'F2',
            'event_url': 'E2',
            'weight_class': 'Light',
            'date': '2025-01-01',
            'venue': 'City A',
            'round': 3,
            'fighter_url': 'f1',
            'result': 'win',
            'role': 'fighter1',
            'opp_url': 'f2'
        },
        {
            'fight_url': 'F2',
            'event_url': 'E2',
            'weight_class': 'Light',
            'date': '2025-01-01',
            'venue': 'City A',
            'round': 3,
            'fighter_url': 'f2',
            'result': 'loss',
            'role': 'fighter2',
            'opp_url': 'f1'
        }
    ]
    # Specify the expected column order as produced by the function.
    expected_columns = ['fight_url', 'event_url', 'weight_class', 'fighter_url', 'result', 
                        'date', 'venue', 'round', 'role', 'opp_url']
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)

    # Execute the transformation
    result_df = wide_to_long_results(df_wide)

    # Assert that the resulting DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_subset_most_recent_fight_basic():
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

def test_subset_most_recent_fight_invalid_dates():
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

def test_subset_most_recent_fight_all_invalid_dates():
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

def test_subset_most_recent_fight_same_date():
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

def test_subset_most_recent_fight_empty_dataframe():
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
