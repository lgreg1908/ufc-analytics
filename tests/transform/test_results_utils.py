import pandas as pd
import pytest
from src.transform.results_utils import wide_to_long_results

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