import pandas as pd
import numpy as np
import pytest

from pipeline.src.clean.cleaners import ResultsCleaner

@pytest.fixture
def sample_data():
    data = {
        'time': ['0:30', '1:00'],
        'round': ['1', '2'],
        'method': ['SUB\n\nGuillotine Choke', 'KO/TKO'],
        'winner': ['win', 'draw'],
        'fighters_urls': [
            ['http://fighter1a.com', 'http://fighter2a.com'],
            ['http://fighter1b.com']  # Only one URL provided; fighter2 should become NaN.
        ]
    }
    return pd.DataFrame(data)

def test_results_cleaner(sample_data):
    # Instantiate ResultsCleaner.
    # If ResultsCleaner does not accept the DataFrame in its constructor,
    # we set it manually.
    cleaner = ResultsCleaner(sample_data)
    
    cleaned_df = cleaner.clean()
    
    # --- Check that expected columns exist ---
    expected_columns = [
        'time', 'round', 'method',
        'time_seconds', 'fight_duration_seconds',
        'method_short', 'method_detail',
        'result_type', 'method_type',
        'fighter1_url', 'fighter2_url',
        'fighter1_result', 'fighter2_result'
    ]
    for col in expected_columns:
        assert col in cleaned_df.columns, f"Missing column: {col}"
    
    # --- Test time conversion ---
    # "0:30" should become 30 seconds and "1:00" 60 seconds.
    assert cleaned_df.loc[0, 'time_seconds'] == 30, "Time conversion error for row 0"
    assert cleaned_df.loc[1, 'time_seconds'] == 60, "Time conversion error for row 1"
    
    # --- Test round conversion ---
    # Ensure that the round values are numeric.
    assert cleaned_df.loc[0, 'round'] == 1, "Round conversion error for row 0"
    assert cleaned_df.loc[1, 'round'] == 2, "Round conversion error for row 1"
    
    # --- Test fight duration calculation ---
    # fight_duration_seconds = time_seconds + ((round - 1) * 300)
    # Row 0: 30 + ((1-1)*300) = 30; Row 1: 60 + ((2-1)*300) = 360.
    assert cleaned_df.loc[0, 'fight_duration_seconds'] == 30, "Fight duration error for row 0"
    assert cleaned_df.loc[1, 'fight_duration_seconds'] == 360, "Fight duration error for row 1"
    
    # --- Test method splitting ---
    # For row 0, "SUB\n\nGuillotine Choke" should be split into:
    #   method_short = "SUB" and method_detail = "Guillotine Choke".
    # For row 1, "KO/TKO" should yield method_detail as NaN.
    assert cleaned_df.loc[0, 'method_short'] == "SUB", "Method short split error for row 0"
    assert cleaned_df.loc[0, 'method_detail'] == "Guillotine Choke", "Method detail split error for row 0"
    assert pd.isna(cleaned_df.loc[1, 'method_detail']), "Method detail should be NaN for row 1"
    
    # --- Test winner processing ---
    # The 'winner' column should be removed and replaced with 'result_type'
    # with the value capitalized.
    assert cleaned_df.loc[0, 'result_type'] == "Win", "Result type capitalization error for row 0"
    assert cleaned_df.loc[1, 'result_type'] == "Draw", "Result type capitalization error for row 1"
    assert 'winner' not in cleaned_df.columns, "Original 'winner' column should be dropped"
    
    # --- Test method_type assignment ---
    # For row 0, method_short "SUB" should assign 'Submission'
    # For row 1, method_short "KO/TKO" should assign 'Knockout'
    assert cleaned_df.loc[0, 'method_type'] == "Submission", "Method type assignment error for row 0"
    assert cleaned_df.loc[1, 'method_type'] == "Knockout", "Method type assignment error for row 1"
    
    # --- Test fighter URLs splitting ---
    assert cleaned_df.loc[0, 'fighter1_url'] == "http://fighter1a.com", "Fighter1 URL error for row 0"
    assert cleaned_df.loc[0, 'fighter2_url'] == "http://fighter2a.com", "Fighter2 URL error for row 0"
    assert cleaned_df.loc[1, 'fighter1_url'] == "http://fighter1b.com", "Fighter1 URL error for row 1"
    assert pd.isna(cleaned_df.loc[1, 'fighter2_url']), "Fighter2 URL should be NaN for row 1"
    
    # --- Test fighter outcomes ---
    # Using assign_fighter_outcomes: if result_type is "Win", expect ("Win", "Loss"),
    # otherwise both outcomes should be the same.
    assert cleaned_df.loc[0, 'fighter1_result'] == "Win", "Fighter1 outcome error for row 0"
    assert cleaned_df.loc[0, 'fighter2_result'] == "Loss", "Fighter2 outcome error for row 0"
    assert cleaned_df.loc[1, 'fighter1_result'] == "Draw", "Fighter1 outcome error for row 1"
    assert cleaned_df.loc[1, 'fighter2_result'] == "Draw", "Fighter2 outcome error for row 1"
