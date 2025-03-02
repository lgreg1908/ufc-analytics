import pandas as pd
import numpy as np
import pytest
from pytest import approx

# Adjust the import path as needed.
from pipeline.src.clean.cleaners import RoundsCleaner

def test_rounds_cleaner_valid_data():
    # Create a sample DataFrame with valid data for all fields.
    data = {
        'round': ['2'],
        'kd': ['1'],
        'sub_att': ['0'],
        'rev': ['1'],
        'ctrl': ['1:43'],  # Expected to become 103 seconds.
        'sig_str_pct': ['50%'],
        'td_pct': ['20%'],
        'sig_str': ['4 of 10'],
        'total_str': ['8 of 20'],
        'head': ['2 of 5'],
        'body': ['1 of 4'],
        'leg': ['1 of 3'],
        'distance': ['5 of 15'],
        'clinch': ['0 of 2'],
        'ground': ['3 of 7'],
        'td': ['1 of 6']
    }
    df = pd.DataFrame(data)
    cleaner = RoundsCleaner(df)
    cleaned = cleaner.clean()
    
    # --- Check simple numeric conversions ---
    assert cleaned['round'].iloc[0] == 2, "Round conversion failed."
    assert cleaned['kd'].iloc[0] == 1, "KD conversion failed."
    assert cleaned['sub_att'].iloc[0] == 0, "Sub_att conversion failed."
    assert cleaned['rev'].iloc[0] == 1, "Rev conversion failed."
    
    # --- Check control time conversion ---
    # "1:43" should convert to 1*60 + 43 = 103 seconds.
    assert cleaned['ctrl_seconds'].iloc[0] == 103, "Control time conversion failed."
    
    # --- Check percentage columns conversion ---
    assert cleaned['sig_str_pct_num'].iloc[0] == 50.0, "Sig str percentage conversion failed."
    assert cleaned['td_pct_num'].iloc[0] == 20.0, "TD percentage conversion failed."
    
    # --- Check fraction columns conversion ---
    # For each fraction column, verify the landed and attempted values.
    # sig_str: "4 of 10" => landed: 4, attempted: 10.
    assert cleaned['sig_str_landed'].iloc[0] == 4, "sig_str landed conversion failed."
    assert cleaned['sig_str_attempted'].iloc[0] == 10, "sig_str attempted conversion failed."
    
    # total_str: "8 of 20"
    assert cleaned['total_str_landed'].iloc[0] == 8, "total_str landed conversion failed."
    assert cleaned['total_str_attempted'].iloc[0] == 20, "total_str attempted conversion failed."
    
    # head: "2 of 5"
    assert cleaned['head_landed'].iloc[0] == 2, "head landed conversion failed."
    assert cleaned['head_attempted'].iloc[0] == 5, "head attempted conversion failed."
    
    # body: "1 of 4"
    assert cleaned['body_landed'].iloc[0] == 1, "body landed conversion failed."
    assert cleaned['body_attempted'].iloc[0] == 4, "body attempted conversion failed."
    
    # leg: "1 of 3"
    assert cleaned['leg_landed'].iloc[0] == 1, "leg landed conversion failed."
    assert cleaned['leg_attempted'].iloc[0] == 3, "leg attempted conversion failed."
    
    # distance: "5 of 15"
    assert cleaned['distance_landed'].iloc[0] == 5, "distance landed conversion failed."
    assert cleaned['distance_attempted'].iloc[0] == 15, "distance attempted conversion failed."
    
    # clinch: "0 of 2"
    assert cleaned['clinch_landed'].iloc[0] == 0, "clinch landed conversion failed."
    assert cleaned['clinch_attempted'].iloc[0] == 2, "clinch attempted conversion failed."
    
    # ground: "3 of 7"
    assert cleaned['ground_landed'].iloc[0] == 3, "ground landed conversion failed."
    assert cleaned['ground_attempted'].iloc[0] == 7, "ground attempted conversion failed."
    
    # td: "1 of 6"
    assert cleaned['td_landed'].iloc[0] == 1, "td landed conversion failed."
    assert cleaned['td_attempted'].iloc[0] == 6, "td attempted conversion failed."
    
    # --- Check that original messy columns have been dropped ---
    dropped_cols = [
        'ctrl', 'sig_str', 'total_str', 'td_pct', 'sig_str_pct', 
        'head', 'body', 'leg', 'distance', 'clinch', 'ground', 'td'
    ]
    for col in dropped_cols:
        assert col not in cleaned.columns, f"Column '{col}' was not dropped."