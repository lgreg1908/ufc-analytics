import pandas as pd
import numpy as np
from src.clean.utils import (
    extract_location_parts, 
    convert_height_to_cm, 
    convert_reach_to_cm, 
    split_method, 
    convert_time_to_seconds,
    parse_fraction,
    parse_percentage,
    assign_method_type,
    assign_fighter_outcomes
    )


class BaseCleaner:
    """
    A base cleaner class using the Template Method pattern.
    Subclasses must implement the clean() method.
    """
    def __init__(self, df: pd.DataFrame):
        # Work on a copy of the dataframe to avoid mutating the original data.
        self.df = df.copy()

    def clean(self) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the clean() method.")


class EventsCleaner(BaseCleaner):
    """
    A cleaner for the events dataset.
    
    Cleaning steps:
      - Convert the date field from string to a pandas datetime.
      - Extract location components into new columns (city, state, country)
        using vectorized string operations.
    """
    def clean(self) -> pd.DataFrame:
        df = self.df.copy()
        # Convert 'date' column to datetime (any invalid parsing becomes NaT)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Extract location parts using our helper function
        loc_parts = extract_location_parts(df['location'])
        
        # Concatenate the new columns to the original dataframe
        df = pd.concat([df, loc_parts], axis=1)
        return df


class ResultsCleaner(BaseCleaner):
    """
    Cleaner for the fight results dataset.
    
    Cleaning steps:
      - Convert the "time" column from a string (e.g., "0:30") to total seconds.
      - Convert the "round" column to numeric.
      - For the "method" column, split it into two new columns ("method_short"
        and "method_detail") when the string contains multiple lines.
      - Convert the "winner" column into a standardized "result_type" column.
      - Split the "fighters_urls" list into two separate columns: "fighter1_url" and "fighter2_url".
    """
    def clean(self) -> pd.DataFrame:
        df = self.df.copy()
        
        # --- Convert time to seconds ---
        # Apply our helper function to the 'time' column
        df['time_seconds'] = df['time'].apply(convert_time_to_seconds)
        
        # --- Convert round to numeric ---
        df['round'] = pd.to_numeric(df['round'], errors='coerce')

        # --- Fight duration --- 
        df['fight_duration_seconds'] = df['time_seconds'] + ((df['round'] - 1) * 300)
        
        # --- Process method ---
        # Create two new columns for method: method_short and method_detail.
        method_split = df['method'].apply(split_method)
        method_split.columns = ['method_short', 'method_detail']
        df = pd.concat([df, method_split], axis=1)
        
        # --- Process winner: rename to result ---
        # For standardization, we can capitalize the first letter.
        df['result_type'] = df['winner'].str.capitalize()
        df.drop(columns=['winner'], inplace=True)
        
        # Create new column method_type based on method_short.
        df['method_type'] = df['method_short'].apply(assign_method_type)

        # --- Process fighter URLs ---
        # Create fighter1_url and fighter2_url columns from the fighters_urls list.
        df['fighter1_url'] = df['fighters_urls'].apply(lambda urls: urls[0] if isinstance(urls, list) and len(urls) > 0 else np.nan)
        df['fighter2_url'] = df['fighters_urls'].apply(lambda urls: urls[1] if isinstance(urls, list) and len(urls) > 1 else np.nan)
        # Optionally drop the original fighters_urls column.
        df.drop(columns=['fighters_urls'], inplace=True)

        # --- Create fighter-specific outcome columns ---
        outcomes = df['result_type'].apply(lambda r: pd.Series(assign_fighter_outcomes(r)))
        outcomes.columns = ['fighter1_result', 'fighter2_result']
        df = pd.concat([df, outcomes], axis=1)

        return df
    

class FighterCleaner(BaseCleaner):
    """
    A cleaner for the fighters dataset.
    
    Cleaning steps:
      - Convert 'date_of_birth' to a pandas datetime.
      - Create a new column 'height_cm' by converting the height from the format
        "5' 10\"" to centimeters.
      - Create a new column 'reach_cm' by converting the reach (in inches) to centimeters.
    """
    def clean(self) -> pd.DataFrame:
        df = self.df.copy()
        
        # Pre-clean the 'date_of_birth' column:
        #  - Fill NaN with empty string, strip whitespace, then replace empty strings with NaN.
        df['date_of_birth'] = df['date_of_birth'].fillna('').str.strip()
        # Pre-clean the date_of_birth strings:
        df['date_of_birth'] = df['date_of_birth'].apply(lambda s: s.replace('\xa0', ' ').strip() if isinstance(s, str) else s)
        
        # Explicitly parse dates with the expected format.
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%b %d, %Y', errors='coerce')
        
        # Convert the 'height' column to centimeters and add as new column
        df['height_cm'] = convert_height_to_cm(df['height'])
        
        # Convert the 'reach' column to centimeters and add as new column
        df['reach_cm'] = convert_reach_to_cm(df['reach'])
        
        return df
    

class RoundsCleaner(BaseCleaner):
    """
    Cleaner for rounds data.
    
    In this dataset we assume each record represents one fighter’s performance in a given round.
    The cleaning steps include:
      - Converting basic numeric fields (round, kd, sub_att, rev).
      - Converting the "ctrl" field (a time string like "0:00" or "1:43") to total seconds.
      - Parsing percentage columns (e.g. "sig_str_pct", "td_pct") into numeric values.
      - Parsing fraction‐style columns (e.g. "sig_str", "total_str", "head", "body", 
        "leg", "distance", "clinch", "ground") into two new columns each (e.g. "sig_str_landed" 
        and "sig_str_attempted").
    """
    def clean(self) -> pd.DataFrame:
        df = self.df.copy()
        
        # --- Convert simple numeric columns ---
        df['round'] = pd.to_numeric(df['round'], errors='coerce')
        df['kd'] = pd.to_numeric(df['kd'], errors='coerce')
        df['sub_att'] = pd.to_numeric(df['sub_att'], errors='coerce')
        df['rev'] = pd.to_numeric(df['rev'], errors='coerce')
        
        # --- Convert control time (ctrl) to seconds ---
        df['ctrl_seconds'] = df['ctrl'].apply(convert_time_to_seconds)
        
        # --- Process percentage columns ---
        df['sig_str_pct_num'] = parse_percentage(df['sig_str_pct'])
        df['td_pct_num'] = parse_percentage(df['td_pct'])
        
        # --- Process fraction columns ---
        # List of columns where the value is of the form "x of y"
        fraction_columns = ['sig_str', 'total_str', 'head', 'body', 'leg', 'distance', 'clinch', 'ground', 'td']
        for col in fraction_columns:
            parsed = parse_fraction(df[col])
            df[f"{col}_landed"] = parsed['landed']
            df[f"{col}_attempted"] = parsed['attempted']
        
        # Drop the original messy columns if they are no longer needed.
        df.drop(columns=['ctrl', 'sig_str', 'total_str', 'td_pct', 'sig_str_pct', 
                         'head', 'body', 'leg', 'distance', 'clinch', 'ground', 'td'], inplace=True)
        
        return df
