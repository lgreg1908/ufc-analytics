import pandas as pd
import numpy as np


def extract_location_parts(location_series: pd.Series) -> pd.DataFrame:
    """
    Given a Series of location strings (e.g. "City, State, Country" or "City, Country"),
    split them into components in a vectorized manner.
    
    Logic:
      - Split by comma.
      - Strip whitespace.
      - If three or more parts: assign the first part as city, the second as state,
        and the last part as country.
      - If two parts: assign the first as city and second as country, with state left as NaN.
      - Otherwise, return NaN for missing values.
    """
    # Split by comma. expand=True returns a DataFrame with as many columns as maximum splits.
    parts = location_series.str.split(',', expand=True)
    # Remove extra whitespace from every element
    parts = parts.apply(lambda col: col.str.strip())
    
    # Initialize a result DataFrame with the same index
    res = pd.DataFrame(index=parts.index)
    
    # City is always the first element
    res['city'] = parts[0]
    
    # Determine assignment based on how many columns were produced.
    if parts.shape[1] >= 3:
        # e.g. "Sydney, New South Wales, Australia"
        res['state'] = parts[1]
        res['country'] = parts.iloc[:, -1]  # Last column
    elif parts.shape[1] == 2:
        # e.g. "Macau, China"
        res['state'] = np.nan
        res['country'] = parts[1]
    else:
        res['state'] = np.nan
        res['country'] = np.nan

    return res

def convert_height_to_cm(height_series: pd.Series) -> pd.Series:
    """
    Convert a Series of height strings in the format "5' 10\"" to centimeters.
    
    For example, "5' 10\"" is converted to 5*30.48 + 10*2.54 ≈ 177.8 cm.
    If the string is missing or cannot be parsed, NaN is returned.
    """
    # Define a regex pattern to capture feet and inches.
    pattern = r"(?P<feet>\d+)'[\s]*(?P<inches>\d+)"
    # Extract feet and inches into separate columns
    extracted = height_series.str.extract(pattern)
    # Convert extracted parts to numeric values (coerce errors to NaN)
    feet = pd.to_numeric(extracted['feet'], errors='coerce')
    inches = pd.to_numeric(extracted['inches'], errors='coerce')
    # 1 foot = 30.48 cm, 1 inch = 2.54 cm
    height_cm = feet * 30.48 + inches * 2.54
    return height_cm

def convert_reach_to_cm(reach_series: pd.Series) -> pd.Series:
    """
    Convert a Series of reach strings (e.g. "71\"") to centimeters.
    
    The function removes quotation marks and whitespace, converts to numeric,
    then multiplies by 2.54 (1 inch = 2.54 cm). If parsing fails, returns NaN.
    """
    # Remove double quotes and any extra whitespace
    cleaned = reach_series.str.replace('"', '', regex=False).str.strip()
    inches = pd.to_numeric(cleaned, errors='coerce')
    reach_cm = inches * 2.54
    return reach_cm

def convert_time_to_seconds(time_str):
    """
    Convert a time string of the form "m:ss" or "h:mm:ss" to total seconds.
    If time_str is not valid, return NaN.
    """
    if not isinstance(time_str, str) or time_str.strip() == "":
        return np.nan
    parts = time_str.split(':')
    try:
        parts = [float(p) for p in parts]
    except Exception:
        return np.nan
    if len(parts) == 2:
        # minutes and seconds
        return parts[0] * 60 + parts[1]
    elif len(parts) == 3:
        # hours, minutes, seconds
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    else:
        return np.nan

def split_method(method_str):
    """
    Split a method string into two parts: a short method and a detail.
    
    For example, if method_str is:
        "SUB\n\n      \n\n        Guillotine Choke"
    then the function returns:
        ["SUB", "Guillotine Choke"]
    
    If no newline exists, returns [method_str, NaN].
    """
    if not isinstance(method_str, str):
        return pd.Series([np.nan, np.nan])
    # Split by newline and remove empty parts
    parts = [part.strip() for part in method_str.split('\n') if part.strip()]
    if len(parts) == 0:
        return pd.Series([np.nan, np.nan])
    elif len(parts) == 1:
        return pd.Series([parts[0], np.nan])
    else:
        # Only take the first two parts
        return pd.Series([parts[0], parts[1]])
    

def parse_percentage(series: pd.Series) -> pd.Series:
    """
    Convert a Series of percentage strings (e.g. "14%") to a numeric Series.
    Values like '---' are interpreted as missing.
    """
    # Replace '---' with NaN, remove the "%" sign, then convert to numeric.
    return pd.to_numeric(series.replace('---', np.nan).str.replace('%', '', regex=False), errors='coerce')

def parse_fraction(series: pd.Series) -> pd.DataFrame:
    """
    For a Series whose values are in the format "x of y" (e.g. "4 of 27"),
    extract two new numeric columns: one for the numerator ("landed") and one for
    the denominator ("attempted"). Returns a DataFrame with columns "landed" and "attempted".
    """
    extracted = series.str.extract(r'(?P<landed>\d+)\s*of\s*(?P<attempted>\d+)')
    return extracted.apply(pd.to_numeric, errors='coerce')