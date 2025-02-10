from typing import List, Dict
import pandas as pd 

def normalize_headers(headers: List[str]) -> List[str]:
    """
    Normalize a list of headers by converting them to lowercase and replacing certain characters.

    - Converts all characters to lowercase.
    - Removes periods ('.').
    - Replaces spaces (' ') with underscores ('_').
    - Replaces percentage signs ('%') with 'pct'.

    Args:
        headers (List[str]): A list of header strings to normalize.

    Returns:
        List[str]: A list of normalized header strings.
    """
    return [i.lower().replace('.', '').replace(' ', '_').replace('%', 'pct') for i in headers]

def combine_dicts(
    dict1: Dict[str, list], 
    dict2: Dict[str, list], 
    how: str = 'outer'
) -> pd.DataFrame:
    """
    Combines two dictionaries with list values into a single pandas DataFrame.

    - Converts both input dictionaries into pandas DataFrames.
    - Merges the two DataFrames based on common columns. If no common columns exist, concatenates the DataFrames.
    - Supports different merge strategies ('outer', 'inner', 'left', 'right').

    Args:
        dict1 (Dict[str, list]): The first dictionary with list values to be combined.
        dict2 (Dict[str, list]): The second dictionary with list values to be combined.
        how (str): The merge strategy to use ('outer', 'inner', 'left', 'right'). Default is 'outer'.

    Returns:
        pd.DataFrame: A pandas DataFrame resulting from the merge or concatenation of the two dictionaries.

    Raises:
        ValueError: If inputs are not dictionaries, or if the merge strategy is invalid.
    """
    
    # Validate that both inputs are dictionaries with lists as values
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        raise ValueError("Both inputs must be dictionaries.")

    # Convert dictionaries to DataFrames
    df1 = pd.DataFrame(dict1)
    df2 = pd.DataFrame(dict2)

    # Find common columns
    common_columns = set(df1.columns).intersection(df2.columns)

    if common_columns:
        # Perform merge if common columns exist
        try:
            combined_df = pd.merge(df1, df2, how=how)
        except ValueError as ve:
            raise ValueError(f"Invalid merge strategy: {how}. Expected one of ['outer', 'inner', 'left', 'right'].") from ve
    else:
        # If no common columns, concatenate the DataFrames along the columns
        combined_df = pd.concat([df1.reset_index(drop=True), df2.reset_index(drop=True)], axis=1)

    return combined_df
