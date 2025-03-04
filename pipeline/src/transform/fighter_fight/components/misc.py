from typing import List
import pandas as pd
 

def add_opponent(
    df: pd.DataFrame, 
    unique_id_col: str = "unique_id", 
    unique_id_opp_col: str = "unique_id_opp", 
    fight_url_col: str = "fight_url",
    exclude_cols: List[str] = ["is_win"],
    suffixes: tuple = ("_f1", "_f2")
) -> pd.DataFrame:
    """
    Adds opponent data to the dataframe by merging fighters with their opponents.

    Parameters:
        df (pd.DataFrame): The input dataframe in long format (two rows per fight).
        unique_id_col (str, optional): Column representing the fighter's unique identifier. Default is "unique_id".
        unique_id_opp_col (str, optional): Column representing the opponent's unique identifier. Default is "unique_id_opp".
        fight_url_col (str, optional): Column representing the fight URL. Default is "fight_url".
        exclude_cols (List[str], optional): Columns to exclude from the opponent data before merging. Default is ["is_win"].
        suffixes (tuple, optional): Suffixes for fighter 1 and fighter 2 columns. Default is ("_f1", "_f2").

    Returns:
        pd.DataFrame: The dataframe with opponent data merged, including fighter (f1) and opponent (f2) attributes.
    """
    # Create a copy of the dataframe, dropping excluded columns
    df_opp = df.drop(columns=exclude_cols, errors="ignore").copy()

    # Merge fighters with their opponents
    df_merged = (
        df.merge(df_opp, left_on=unique_id_col, right_on=unique_id_opp_col, suffixes=suffixes)
        .drop_duplicates(subset=f"{fight_url_col}{suffixes[0]}", keep="first")
    )

    return df_merged

