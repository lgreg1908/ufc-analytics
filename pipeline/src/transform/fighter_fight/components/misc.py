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


def correct_perf_bonus(df: pd.DataFrame, result_col: str = "result", perf_bonus_col: str = "perf_bonus") -> pd.DataFrame:
    """
    Corrects the 'perf_bonus' column in the given dataframe by ensuring that no fighter 
    receives a performance bonus if they lost the fight.

    Parameters:
        df (pd.DataFrame): The dataframe containing fight data.
        result_col (str, optional): The column indicating the fight result. Default is "result".
        perf_bonus_col (str, optional): The column indicating if a fighter received a performance bonus. Default is "perf_bonus".

    Returns:
        pd.DataFrame: The dataframe with corrected 'perf_bonus' values.
    """
    # Ensure 'perf_bonus' is boolean
    if df[perf_bonus_col].dtype != bool:
        df[perf_bonus_col] = df[perf_bonus_col].astype(bool)

    # Apply correction: If result is 'Loss' and perf_bonus is True, set perf_bonus to False
    mask = (df[result_col] == 'Loss') & (df[perf_bonus_col])
    df.loc[mask, perf_bonus_col] = False

    return df