from typing import List, Optional
import pandas as pd
 
 
def wide_to_long_results(
    df: pd.DataFrame, 
    fighter_cols: List[str] = ["fighter1_url", "fighter2_url"], 
    result_cols: List[str] = ["fighter1_result", "fighter2_result"], 
    sort_by: List[str] = ["event_url", "fight_url", "fighter_url"]
) -> pd.DataFrame:
    """
    Convert a wide-format fight results DataFrame (with separate fighter1 and fighter2 columns)
    into a long-format DataFrame with one row per fighter per fight.

    Parameters:
        df (pd.DataFrame): The input wide-format fight results dataframe.
        fighter_cols (List[str], optional): Column names for fighters. Default is ["fighter1_url", "fighter2_url"].
        result_cols (List[str], optional): Column names for fight results. Default is ["fighter1_result", "fighter2_result"].
        sort_by (List[str], optional): Columns to sort the output by. Default is ["event_url", "fight_url", "fighter_url"].

    Returns:
        pd.DataFrame: A long-format DataFrame with:
            - 'fighter_url': The fighter's URL.
            - 'result': The fight result.
            - 'role': Either 'fighter1' or 'fighter2'.
            - 'opp_url': The opponent’s URL.
    """
    roles = ['fighter1', 'fighter2']

    # Create a list to store transformed dataframes
    long_format_dfs = []

    for role, fighter_col, result_col in zip(roles, fighter_cols, result_cols):
        df_transformed = df.copy().rename(columns={fighter_col: "fighter_url", result_col: "result"})
        df_transformed["role"] = role
        df_transformed["opp_url"] = df[fighter_cols[1] if role == 'fighter1' else fighter_cols[0]]
        
        # Drop original fighter-specific columns
        df_transformed = df_transformed.drop(columns=fighter_cols + result_cols, errors="ignore")
        
        long_format_dfs.append(df_transformed)

    # Concatenate transformed data and sort
    long_df = pd.concat(long_format_dfs, ignore_index=True)
    
    if sort_by:
        long_df = long_df.sort_values(by=sort_by).reset_index(drop=True)

    return long_df
