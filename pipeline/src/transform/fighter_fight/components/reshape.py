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


def shift_dataframe(
    df: pd.DataFrame, 
    group_col: str = "fighter_url", 
    exclude_cols: Optional[List[str]] = None, 
    shift_steps: int = 1
) -> pd.DataFrame:
    """
    Shifts selected columns in the dataframe to retrieve data from previous fights.

    Parameters:
        df (pd.DataFrame): The input dataframe containing fight data.
        group_col (str, optional): The column to group by before shifting (default: 'fighter_url').
        exclude_cols (Optional[List[str]], optional): Columns to exclude from shifting (default: key fight metadata).
        shift_steps (int, optional): The number of steps to shift (default: 1, for previous fight data).

    Returns:
        pd.DataFrame: The dataframe with shifted columns for past fight data.
    """
    if exclude_cols is None:
        exclude_cols = ['fight_url', 'fighter_url', 'opp_url', 'is_win', 'unique_id', 
                        'unique_id_opp', 'date', 'result']

    # Identify columns to shift
    cols_to_shift = df.columns.difference(exclude_cols)

    # Apply the shift operation
    for col in cols_to_shift:
        df[f'prev_{col}'] = df.groupby(group_col)[col].shift(shift_steps)

    return df


def subset_most_recent_fight(df: pd.DataFrame, fighter_col: str, date_col: str) -> pd.DataFrame:
    """
    Subset the DataFrame to return the most recent fight for each fighter based on the given date column.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame containing fight records.
    fighter_col : str
        The column name that uniquely identifies each fighter.
    date_col : str
        The column name that contains the date or timestamp of the fight.
        This column should be convertible to datetime.

    Returns
    -------
    DataFrame
        A subset of the input DataFrame containing only the most recent fight for each fighter.
    """
    # Convert the date column to datetime (coercing errors to NaT)
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Drop rows where the date conversion failed (NaT)
    df = df.dropna(subset=[date_col])
    
    # Group by fighter and get the index of the row with the maximum (most recent) date
    idx = df.groupby(fighter_col)[date_col].idxmax()
    
    # Return the subset of rows corresponding to the most recent fight per fighter.
    return df.loc[idx].copy().reset_index(drop=True)
