import pandas as pd
import os
from pandas import DataFrame
from typing import List


def add_dummy_cumsum(df: DataFrame, dummy_col: str, group_col: str, prefix: str = "total_") -> DataFrame:
    """
    Adds dummy variable columns for a specified categorical column and computes
    the cumulative sum of these dummy variables grouped by a given column.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame.
    dummy_col : str
        The name of the column from which to create dummy variables.
        If the column is of string type, it will be fully cleaned.
    group_col : str
        The column name to group by when computing cumulative sums.
    prefix : str, optional
        Prefix for the cumulative sum columns (default is "total_").

    Returns
    -------
    DataFrame
        The DataFrame with added dummy and cumulative sum columns.
    """
    # Fully clean the column data if it's of string type:
    # fill missing values, convert to string, lower case, strip whitespace,
    # and replace spaces with underscores.
    col_data = df[dummy_col]
    if pd.api.types.is_string_dtype(col_data):
        col_data = (
            col_data.fillna("")
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
            .str.replace("'", "")
        )
    
    # Create dummy variables and convert them to integers.
    dummies = pd.get_dummies(col_data).astype(int)
    df = pd.concat([df, dummies], axis=1)

    # Compute the cumulative sum for each group defined by group_col.
    cumsum = df.groupby(group_col)[dummies.columns].cumsum()

    # Rename the cumulative sum columns using the provided prefix.
    cumsum.columns = [f"{prefix}{col}" for col in dummies.columns]

    # Concatenate the cumulative sum columns with the original DataFrame.
    df = pd.concat([df, cumsum], axis=1)

    # Drop the intermediate dummy columns.
    df.drop(columns=dummies.columns, inplace=True)

    return df


def add_numerical_cumsum(df: DataFrame, num_col: str, group_col: str, prefix: str = "total_") -> DataFrame:
    """
    Cleans a numerical column by ensuring numeric type and filling missing values,
    then computes its cumulative sum grouped by a specified column, and adds the result 
    as a new column with a given prefix.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame.
    num_col : str
        The name of the numerical column to be processed.
    group_col : str
        The column name to group by when computing cumulative sums.
    prefix : str, optional
        Prefix for the cumulative sum column name (default is "total_").

    Returns
    -------
    DataFrame
        The DataFrame with an additional cumulative sum column.
    """
    # Extract the numerical column and ensure it is numeric.
    col_data = pd.to_numeric(df[num_col], errors='coerce')
    # Fill missing values with 0.
    col_data = col_data.fillna(0)
    # Update the DataFrame with the cleaned numeric column.
    df[num_col] = col_data

    # Compute the cumulative sum for the numerical column grouped by group_col.
    cumsum = df.groupby(group_col)[num_col].cumsum()
    
    # Define the new column name and add the cumulative sum to the DataFrame.
    new_col_name = f"{prefix}{num_col}"
    df[new_col_name] = cumsum

    return df


def add_all_cumsum_columns(
    df: DataFrame, 
    dummy_cols: List[str], 
    numerical_cols: List[str], 
    group_col: str, 
    prefix: str = "total_",
    row_count_col: str = "row_count"
) -> DataFrame:
    """
    Applies cumulative sum calculations for both dummy and numerical columns,
    and also adds a cumulative row count column for each group.
    
    For each column in `dummy_cols`, the function creates one-hot encoded (dummy)
    columns (after cleaning string values) and computes their cumulative sum grouped 
    by `group_col`. For each column in `numerical_cols`, it ensures the column is numeric,
    cleans it, and computes its cumulative sum grouped by `group_col`. It also computes a 
    cumulative row count for each group.
    
    Parameters
    ----------
    df : DataFrame
        The input DataFrame.
    dummy_cols : List[str]
        List of column names for which to generate dummy variables and compute cumulative sums.
    numerical_cols : List[str]
        List of numerical column names for which to compute cumulative sums.
    group_col : str
        The column name to group by when computing cumulative sums.
    prefix : str, optional
        Prefix for the cumulative sum columns (default is "total_").
    row_count_col : str, optional
        Column name for the cumulative row count (default is "row_count").
    
    Returns
    -------
    DataFrame
        The DataFrame with added dummy columns, cumulative sum columns, and a cumulative row count column.
    """
    # Process each dummy column using the modular function.
    for col in dummy_cols:
        df = add_dummy_cumsum(df, col, group_col, prefix)
    
    # Process each numerical column using the modular function.
    for col in numerical_cols:
        df = add_numerical_cumsum(df, col, group_col, prefix)
        
    # Compute the cumulative row count for each group.
    df[row_count_col] = df.groupby(group_col).cumcount() + 1
    
    return df


def subset_most_recent_fight(df: DataFrame, fighter_col: str, date_col: str) -> DataFrame:
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
