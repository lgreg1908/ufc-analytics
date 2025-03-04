import pandas as pd
 
def wide_to_long_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a wide-format results DataFrame (with separate fighter1 and fighter2 columns)
    into a long-format DataFrame with one row per fighter per fight.
    
    The output includes:
      - Common fight fields (fight_url, event_url, weight_class, etc.)
      - 'fighter_url' (from fighter1_url or fighter2_url)
      - 'result' (from fighter1_result or fighter2_result)
      - 'role' (either 'fighter1' or 'fighter2')
      - 'opp_url' (the opponent’s URL)
    """
    # For fighter1 rows:
    df1 = df.copy().rename(columns={
        'fighter1_url': 'fighter_url',
        'fighter1_result': 'result'
    })
    df1['role'] = 'fighter1'
    df1['opp_url'] = df['fighter2_url']
    
    # For fighter2 rows:
    df2 = df.copy().rename(columns={
        'fighter2_url': 'fighter_url',
        'fighter2_result': 'result'
    })
    df2['role'] = 'fighter2'
    df2['opp_url'] = df['fighter1_url']
    
    # Drop the original fighter-specific columns.
    drop_cols = ['fighter1_url', 'fighter2_url', 'fighter1_result', 'fighter2_result']
    df1 = df1.drop(columns=drop_cols, errors='ignore')
    df2 = df2.drop(columns=drop_cols, errors='ignore')
    
    long_df = (
        pd.concat([df1, df2], ignore_index=True)
        .sort_values(by=['event_url', 'fight_url', 'fighter_url'])
        .reset_index(drop=True)
        )

    return long_df