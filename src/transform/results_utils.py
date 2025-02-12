import pandas as pd
import numpy as np


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
    
    long_df = pd.concat([df1, df2], ignore_index=True)
    return long_df

# ---------------------------
# (A) Fighter's Own Stats
# ---------------------------
def compute_base_fighter_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute each fighter's own cumulative (after-fight) statistics and "incoming" statistics
    (i.e. the record coming into the current fight).
    """
    # Create binary win/loss columns.
    df['win'] = (df['result'] == 'Win').astype(int)
    df['loss'] = (df['result'] == 'Loss').astype(int)
    
    # "After fight" stats (including current fight)
    df['total_fights_after'] = df.groupby('fighter_url').cumcount() + 1
    df['cumulative_wins_after'] = df.groupby('fighter_url')['win'].cumsum()
    df['cumulative_losses_after'] = df.groupby('fighter_url')['loss'].cumsum()
    df['win_rate_after'] = df['cumulative_wins_after'] / df['total_fights_after']
    df['avg_time_seconds_after'] = df.groupby('fighter_url')['time_seconds'] \
                                     .transform(lambda x: x.expanding().mean())
    
    # "Incoming" stats (before current fight)
    df['total_fights_in'] = df.groupby('fighter_url').cumcount()  # = total_fights_after - 1
    df['cumulative_wins_in'] = df.groupby('fighter_url')['cumulative_wins_after'].shift(1).fillna(0)
    df['cumulative_losses_in'] = df.groupby('fighter_url')['cumulative_losses_after'].shift(1).fillna(0)
    df['win_rate_in'] = np.where(df['total_fights_in'] > 0,
                                 df['cumulative_wins_in'] / df['total_fights_in'],
                                 np.nan)
    df['avg_time_seconds_in'] = df.groupby('fighter_url')['time_seconds'] \
                                  .transform(lambda x: x.shift(1).expanding().mean())
    return df

# ---------------------------
# (B) Fixed-window Aggregates
# ---------------------------
def compute_fixed_window_stats(df: pd.DataFrame, window_list=[3, 5]) -> pd.DataFrame:
    for w in window_list:
        # After fight: rolling aggregates on original series.
        df[f'rolling_{w}_wins'] = df.groupby('fighter_url')['win'] \
                                      .transform(lambda x: x.rolling(window=w, min_periods=1).sum())
        df[f'rolling_{w}_fights'] = df.groupby('fighter_url')['win'] \
                                       .transform(lambda x: x.rolling(window=w, min_periods=1).count())
        df[f'rolling_{w}_win_rate'] = df[f'rolling_{w}_wins'] / df[f'rolling_{w}_fights']
        df[f'rolling_{w}_avg_time'] = df.groupby('fighter_url')['time_seconds'] \
                                         .transform(lambda x: x.rolling(window=w, min_periods=1).mean())
        # Incoming: rolling aggregates computed on the shifted series.
        df[f'rolling_{w}_wins_in'] = df.groupby('fighter_url')['win'] \
                                       .transform(lambda x: x.shift(1).rolling(window=w, min_periods=1).sum())
        df[f'rolling_{w}_fights_in'] = df.groupby('fighter_url')['win'] \
                                        .transform(lambda x: x.shift(1).rolling(window=w, min_periods=1).count())
        df[f'rolling_{w}_win_rate_in'] = np.where(df[f'rolling_{w}_fights_in'] > 0,
                                                  df[f'rolling_{w}_wins_in'] / df[f'rolling_{w}_fights_in'],
                                                  np.nan)
        df[f'rolling_{w}_avg_time_in'] = df.groupby('fighter_url')['time_seconds'] \
                                           .transform(lambda x: x.shift(1).rolling(window=w, min_periods=1).mean())
    return df

# ---------------------------
# (C) Time Differences
# ---------------------------
def compute_time_differences(df: pd.DataFrame) -> pd.DataFrame:
    df['first_fight_date'] = df.groupby('fighter_url')['event_date'].transform('first')
    df['time_since_first_fight'] = (df['event_date'] - df['first_fight_date']).dt.days
    df['prev_fight_date'] = df.groupby('fighter_url')['event_date'].shift(1)
    df['time_since_last_fight'] = (df['event_date'] - df['prev_fight_date']).dt.days
    df = df.drop(columns=['first_fight_date', 'prev_fight_date'])
    return df

# ---------------------------
# (D) Opponent Statistics
# ---------------------------
def compute_opponent_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute opponent statistics for each row:
      - Incoming opponent stats: opponent's cumulative wins/losses before the fight.
      - After opponent stats: opponent's cumulative wins/losses including the fight.
      - Aggregated (total) opponent stats: cumulative sums over all fights so far.
    """
    # For opponent incoming stats:
    opp_stats_in = df[['fighter_url', 'event_date', 'cumulative_wins_after', 'cumulative_losses_after']].copy()
    # Shift within each opponent group so that we capture the record _before_ the fight.
    opp_stats_in['opp_cumulative_wins_in'] = opp_stats_in.groupby('fighter_url')['cumulative_wins_after'].shift(1)
    opp_stats_in['opp_cumulative_losses_in'] = opp_stats_in.groupby('fighter_url')['cumulative_losses_after'].shift(1)
    opp_stats_in = opp_stats_in.rename(columns={'fighter_url': 'opp_url'})
    
    df = df.sort_values('event_date').reset_index(drop=True)
    opp_stats_in = opp_stats_in.sort_values('event_date').reset_index(drop=True)
    
    df = pd.merge_asof(
        df,
        opp_stats_in[['opp_url', 'event_date', 'opp_cumulative_wins_in', 'opp_cumulative_losses_in']],
        on='event_date',
        by='opp_url',
        direction='backward'
    )
    
    # For opponent after stats (including the current fight):
    opp_stats_after = df[['fighter_url', 'event_date', 'cumulative_wins_after', 'cumulative_losses_after']].copy()
    opp_stats_after = opp_stats_after.rename(columns={
        'fighter_url': 'opp_url',
        'cumulative_wins_after': 'opp_cumulative_wins_after',
        'cumulative_losses_after': 'opp_cumulative_losses_after'
    })
    opp_stats_after = opp_stats_after.sort_values('event_date').reset_index(drop=True)
    df = pd.merge_asof(
        df.sort_values('event_date'),
        opp_stats_after[['opp_url', 'event_date', 'opp_cumulative_wins_after', 'opp_cumulative_losses_after']],
        on='event_date',
        by='opp_url',
        direction='forward'
    )
    
    # Aggregated (total) opponent incoming stats.
    df['opp_cumulative_wins_in_filled'] = df['opp_cumulative_wins_in'].fillna(0)
    df['opp_cumulative_losses_in_filled'] = df['opp_cumulative_losses_in'].fillna(0)
    df['total_opp_cumulative_wins_in'] = df.groupby('fighter_url')['opp_cumulative_wins_in_filled'].transform('cumsum')
    df['total_opp_cumulative_losses_in'] = df.groupby('fighter_url')['opp_cumulative_losses_in_filled'].transform('cumsum')
    df.drop(columns=['opp_cumulative_wins_in_filled', 'opp_cumulative_losses_in_filled'], inplace=True)
    
    # Aggregated (total) opponent after stats.
    df['opp_cumulative_wins_after_filled'] = df['opp_cumulative_wins_after'].fillna(0)
    df['opp_cumulative_losses_after_filled'] = df['opp_cumulative_losses_after'].fillna(0)
    df['total_opp_cumulative_wins_after'] = df.groupby('fighter_url')['opp_cumulative_wins_after_filled'].transform('cumsum')
    df['total_opp_cumulative_losses_after'] = df.groupby('fighter_url')['opp_cumulative_losses_after_filled'].transform('cumsum')
    df.drop(columns=['opp_cumulative_wins_after_filled', 'opp_cumulative_losses_after_filled'], inplace=True)
    
    return df

# ---------------------------
# Master Function
# ---------------------------
def compute_stats_results(df: pd.DataFrame, window_list=[3, 5]) -> pd.DataFrame:
    """
    Modular function that computes:
      (A) Fighter's own stats (after and incoming)
      (B) Fixed-window rolling aggregates (after and incoming)
      (C) Time differences
      (D) Opponent statistics (incoming, after, and aggregated)
    """
    df = df.copy()
    
    df = wide_to_long_results(df)
    # (A) Fighter's Own Stats:
    df = compute_base_fighter_stats(df)
    # (B) Fixed-window Aggregates:
    df = compute_fixed_window_stats(df, window_list)
    # (C) Time Differences:
    df = compute_time_differences(df)
    # (D) Opponent Statistics:
    df = compute_opponent_stats(df)
    return df