import pandas as pd
import numpy as np

def compute_stats_fights(raw_df: pd.DataFrame, window_list: list = [3, 5]) -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3]})

# # -----------------------------------------------------------
# # (I) Aggregation: Convert round-level data to fight-level data
# # -----------------------------------------------------------
# def aggregate_rounds_by_fight(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Aggregate round-level data into a fight-level summary per fighter.

#     Group by ['fight_url', 'fighter'] and aggregate:
#       - For 'round': take the maximum.
#       - For numeric columns: sum them.
#       - For non-numeric columns: take the first value.
#     Then, for each fraction pair (e.g. 'td_landed' and 'td_attempted'),
#     compute an accuracy column (e.g. 'td_accuracy').
#     """
#     group_cols = ['fight_url', 'fighter']
#     other_cols = [col for col in df.columns if col not in group_cols]
#     agg_dict = {}
#     for col in other_cols:
#         if col == 'round':
#             agg_dict[col] = 'max'
#         else:
#             agg_dict[col] = 'sum' if pd.api.types.is_numeric_dtype(df[col]) else 'first'
#     fight_df = df.groupby(group_cols, as_index=False).agg(agg_dict)

#     # Compute accuracy for fraction pairs.
#     fraction_pairs = [
#         ('sig_str_landed', 'sig_str_attempted'),
#         ('total_str_landed', 'total_str_attempted'),
#         ('head_landed', 'head_attempted'),
#         ('body_landed', 'body_attempted'),
#         ('leg_landed', 'leg_attempted'),
#         ('distance_landed', 'distance_attempted'),
#         ('clinch_landed', 'clinch_attempted'),
#         ('ground_landed', 'ground_attempted'),
#         ('td_landed', 'td_attempted')
#     ]
#     for landed, attempted in fraction_pairs:
#         if landed in fight_df.columns and attempted in fight_df.columns:
#             accuracy_col = f"{landed.split('_')[0]}_accuracy"
#             fight_df[accuracy_col] = np.where(
#                 fight_df[attempted] > 0,
#                 fight_df[landed] / fight_df[attempted],
#                 np.nan
#             )
#     return fight_df

# # -----------------------------------------------------------
# # (II) Cumulative & Rolling Stats for a Fighter’s Own Metrics
# # -----------------------------------------------------------
# def compute_cumulative_stats(df: pd.DataFrame, metric_cols: list, fight_order_col: str = 'fight_order') -> pd.DataFrame:
#     """
#     For each metric column in `metric_cols`, compute two cumulative statistics:
#       - *_current: cumulative sum (and average) including the current fight.
#       - *_in: cumulative sum (and average) coming into the fight (i.e. from previous fights).
#     """
#     cum_dict = {}
#     for col in metric_cols:
#         cum_current = df.groupby('fighter')[col].cumsum()
#         cum_dict[f'cum_{col}_current'] = cum_current
#         cum_dict[f'avg_{col}_current'] = cum_current / (df[fight_order_col] + 1)
#         shifted = cum_current.shift(1).fillna(0)
#         cum_dict[f'cum_{col}_in'] = shifted
#         cum_dict[f'avg_{col}_in'] = np.where(df[fight_order_col] > 0, shifted / df[fight_order_col], np.nan)
#     return pd.DataFrame(cum_dict, index=df.index)

# def compute_rolling_aggregates(df: pd.DataFrame, metric_cols: list, window_list: list = [3, 5]) -> pd.DataFrame:
#     """
#     For each metric in `metric_cols` and each window size in `window_list`,
#     compute fixed-window rolling aggregates on the incoming data (i.e. after shifting by one fight):
#       - rolling_{w}_{col}_in: rolling sum over the previous w fights.
#       - rolling_{w}_avg_{col}_in: rolling mean over the previous w fights.
#       - Also compute rolling fight count.
#     """
#     roll_dict = {}
#     for w in window_list:
#         roll_stats = df.groupby('fighter')[metric_cols].apply(
#             lambda g: g.shift(1).rolling(window=w, min_periods=1).agg(['sum', 'mean'])
#         ).reset_index(level=0, drop=True)
#         for col in metric_cols:
#             roll_dict[f'rolling_{w}_{col}_in'] = roll_stats[(col, 'sum')]
#             roll_dict[f'rolling_{w}_avg_{col}_in'] = roll_stats[(col, 'mean')]
#         fights_count = df.groupby('fighter')['fight_order'].apply(
#             lambda x: x.shift(1).rolling(window=w, min_periods=1).count()
#         ).reset_index(level=0, drop=True)
#         roll_dict[f'rolling_{w}_fights_in'] = fights_count
#     return pd.DataFrame(roll_dict, index=df.index)

# # -----------------------------------------------------------
# # (III) Opponent Statistics
# # -----------------------------------------------------------
# def assign_opp_url(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     For each fight (grouped by 'fight_url'), assign an 'opp_url' column so that each row's
#     opponent URL is the fighter URL of the other participant.
#     Assumes each fight has exactly two rows.
#     """
#     df = df.copy()
#     df['opp_url'] = (df.groupby('fight_url')['fighter']
#                      .transform(lambda x: x.iloc[::-1].values))
#     return df

# def compute_opponent_stats_fights(df: pd.DataFrame, metric_cols: list) -> pd.DataFrame:
#     """
#     For each row in the aggregated fight-level DataFrame (which must have an 'opp_url' column),
#     compute the opponent's cumulative stats coming into the fight and after the fight.

#     We use merge_asof to join the opponent's statistics from the same DataFrame.
#     For example, for a given metric, the opponent's incoming cumulative value (opp_cum_{col}_in)
#     is obtained by merging on event_date with a shift (i.e. the record before the fight).

#     Returns a DataFrame with additional columns for each metric:
#       - opp_cum_{col}_in, opp_avg_{col}_in: opponent cumulative incoming stats.
#       - opp_cum_{col}_current, opp_avg_{col}_current: opponent cumulative stats including the fight.
#     """
#     # Ensure that the data is sorted by 'opp_url' and 'event_date'.
#     df = df.sort_values('event_date').reset_index(drop=True)

#     # For incoming opponent stats, take the columns computed for fighters
#     opp_in = df[['fighter', 'event_date'] + [f'cum_{col}_in' for col in metric_cols] +
#                 [f'avg_{col}_in' for col in metric_cols]].copy()
#     opp_in = opp_in.rename(columns={'fighter': 'opp_url'})
#     # For current opponent stats:
#     opp_current = df[['fighter', 'event_date'] + [f'cum_{col}_current' for col in metric_cols] +
#                      [f'avg_{col}_current' for col in metric_cols]].copy()
#     opp_current = opp_current.rename(columns={'fighter': 'opp_url'})

#     # Use merge_asof to attach opponent incoming stats.
#     df = df.sort_values('event_date').reset_index(drop=True)
#     df = pd.merge_asof(
#         df.sort_values('event_date'),
#         opp_in.sort_values('event_date'),
#         on='event_date',
#         by='opp_url',
#         direction='backward',
#         suffixes=('', '_opp_in')
#     )

#     # And opponent current stats.
#     df = pd.merge_asof(
#         df.sort_values('event_date'),
#         opp_current.sort_values('event_date'),
#         on='event_date',
#         by='opp_url',
#         direction='forward',
#         suffixes=('', '_opp_current')
#     )
#     return df

# # -----------------------------------------------------------
# # (IV) Master Function for Fight-level Stats from Rounds Data
# # -----------------------------------------------------------
# def compute_stats_fights(raw_df: pd.DataFrame, window_list: list = [3, 5]) -> pd.DataFrame:
#     """
#     Given raw round-level data (with one row per fighter per round) that includes an 'event_date'
#     column, this function aggregates the data to fight-level (one row per fighter per fight) and then computes:

#       (A) Fighter’s own cumulative stats:
#           - _current (including current fight) and _in (incoming, before the fight).
#       (B) Fixed-window rolling aggregates (on incoming data) for each metric.
#       (C) Opponent statistics: by first assigning an 'opp_url' (the opponent's fighter URL) and then merging
#           to compute the opponent's cumulative stats (both incoming and current) for the given metrics.

#     Parameters:
#       raw_df: The raw rounds DataFrame.
#       window_list: List of window sizes for rolling aggregates.

#     Returns:
#       A DataFrame with aggregated fight-level data, fighter’s own stats, fixed-window rolling stats,
#       and opponent stats.
#     """
#     # First, aggregate round-level data.
#     fight_df = aggregate_rounds_by_fight(raw_df)
    
#     # Check that event_date is available.
#     if 'event_date' not in fight_df.columns:
#         raise ValueError("Missing 'event_date' column in aggregated data. Please merge event dates before calling.")
#     fight_df['event_date'] = pd.to_datetime(fight_df['event_date'], errors='coerce')
    
#     # Sort by fighter and event_date and assign a fight order.
#     fight_df = fight_df.sort_values(['fighter', 'event_date']).reset_index(drop=True)
#     fight_df['fight_order'] = fight_df.groupby('fighter').cumcount()
    
#     # Identify numeric metric columns (exclude keys and temporary columns).
#     keys = ['fight_url', 'fighter', 'event_date', 'fight_order']
#     numeric_cols = fight_df.select_dtypes(include=[np.number]).columns.tolist()
#     metric_cols = [col for col in numeric_cols if col not in keys and 
#                    not (col.startswith('cum_') or col.startswith('avg_') or col.startswith('rolling_'))]
    
#     # (A) Compute fighter's own cumulative stats.
#     cum_df = compute_cumulative_stats(fight_df, metric_cols, fight_order_col='fight_order')
#     # (B) Compute fixed-window rolling aggregates.
#     roll_df = compute_rolling_aggregates(fight_df, metric_cols, window_list)
    
#     # Combine the computed columns.
#     fight_df = pd.concat([fight_df, cum_df, roll_df], axis=1)
#     fight_df.drop(columns=['fight_order'], inplace=True)
    
#     # (C) Compute opponent stats:
#     # First assign opponent URL for each fight.
#     fight_df = assign_opp_url(fight_df)
#     # Then compute opponent cumulative stats for the same metric columns.
#     fight_df = compute_opponent_stats_fights(fight_df, metric_cols)
    
#     return fight_df
