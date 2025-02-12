import pandas as pd
from abc import ABC, abstractmethod
from src.transform.results_utils import compute_stats_results
from src.transform.fights_utils import compute_stats_fights

def validate_columns(df: pd.DataFrame, required: list, df_name: str = "DataFrame") -> None:
    """Raise a ValueError if any column in `required` is missing from `df`."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {df_name}: {missing}")

def merge_events(df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge event data into the DataFrame.
    
    Expects events_df to have columns 'event_url' and 'date'. Renames 'date' to 'event_date'.
    """
    validate_columns(events_df, ['event_url', 'date'], df_name="Events DataFrame")
    events_df = events_df.rename(columns={'date': 'event_date'})
    return df.merge(events_df, on='event_url', how='left')

def merge_results(df: pd.DataFrame, results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge results data into the DataFrame.
    
    Expects results_df to have columns 'fight_url' and 'event_url'.
    """
    validate_columns(results_df, ['fight_url', 'event_url'], df_name="Results DataFrame")
    res_subset = results_df[['fight_url', 'event_url']].drop_duplicates()
    return df.merge(res_subset, on='fight_url', how='left')

def merge_fighter(df: pd.DataFrame, fighter_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge fighter data into the DataFrame.
    
    Expects results_df to have columns 'fighter_url' 
    """
    validate_columns(fighter_df, ['fighter_url'], df_name="Results DataFrame")
    fighter_df = fighter_df.drop_duplicates()
    return df.merge(fighter_df, on='fighter_url', how='left')

# -----------------------------------------------------------------------------
# Base Transformer Class
# -----------------------------------------------------------------------------
class BaseTransformer(ABC):
    """
    Abstract base transformer class.
    
    Subclasses must implement the transform() method.
    """
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    @abstractmethod
    def transform(self, **kwargs) -> pd.DataFrame:
        pass

# -----------------------------------------------------------------------------
# ResultsTransformer Class
# -----------------------------------------------------------------------------
class ResultsTransformer(BaseTransformer):
    """
    Transformer for results data.

    Transformation phases:
      1. Convert the wide-format results DataFrame into a long-format DataFrame.
      2. Optionally merge with events data (columns: 'event_url' and 'date') to add an 'event_date'.
      3. Compute all fighter and opponent rolling aggregates.
    """
    REQUIRED_COLUMNS = [
        'fight_url', 'event_url', 'weight_class', 'method', 'round', 'time',
        'title_fight', 'perf_bonus', 'fight_of_the_night', 'time_seconds',
        'fight_duration_seconds', 'fighter1_url', 'fighter2_url',
        'fighter1_result', 'fighter2_result', 'result_type'
    ]
    
    def _validate_input(self) -> None:
        validate_columns(self.df, self.REQUIRED_COLUMNS, df_name="Wide Results DataFrame")
    
    def transform(self, events_df: pd.DataFrame = None, fighter_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Transform the raw results DataFrame by validating required columns,
        optionally merging in event dates, and computing rolling aggregates.
        """
        self._validate_input()
        df = self.df.copy()

        if events_df is not None:
            df = merge_events(df, events_df)

        # Call the external results rolling function.
        df = compute_stats_results(df, window_list=[3, 5])

        # Merge in fighter if results data is provided.
        if fighter_df is not None:
            df = merge_fighter(df, fighter_df) 
        return df

# -----------------------------------------------------------------------------
# FightTransformer Class
# -----------------------------------------------------------------------------
class FightTransformer(BaseTransformer):
    """
    Transformer for rounds data (aggregated to fight-level statistics).

    Transformation phases:
      1. Validate that the raw rounds DataFrame contains the required columns.
      2. Optionally merge with a results DataFrame (to bring in event_url).
      3. Optionally merge with an events DataFrame (to obtain event_date).
      4. Aggregate round-level data to one row per fighter per fight.
      5. Compute cumulative (current and incoming) and fixed-window rolling aggregates.
    """
    REQUIRED_COLUMNS = [
        'fight_url', 'fighter', 'round', 'kd', 'sub_att', 'rev', 'ctrl_seconds',
        'sig_str_landed', 'sig_str_attempted', 'total_str_landed', 'total_str_attempted',
        'head_landed', 'head_attempted', 'body_landed', 'body_attempted',
        'leg_landed', 'leg_attempted', 'distance_landed', 'distance_attempted',
        'clinch_landed', 'clinch_attempted', 'ground_landed', 'ground_attempted',
        'td_landed', 'td_attempted'
    ]
    
    def _validate_input(self) -> None:
        validate_columns(self.df, self.REQUIRED_COLUMNS, df_name="Raw Rounds DataFrame")
    
    def transform(
            self, 
            results_df: pd.DataFrame = None, 
            events_df: pd.DataFrame = None,
            fighter_df: pd.DataFrame = None
            ) -> pd.DataFrame:
        """
        Transform raw rounds data into an aggregated fight-level DataFrame with cumulative
        and rolling aggregates.
        """
        self._validate_input()
        df = self.df.copy()

        # Merge in event_url if results data is provided.
        if results_df is not None:
            df = merge_results(df, results_df)
        
        # Merge in event_date if events data is provided.
        if events_df is not None:
            df = merge_events(df, events_df)
        else:
            # Otherwise, create a dummy ordering.
            df['event_date'] = pd.to_datetime('1970-01-01') + pd.to_timedelta(range(len(df)), unit='D')
        
        # Call the external fights stats function (which aggregates rounds and computes stats).
        fight_stats_df = compute_stats_fights(df, window_list=[3, 5])

        # Merge in fighter if results data is provided.
        fight_stats_df = fight_stats_df.rename(columns={'fighter': 'fighter_url'})
        if fighter_df is not None:
            fight_stats_df = merge_fighter(fight_stats_df, fighter_df) 

        return fight_stats_df
