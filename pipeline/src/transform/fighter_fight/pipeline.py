import pandas as pd
import numpy as np
from pipeline.src.clean.load_clean import CleanData
from pipeline.src.transform.fighter_fight.components.reshape import wide_to_long_results
from pipeline.src.transform.fighter_fight.components.numerical import add_all_cumsum_columns, add_timedelta_columns


def create_base_dataframe(data: CleanData) -> pd.DataFrame:
    df_results = data.results.copy()
    df_events = data.events.copy()
    df_fighter = data.fighters.copy()

    return (
            df_results
            .pipe(wide_to_long_results)
            .merge(df_fighter, on='fighter_url')
            .merge(df_events, on='event_url')
            .sort_values(by=['date', 'fight_url', 'fighter_url'])
            .reset_index(drop=True)
            .assign(
                is_win=lambda df: np.where(df['result'] == 'Win', 1, 0),
                unique_id=lambda df: df['fight_url'] + df['fighter_url'],
                unique_id_opp=lambda df: df['fight_url'] + df['opp_url'],
                result_method=lambda x: x['result'].str.lower() + '_' + x['method_type'].str.lower()
            )
    )

def add_cumsum_columns(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
    
        .pipe(
            add_all_cumsum_columns,
            dummy_cols=['result', 'result_method', 'weight_class'],
            numerical_cols=['title_fight', 'perf_bonus', 'fight_of_the_night', 'fight_duration_seconds'],
            group_col='fighter_url',
            row_count_col='total_fights'
        )
        .drop(['result_method'], axis=1)
        )

class FighterFight:
    def __init__(self, data: CleanData) -> None:
        self.data = data

    def _create_base_dataframe(self) -> pd.DataFrame:
        return create_base_dataframe(self.data)

    def _add_cumsum_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return add_cumsum_columns(df=df)

    def run(self):
        return (
            self._create_base_dataframe()
            .pipe(self._add_cumsum_columns)
            .pipe(add_timedelta_columns)
        )
        
    

