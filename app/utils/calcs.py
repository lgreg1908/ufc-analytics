import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any


def compute_fighter_avg_interval(df: pd.DataFrame) -> pd.Series:
    """Compute the average time between fights for each fighter."""
    def avg_interval(dates):
        if len(dates) > 1:
            sorted_dates = sorted(dates)
            total_span = sorted_dates[-1] - sorted_dates[0]
            return total_span.days / (len(sorted_dates) - 1)
        return None
    return df.groupby('fighter_url')['date'].apply(lambda x: avg_interval(x.tolist())).dropna()

def compute_weight_class_distribution(fights_df: pd.DataFrame) -> str:
    """
    Compute the percentage distribution of fights per weight class for a fighter.
    Returns a string like "Middleweight: 90%, Welterweight: 10%".
    """
    counts = fights_df['weight_class'].value_counts(normalize=True) * 100
    counts = counts.round(1)
    distribution_str = ", ".join([f"{wc}: {perc}%" for wc, perc in counts.items()])
    return distribution_str

def calculate_age(birth_date: datetime) -> int:
    """
    Calculate the age of a person based on their birth date.

    Args:
        birth_date (datetime): The birth date of the person.

    Returns:
        int: The person's age in years.
    """
    today = datetime.today()
    return (today - birth_date).days // 365

def calculate_time_since_last_fight(last_fight_date: datetime) -> timedelta:
    """
    Calculate the time elapsed since the last fight.

    Args:
        last_fight_date (datetime): The date of the last fight.

    Returns:
        timedelta: The time duration since the last fight.
    """
    today = datetime.today()
    return today - last_fight_date

def calculate_average_time_between_fights(fight_dates: List[datetime]) -> Optional[timedelta]:
    """
    Calculate the average time interval between fights.

    Args:
        fight_dates (List[datetime]): A list of fight dates.

    Returns:
        Optional[timedelta]: The average time interval between fights or None if there are less than two fights.
    """
    if len(fight_dates) <= 1:
        return None
    sorted_dates = sorted(fight_dates)
    total_span = sorted_dates[-1] - sorted_dates[0]
    return total_span / (len(sorted_dates) - 1)

def compute_additional_stats(fighter: Dict[str, Any], fights_df: Any) -> Dict[str, Any]:
    """
    Compute additional statistics for a fighter.

    Args:
        fighter (Dict[str, Any]): A dictionary containing fighter's information, including 'date_of_birth' and 'date' (last fight date).
        fights_df (Any): A DataFrame containing fight records with a 'date' column.

    Returns:
        Dict[str, Any]: A dictionary containing age, time since last fight in days, and average fight interval in days.
    """
    age = calculate_age(fighter['date_of_birth'])
    time_since_last = calculate_time_since_last_fight(fighter['date'])
    fight_dates = fights_df['date'].tolist()
    avg_interval = calculate_average_time_between_fights(fight_dates)
    
    return {
        'age': age,
        'time_since_last_days': time_since_last.days,
        'avg_interval_days': avg_interval.days if avg_interval is not None else "N/A"
    }
