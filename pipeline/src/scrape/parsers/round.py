from typing import Optional, List, Dict, Any
import logging
from bs4 import BeautifulSoup
from pydantic import BaseModel
import pandas as pd
from pipeline.src.scrape.parsers.utils import combine_dicts, normalize_headers

logger = logging.getLogger(__name__)

class Round(BaseModel):
    round: int
    fight_url: str
    fighter: str
    kd: Optional[str] = None
    sig_str: Optional[str] = None
    sig_str_pct: Optional[str] = None
    total_str: Optional[str] = None
    td: Optional[str] = None           # <-- New field to capture takedowns (Td)
    td_pct: Optional[str] = None
    sub_att: Optional[str] = None
    rev: Optional[str] = None
    ctrl: Optional[str] = None
    head: Optional[str] = None
    body: Optional[str] = None
    leg: Optional[str] = None
    distance: Optional[str] = None
    clinch: Optional[str] = None
    ground: Optional[str] = None

def parse_rounds(soup: BeautifulSoup) -> List[Round]:
    """
    Parses the HTML of a fight page and extracts rounds data, including fighter statistics,
    and returns a list of Round objects.
    ...
    """
    rounds_data = []

    # Extract the fight URL or ID
    fight_url = None
    fight_url_element = soup.find('a', class_='b-link')
    if fight_url_element:
        fight_url = fight_url_element['href']

    # Find all rounds sections
    rounds_sections = soup.find_all('section', class_='b-fight-details__section js-fight-section')

    if rounds_sections and "not currently available" in rounds_sections[0].text:
        logger.warning("No round details found for this fight.")
        return []  # Return an empty list when no round details are available
    
    # Iterate through each round table
    for section in rounds_sections:
        # Find the main table
        table = section.find('table', class_='b-fight-details__table')
        if table is None:
            continue

        # Get headers (Fighter, KD, Sig. str., etc.)
        headers = [header.get_text(strip=True) for header in table.find('thead').find_all('th')]
        headers = normalize_headers(headers=headers)

        # Adjust the duplicated 'td_pct' header.
        # In the per-round table the takedown columns come in as two identical "td_pct" columns.
        # We rename the first occurrence to "td" so that we capture both takedowns and takedown percentage.
        for i in range(1, len(headers)):
            if headers[i] == 'td_pct' and headers[i-1] == 'td_pct':
                headers[i-1] = 'td'
                break 

        # Get rows of data
        rows = table.find('tbody').find_all('tr')
        round_fighters_data = []

        # Extract fighter data from each row
        for row in rows:
            fighter_data = {'fight_url': fight_url}  # Add fight_url to fighter data
            cols = row.find_all('td')
            for idx, col in enumerate(cols):
                # Handle fighter URLs separately, as they are stacked in one cell
                if headers[idx] == 'fighter':
                    fighter_data[headers[idx]] = [fighter['href'] for fighter in col.find_all('a')]
                else:
                    fighter_data[headers[idx]] = [data.get_text(strip=True) for data in col.find_all('p')]
            round_fighters_data.append(fighter_data)

        rounds_data.append(round_fighters_data)
        
    return combine_stats_and_split_fighter(rounds_data)


def combine_stats_and_split_fighter(rounds_details: List[List[Dict[str, Any]]]) -> List[Round]:
    """
    Combines fighter stats from two nested lists of dictionaries and splits the data by fighter,
    returning a list of Round objects with individual stats for each fighter and their corresponding round.
    ...
    """
    all_rounds = []

    # Combine rounds in a vectorized way
    for dict1, dict2 in zip(rounds_details[0], rounds_details[1]):
        combined_df = combine_dicts(dict1, dict2)
        all_rounds.append(combined_df)

    # Concatenate all the round DataFrames, reset index, and drop unnecessary columns
    data = (
        pd.concat(all_rounds, keys=range(1, len(all_rounds) + 1), names=['round'])
        .reset_index().drop(['level_1'], axis=1)
        .to_dict(orient='records')
    )
    return [Round(**row) for row in data]
