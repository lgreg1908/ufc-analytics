import logging
from bs4 import BeautifulSoup
from pydantic import ValidationError, BaseModel, Field
from typing import List, Optional

logger = logging.getLogger(__name__)

class Result(BaseModel):
    fight_url: str
    event_url: Optional[str]= Field(default=None)
    winner: str
    fighters_urls: List[str]
    weight_class: str
    method: str
    round: str
    time: str
    title_fight: bool = Field(default=False)
    perf_bonus: bool = Field(default=False)
    fight_of_the_night: bool = Field(default=False)

def parse_results(soup: BeautifulSoup) -> List[Result]:
    """
    Parses fight results from a BeautifulSoup object, including the event_id, and returns validated fight results.

    Args:
        soup (BeautifulSoup): Parsed HTML content from the fight results page.
        event_id (str): The unique identifier of the event

    Returns:
        List[Result]: A list of fight result objects.
    """
    fight_details = []
    fight_rows = soup.select('tr.b-fight-details__table-row.b-fight-details__table-row__hover.js-fight-details-click')

    for row in fight_rows:
        try:
            # Check for title fight, performance bonus, and fight of the night
            fight_bonus_images = row.select_one('td:nth-child(7)').select('img')
            title_fight = any(img['src'].endswith('belt.png') for img in fight_bonus_images)
            perf_bonus = any(img['src'].endswith('perf.png') for img in fight_bonus_images)
            fight_of_the_night = any(img['src'].endswith('fight.png') for img in fight_bonus_images)

            # Parse fight details
            details = {
                'fight_url': row.get('data-link'),
                'winner': row.select_one('td:first-child a.b-flag').text.strip(),
                'fighters_urls': [a['href'] for a in row.select('td:nth-child(2) a')],
                'weight_class': row.select_one('td:nth-child(7)').text.strip(),
                'method': row.select_one('td:nth-child(8)').text.strip(),
                'round': row.select_one('td:nth-child(9)').text.strip(),
                'time': row.select_one('td:nth-child(10)').text.strip(),
                'title_fight': title_fight,
                'perf_bonus': perf_bonus,
                'fight_of_the_night': fight_of_the_night
            }

            # Create Result object and append to fight_details
            fight_details_obj = Result(**details)
            fight_details.append(fight_details_obj)

        except ValidationError as e:
            logger.error(f"Pydantic schema validation error: {e}")
            logger.debug(f"Row content: {row}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            logger.debug(f"Row content: {row}")
            raise e

    if not fight_details:
        logger.warning(f"No fight details were parsed from the event.")

    return fight_details
