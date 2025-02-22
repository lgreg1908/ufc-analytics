import logging
from bs4 import BeautifulSoup
from typing import List
from pydantic import ValidationError, BaseModel

logger = logging.getLogger(__name__)

class Event(BaseModel):
    """
    A Pydantic model representing the details of a UFC event.
    
    Attributes:
        event (str): The name of the event.
        event_url (str): The URL of the event detail page.
        date (str): The date of the event, in 'Month DD, YYYY' format.
        location (str): The location where the event is held.
    """
    event: str
    event_url: str
    date: str
    location: str

def parse_event(soup: BeautifulSoup) -> List[Event]:
    """
    Extracts event details from a BeautifulSoup object and returns them as a list of Event objects.

    Parameters:
        soup (BeautifulSoup): Parsed webpage content.

    Returns:
        List[Event]: A list of Event objects,
    """

    events = []
    try:
        event_names = [event.get_text(strip=True) for event in soup.select('a.b-link.b-link_style_black')]
        event_urls = [event.get('href') for event in soup.select('a.b-link.b-link_style_black')]
        event_dates = [date.get_text(strip=True) for date in soup.select('span.b-statistics__date')][1:]  # Skip the first (upcoming event)
        event_locations = [location.get_text(strip=True) for location in soup.select('td.b-statistics__table-col.b-statistics__table-col_style_big-top-padding')][1:]  # Skip the first (upcoming event)

        for event_name, event_url, event_date, event_location in zip(event_names, event_urls, event_dates, event_locations):
            try:
                event = Event(
                    event=event_name,
                    event_url=event_url,
                    date=event_date,  
                    location=event_location
                )
                events.append(event)
            except ValidationError as ve:
                logger.error(f"Validation error for event '{event_name}': {ve}")
                raise ve
        return events
    except Exception as e:
        logger.error(f"An error occurred while parsing event details: {e}")
        raise e