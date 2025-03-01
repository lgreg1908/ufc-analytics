import pytest
from bs4 import BeautifulSoup
from src.scrape.parsers.event import parse_event, Event


def test_parse_event_from_file():
    """
    Test parse_event using the provided HTML file (test_event.html).
    
    Ensure that the function parses the file and extracts the correct event details.
    """
    # Load the HTML content from the uploaded file
    with open("tests/data/test_event.html", 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    events = parse_event(soup)

    # Check that some expected events are correctly parsed
    expected_event_1 = Event(
        event='UFC Fight Night: Blanchfield vs. Fiorot',
        event_url='http://ufcstats.com/event-details/dba230fe33011201',
        date='March 30, 2024',
        location='Atlantic City, New Jersey, USA'
    )
    
    expected_event_2 = Event(
        event='UFC Fight Night: Ribas vs. Namajunas',
        event_url='http://ufcstats.com/event-details/79ff6545b0abc685',
        date='March 23, 2024',
        location='Las Vegas, Nevada, USA'
    )

    assert len(events) > 0  # Ensure that events were parsed
    assert expected_event_1 in events  # Ensure that expected events were parsed
    assert expected_event_2 in events
