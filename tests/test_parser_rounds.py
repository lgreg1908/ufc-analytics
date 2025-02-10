import pytest
from bs4 import BeautifulSoup
from src.scrape.parsers.rounds import parse_rounds, Round


@pytest.fixture
def soup():
    """
    Fixture to load the test HTML content from 'test_details.html'.
    
    This fixture reads the test HTML file and creates a BeautifulSoup object
    which can be used in multiple tests.
    
    Returns:
        BeautifulSoup: Parsed HTML content.
    """
    with open('tests/data/test_rounds.html', 'r', encoding='utf-8') as file:
        content = file.read()
    return BeautifulSoup(content, 'html.parser')

def test_parse_rounds(soup):
    """
    Test the parse_rounds function with valid round data from the provided HTML.

    This test verifies that:
    - The parse_rounds function returns a list of Round objects.
    - Each item in the result is an instance of the Round class.
    - Key details of the parsed data (like round number, fighter names, significant strikes, etc.) match expected values.

    Args:
        soup (BeautifulSoup): Parsed HTML content for testing.
    """
    result = parse_rounds(soup)
    
    # Validate the result is a list of Round objects
    assert isinstance(result, list)
    assert len(result) > 0
    for round_obj in result:
        assert isinstance(round_obj, Round)
    
    # Validate first fighter details in round 1
    assert result[0].round == 1
    assert result[0].fighter == 'http://ufcstats.com/fighter-details/bc711b6dd95c1af6'
    assert result[0].sig_str == '20 of 53'
    assert result[0].sig_str_pct == '37%'
    assert result[0].total_str == '40 of 81'
    assert result[0].kd == '0'

    # Validate second fighter details in round 1
    assert result[1].round == 1
    assert result[1].fighter == 'http://ufcstats.com/fighter-details/d0f3959b4a9747e6'
    assert result[1].sig_str == '13 of 26'
    assert result[1].sig_str_pct == '50%'
    assert result[1].total_str == '20 of 38'
    assert result[1].kd == '0'