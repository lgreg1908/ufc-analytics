import pytest
from bs4 import BeautifulSoup
from src.scrape.parsers.fighter import parse_fighter, Fighter

def test_parse_fighter_valid_html():
    """
    Test parse_fighter with valid fighter HTML data.
    This test checks if the fighter details are correctly extracted from the HTML.
    """

    # Load the test HTML file
    with open('tests/data/test_fighter.html', 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Call the function to parse the fighter details
    result = parse_fighter(soup)

    # Define the expected Fighter object
    expected_fighter = Fighter(
        full_name="Israel Adesanya",
        nickname="The Last Stylebender",
        height="6' 4\"",
        reach="80\"",
        stance="Switch",
        date_of_birth="Jul 22, 1989",
        record="24-3-0"
    )

    # Assert that the result matches the expected Fighter object
    assert result == expected_fighter