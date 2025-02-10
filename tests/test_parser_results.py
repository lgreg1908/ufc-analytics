import pytest
from bs4 import BeautifulSoup
from src.scrape.parsers.results import parse_results
import os

@pytest.fixture
def test_draw_soup():
    """Fixture to load and return the BeautifulSoup object for test_result_draw.html"""
    with open(os.path.join("tests", "data", "test_result_draw.html")) as f:
        return BeautifulSoup(f, "html.parser")

@pytest.fixture
def test_win_soup():
    """Fixture to load and return the BeautifulSoup object for test_result_win.html"""
    with open(os.path.join("tests", "data", "test_result_win.html")) as f:
        return BeautifulSoup(f, "html.parser")

def test_parse_results_draw(test_draw_soup):
    """Test parsing fight results from a draw fight result."""
    results = parse_results(test_draw_soup)
    
    assert len(results) > 0  # Ensure results are returned
    assert results[1].winner == "draw"  # Ensure draw has no winner
    assert results[1].fighters_urls == [
        "http://ufcstats.com/fighter-details/140745cbbcb023ac",
        "http://ufcstats.com/fighter-details/eabf206b162b3b83"
    ]
    assert results[1].method == "S-DEC"
    assert results[1].round == "3"
    assert results[1].time == "5:00"
    
def test_parse_results_win(test_win_soup):
    """Test parsing fight results from a win fight result."""
    results = parse_results(test_win_soup)
    
    assert len(results) > 0  # Ensure results are returned
    assert results[0].winner == "win"  # Ensure winner is parsed correctly
    assert results[0].fighters_urls == [
        "http://ufcstats.com/fighter-details/d0f3959b4a9747e6",
        "http://ufcstats.com/fighter-details/05339613bf8e9808"
    ]
    assert results[0].method == "U-DEC"
    assert results[0].round == "5"
    assert results[0].time == "5:00"
