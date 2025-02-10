import pytest
from unittest.mock import patch
from src.scrape.utils import get_event_urls, get_fighter_urls, get_fight_urls

@patch('src.utils.logger')
def test_get_event_urls_with_valid_data(mock_logger):
    """
    Test get_event_urls with valid event data.
    Ensure that it correctly extracts and returns the event URLs.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    events_data = [
        {'event_url': 'https://www.ufc.com/event1'},
        {'event_url': 'https://www.ufc.com/event2'},
        {'event_url': 'https://www.ufc.com/event3'}
    ]
    
    expected_urls = [
        'https://www.ufc.com/event1',
        'https://www.ufc.com/event2',
        'https://www.ufc.com/event3'
    ]
    
    result = get_event_urls(events_data)
    
    assert result == expected_urls

@patch('src.utils.logger')
def test_get_event_urls_with_missing_event_url(mock_logger):
    """
    Test get_event_urls when an event is missing the 'event_url' field.
    Ensure that it raises a KeyError.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    events_data = [
        {'event_url': 'https://www.ufc.com/event1'},
        {},
        {'event_url': 'https://www.ufc.com/event3'}
    ]
    
    with pytest.raises(KeyError):
        get_event_urls(events_data)


@patch('src.utils.logger')
def test_get_fighter_urls_with_valid_data(mock_logger):
    """
    Test get_fighter_urls with valid fighter data.
    Ensure that it correctly extracts and returns all unique fighter URLs.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = [
        {'fighters_urls': ['https://www.ufc.com/fighter1', 'https://www.ufc.com/fighter2']},
        {'fighters_urls': ['https://www.ufc.com/fighter3', 'https://www.ufc.com/fighter4']}
    ]
    
    expected_urls = [
        'https://www.ufc.com/fighter1',
        'https://www.ufc.com/fighter2',
        'https://www.ufc.com/fighter3',
        'https://www.ufc.com/fighter4'
    ]
    
    result = get_fighter_urls(results_data)
    
    assert set(result) == set(expected_urls)

@patch('src.utils.logger')
def test_get_fighter_urls_with_missing_fighters_urls_field(mock_logger):
    """
    Test get_fighter_urls when a fight is missing the 'fighters_urls' field.
    Ensure that it raises a ValueError.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = [
        {'some_other_field': 'value'},
        {'fighters_urls': ['https://www.ufc.com/fighter1']}
    ]
    
    with pytest.raises(ValueError, match="'fighters_urls' field is missing in the data."):
        get_fighter_urls(results_data)

@patch('src.utils.logger')
def test_get_fighter_urls_with_empty_data(mock_logger):
    """
    Test get_fighter_urls with empty results data.
    Ensure that it returns an empty list.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = []
    
    result = get_fighter_urls(results_data)
    
    assert result == []

@patch('src.utils.logger')
def test_get_fighter_urls_with_duplicate_urls(mock_logger):
    """
    Test get_fighter_urls when there are duplicate fighter URLs.
    Ensure that it only returns unique fighter URLs.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = [
        {'fighters_urls': ['https://www.ufc.com/fighter1', 'https://www.ufc.com/fighter2']},
        {'fighters_urls': ['https://www.ufc.com/fighter1', 'https://www.ufc.com/fighter3']}
    ]
    
    expected_urls = [
        'https://www.ufc.com/fighter1',
        'https://www.ufc.com/fighter2',
        'https://www.ufc.com/fighter3'
    ]
    
    result = get_fighter_urls(results_data)
    
    assert set(result) == set(expected_urls)


@patch('src.utils.logger')
def test_get_fight_urls_with_valid_data(mock_logger):
    """
    Test get_fight_urls with valid fight data.
    Ensure that it correctly extracts and returns all unique fight URLs.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = [
        {'fight_url': 'https://www.ufc.com/fight1'},
        {'fight_url': 'https://www.ufc.com/fight2'},
        {'fight_url': 'https://www.ufc.com/fight3'}
    ]
    
    expected_urls = [
        'https://www.ufc.com/fight1',
        'https://www.ufc.com/fight2',
        'https://www.ufc.com/fight3'
    ]
    
    result = get_fight_urls(results_data)
    
    assert set(result) == set(expected_urls)

@patch('src.utils.logger')
def test_get_fight_urls_with_missing_fight_url_field(mock_logger):
    """
    Test get_fight_urls when a fight is missing the 'fight_url' field.
    Ensure that it raises a ValueError.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = [
        {'some_other_field': 'value'},
        {'fight_url': 'https://www.ufc.com/fight1'}
    ]
    
    with pytest.raises(ValueError, match="'fight_url' field is missing in the data."):
        get_fight_urls(results_data)

@patch('src.utils.logger')
def test_get_fight_urls_with_empty_data(mock_logger):
    """
    Test get_fight_urls with empty results data.
    Ensure that it returns an empty list.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = []
    
    result = get_fight_urls(results_data)
    
    assert result == []

@patch('src.utils.logger')
def test_get_fight_urls_with_duplicate_urls(mock_logger):
    """
    Test get_fight_urls when there are duplicate fight URLs.
    Ensure that it only returns unique fight URLs.
    """
    mock_logger.level = 20  # Ensure the logger has an appropriate level
    results_data = [
        {'fight_url': 'https://www.ufc.com/fight1'},
        {'fight_url': 'https://www.ufc.com/fight2'},
        {'fight_url': 'https://www.ufc.com/fight1'}
    ]
    
    expected_urls = [
        'https://www.ufc.com/fight1',
        'https://www.ufc.com/fight2'
    ]
    
    result = get_fight_urls(results_data)
    
    assert set(result) == set(expected_urls)
