import os
import logging
from unittest.mock import patch, MagicMock
import pytest
from typing import Any
from src.logger import (
    setup_logger, 
    ensure_log_directory_exists, 
    create_console_handler, 
    create_file_handler, 
    LoggerSetupError,
    RotatingFileHandler)

# Test for ensuring the log directory exists
@patch('os.makedirs')
@patch('os.path.exists', return_value=False)
def test_ensure_log_directory_exists(mock_exists: Any, mock_makedirs: Any) -> None:
    """
    Test that ensure_log_directory_exists creates a directory if it does not exist.
    """
    ensure_log_directory_exists('logs/test.log')
    mock_makedirs.assert_called_once_with('logs')

# Test to check error when log directory cannot be created
@patch('os.makedirs', side_effect=OSError("Permission denied"))
@patch('os.path.exists', return_value=False)
def test_ensure_log_directory_exists_raises_error(mock_exists: Any, mock_makedirs: Any) -> None:
    """
    Test that ensure_log_directory_exists raises LoggerSetupError if directory creation fails.
    """
    with pytest.raises(LoggerSetupError, match="Failed to create log directory"):
        ensure_log_directory_exists('logs/test.log')

# Test for creating a valid console handler
def test_create_console_handler_valid_level() -> None:
    """
    Test that create_console_handler returns a handler with the correct log level.
    """
    handler = create_console_handler('INFO')
    assert isinstance(handler, logging.StreamHandler)
    assert handler.level == logging.INFO

# Test for invalid log level in console handler creation
def test_create_console_handler_invalid_level() -> None:
    """
    Test that create_console_handler raises LoggerSetupError for an invalid log level.
    """
    with pytest.raises(LoggerSetupError, match="Invalid log level"):
        create_console_handler('INVALID_LEVEL')

@patch('src.logger.RotatingFileHandler')  # Ensure this path matches your module's import path
def test_create_file_handler_valid(mock_rotating_file_handler: Any) -> None:
    """
    Test that create_file_handler creates a valid file handler with rotation.
    """
    create_file_handler('logs/test.log', 'INFO')
    mock_rotating_file_handler.assert_called_once_with('logs/test.log', maxBytes=5*1024*1024, backupCount=5)

# Test for file handler creation failure
@patch('src.logger.RotatingFileHandler', side_effect=OSError("Permission denied"))
def test_create_file_handler_raises_error(mock_rotating_file_handler: Any) -> None:
    """
    Test that create_file_handler raises LoggerSetupError if file handler creation fails.
    """
    with pytest.raises(LoggerSetupError, match="Failed to create file handler"):
        create_file_handler('logs/test.log', 'INFO')

# Test for the full setup_logger function
@patch('src.logger.ensure_log_directory_exists')  # Correct the module path
@patch('src.logger.create_console_handler')       # Correct the module path
@patch('src.logger.create_file_handler')          # Correct the module path
def test_setup_logger(mock_file_handler: Any, mock_console_handler: Any, mock_ensure_log_directory_exists: Any) -> None:
    """
    Test the full setup_logger function to ensure it sets up the handlers and logger correctly.
    """
    mock_console = MagicMock()
    mock_file = MagicMock()
    mock_console_handler.return_value = mock_console
    mock_file_handler.return_value = mock_file

    logger = setup_logger('logs/test.log', 'INFO')

    mock_ensure_log_directory_exists.assert_called_once_with('logs/test.log')
    mock_console_handler.assert_called_once_with('INFO')
    mock_file_handler.assert_called_once_with('logs/test.log', 'INFO')

    # Check if the handlers were added to the logger
    assert mock_console in logger.handlers
    assert mock_file in logger.handlers
