import logging
import os
from logging.handlers import RotatingFileHandler

class LoggerSetupError(Exception):
    """Custom exception for logger setup errors."""
    pass

def ensure_log_directory_exists(log_file: str):
    """
    Ensures the log directory exists, creates it if necessary.
    
    Args:
        log_file (str): Path to the log file.

    Raises:
        LoggerSetupError: If the directory cannot be created.
    """
    log_dir = os.path.dirname(log_file)
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    except OSError as e:
        raise LoggerSetupError(f"Failed to create log directory '{log_dir}': {e}") from e

def create_console_handler(log_level: str):
    """
    Creates a logging handler for console output.

    Args:
        log_level (str): Log level as a string (DEBUG, INFO, WARNING, ERROR).

    Returns:
        console_handler: Configured console logging handler.

    Raises:
        LoggerSetupError: If the log level is invalid.
    """
    try:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        return console_handler
    except ValueError as e:
        raise LoggerSetupError(f"Invalid log level for console handler: {e}") from e

def create_file_handler(log_file: str, log_level: str):
    """
    Creates a logging handler for file output with rotation.

    Args:
        log_file (str): Path to the log file.
        log_level (str): Log level as a string (DEBUG, INFO, WARNING, ERROR).

    Returns:
        file_handler: Configured file logging handler with rotation.

    Raises:
        LoggerSetupError: If the file handler cannot be created.
    """
    try:
        file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)  # 5 MB per file, 5 backups
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        return file_handler
    except (OSError, ValueError) as e:
        raise LoggerSetupError(f"Failed to create file handler for '{log_file}': {e}") from e

def setup_logger(log_file: str = "logs/app.log", log_level: str = "INFO"):
    """
    Sets up the logger to log both to the console and to a file with rotation.

    Args:
        log_file (str): Path to the log file.
        log_level (str): Log level as a string (DEBUG, INFO, WARNING, ERROR).
    
    Returns:
        logger: Configured logger with handlers.

    Raises:
        LoggerSetupError: If any part of the logger setup fails.
    """
    try:
        ensure_log_directory_exists(log_file)
    except LoggerSetupError as e:
        print(f"Error during logger setup: {e}")
        raise

    # Create logger
    logger = logging.getLogger()

    try:
        logger.setLevel(log_level)
    except ValueError as e:
        raise LoggerSetupError(f"Invalid log level '{log_level}': {e}") from e

    try:
        # Add handlers
        logger.addHandler(create_console_handler(log_level))
        logger.addHandler(create_file_handler(log_file, log_level))
    except LoggerSetupError as e:
        print(f"Error during handler setup: {e}")
        raise

    return logger
