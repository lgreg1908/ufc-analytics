import logging
from typing import List, TypeVar, Union, Dict, Any
from pathlib import Path
import json
from pydantic import BaseModel
import yaml

logger = logging.getLogger(__name__)
T = TypeVar('T', bound=BaseModel)


def load_json(filepath: str) -> List[Dict[str, Any]]:
    """
    Reads JSON data from a filepath.

    Args:
        filepath (str): The filepath containing the JSON data to load.
    Returns:
        List[Dict[str, Any]]: The JSON data.
        
    Raises:
        FileNotFoundError: If the file is not found.
    """
    try:
        with open(filepath, 'r') as file:
            data = json.load(file)
            return data

    except FileNotFoundError as fnf_err:
        logger.error(f"JSON file not found: {fnf_err}")
        raise

def load_yaml(yaml_path: str) -> Dict[str, Any]:
    """
    Load from a YAML file.

    Args:
        yaml_path (str): Path to the YAML  file.

    Returns:
        dict: A dictionary containing the data.

    Raises:
        FileNotFoundError: If the file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    try:
        with open(yaml_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError as fnf_err:
        logger.error(f"File not found: {fnf_err}")
        raise
    except yaml.YAMLError as yaml_err:
        logger.error(f"Error parsing file: {yaml_err}")
        raise

def ensure_directory_exists(filepath: Union[str, Path]) -> None:
    """
    Ensure the parent directory for the given file path exists. Creates directories if needed.
    
    Args:
        filepath (Union[str, Path]): The path where the file will be saved. The parent directory will be created if it doesn't exist.
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)


def convert_models_to_dicts(models: List[T]) -> List[Dict[str, Any]]:
    """
    Convert a list of Pydantic models to a list of dictionaries.

    Args:
        models (List[T]): A list of Pydantic model instances.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries where each dictionary represents a model.
    """

    return [model.model_dump() for model in models]

def save_models_to_json(models: List[BaseModel], filepath: str) -> None:
    """
    Save a list of Pydantic models to a JSON file.

    Args:
        models (List[BaseModel]): List of Pydantic model instances.
        filepath (str): The path to the JSON file where the data will be saved.
    """
    # Convert each model to a dict
    data = convert_models_to_dicts(models=models)
    
    # Ensure the directory exists
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write the list of dictionaries to the JSON file
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def get_event_urls(events_data: List[Dict[str, Any]]) -> List[str]:
    """
    Extracts the event URL from the UFC event data in JSON format

    Args:
        events_data (List[Dict[str, Any]]): The event data in JSON format.
    Returns:
        List[str]: A list of event URLs extracted from the 'event_url' field.

    Raises:
        Exception: If an unexpected error occurs.
    """
    try:
        # Extract the event URLs from the 'event_url' field
        event_urls = [event['event_url'] for event in events_data]
        logger.info(f"Successfully extracted {len(event_urls)} event URLs.")
        return event_urls

    except Exception as e:
        logger.error(f"An unexpected error occurred while extracting event URLs: {e}")
        raise


def get_fighter_urls(results_data: List[Dict[str, Any]]) -> List[str]:
    """
    Extracts the fighter URLs from UFC fight results data in JSON format.
    
    Args:
        results_data (List[Dict[str, Any]]): The path to the file containing the fight results data in JSON format.
    Returns:
        List[str]: A list of unique fighter URLs extracted from the 'fighters_urls' field.

    Raises:
        Exception: If an unexpected error occurs.
    """
    try:

        # Check if 'fighters_urls' is present in each fight result
        all_fighter_urls = []
        for fight in results_data:
            if 'fighters_urls' not in fight:
                logger.error("'fighters_urls' field is missing in the data.")
                raise ValueError("'fighters_urls' field is missing in the data.")
            # Append all fighter URLs from the current fight
            all_fighter_urls.extend(fight['fighters_urls'])

        # Get all unique fighter URLs and return them
        unique_fighter_urls = list(set(all_fighter_urls))
        logger.info(f"Successfully extracted {len(unique_fighter_urls)} unique fighter URLs.")
        return unique_fighter_urls

    except Exception as e:
        logger.error(f"An unexpected error occurred while extracting fighter URLs: {e}")
        raise


def get_fight_urls(results_data: List[Dict[str, Any]]) -> List[str]:
    """
    Extracts the fight URLs from the UFC fight results data in JSON format.
    
    Args:
        results_data (List[Dict[str, Any]]): The fight results data in JSON format.
    Returns:
        List[str]: A list of unique fight URLs extracted from the 'fight_url' field.

    Raises:
        Exception: If an unexpected error occurs.
    """
    try:
        # Check if 'fight_url' is present in each fight result
        fight_urls = []
        for fight in results_data:
            if 'fight_url' not in fight:
                logger.error("'fight_url' field is missing in the data.")
                raise ValueError("'fight_url' field is missing in the data.")
            # Append the fight URL to the list
            fight_urls.append(fight['fight_url'])

        # Get all unique fight URLs and return them
        unique_fight_urls = list(set(fight_urls))
        logger.info(f"Successfully extracted {len(unique_fight_urls)} unique fight URLs.")
        return unique_fight_urls

    except Exception as e:
        logger.error(f"An unexpected error occurred while extracting fight URLs: {e}")
        raise