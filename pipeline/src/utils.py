import yaml
import json
from typing import List, Dict, Any


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
        raise FileNotFoundError(f"JSON file not found: {fnf_err}")

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
        raise FileNotFoundError(f"YAML file not found: {fnf_err}")
    except yaml.YAMLError as yaml_err:
        raise yaml.YAMLError(f"Error parsing file YAML fo;e: {yaml_err}")
