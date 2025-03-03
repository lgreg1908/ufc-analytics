import json
from typing import List, Dict, Any, NamedTuple
import io
import yaml
import pandas as pd
from google.cloud import storage

#------File Loading------#
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
        raise FileNotFoundError(f"JSON file not found: {fnf_err}") from fnf_err

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
        raise FileNotFoundError(f"YAML file not found: {fnf_err}") from fnf_err
    except yaml.YAMLError as yaml_err:
        raise yaml.YAMLError(f"Error parsing file YAML fo;e: {yaml_err}") from yaml_err

#--------Cloud Storage--------
def upload_to_gcs(bucket_name: str, source_file: str, destination_blob_name: str) -> None:
    """
    Uploads a file from the local filesystem to the specified GCS bucket using the full path.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)
    except Exception as e:
        raise IOError(f"Failed to upload {source_file} to GCS: {str(e)}") from e

def load_json_from_gcs(blob_name: str, bucket_name: str) -> pd.DataFrame:
    """
    Downloads the JSON file from GCS and loads it into a DataFrame.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        # Download the blob as text
        json_data = blob.download_as_text()
        # Load the JSON data into a DataFrame using StringIO
        df = pd.read_json(io.StringIO(json_data))
        return df
    except Exception as e:
        raise IOError(f"Error loading data from {blob_name}: {str(e)}") from e

def load_parquet_from_gcs(blob_name: str, bucket_name: str) -> pd.DataFrame:
    """
    Downloads a Parquet file from GCS and loads it into a pandas DataFrame.
    """
    try:
        from io import BytesIO
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        # Download the blob content as bytes
        bytes_data = blob.download_as_bytes()
        # Wrap the bytes in a BytesIO buffer for pandas
        buffer = BytesIO(bytes_data)
        df = pd.read_parquet(buffer)
        return df
    except Exception as e:
        raise IOError(f"Error loading parquet data from {blob_name}: {str(e)}") from e


#-----Data Loading-----
class CleanData(NamedTuple):
    results: pd.DataFrame
    fighters: pd.DataFrame
    events: pd.DataFrame
    rounds: pd.DataFrame
    
    
def load_clean_data(config: Dict[str, str]) -> CleanData:
    """
    Loads dataframes from GCS based on the provided configuration.

    Parameters:
        config (dict): A dictionary containing configuration keys for blob names and bucket details.

    Returns:
        CleanData: A namedtuple containing the clean dataframes for results, fighters, events, and rounds.
    """
    bucket = config['gcs']['bucket']
    clean_paths = config['output_files']['clean']
    
    return CleanData(
        results=load_parquet_from_gcs(
            blob_name=clean_paths['results'],
            bucket_name=bucket
        ),
        fighters=load_parquet_from_gcs(
            blob_name=clean_paths['fighters'],
            bucket_name=bucket
        ),
        events=load_parquet_from_gcs(
            blob_name=clean_paths['events'],
            bucket_name=bucket
        ),
        rounds=load_parquet_from_gcs(
            blob_name=clean_paths['rounds'],
            bucket_name=bucket
        )
    )

