import os
import sys
import io
import pandas as pd
from google.cloud import storage
from pipeline.src.utils import load_yaml
from pipeline.src.clean.cleaners import (
    EventsCleaner, 
    FighterCleaner, 
    ResultsCleaner, 
    RoundsCleaner
)
from pipeline.src.logger import setup_logger

logger = setup_logger(log_file="logs/clean.log", log_level="INFO")

CONFIG_PATH = "/app/config/config.yaml"

def upload_to_gcs(bucket_name: str, source_file: str, destination_blob_name: str) -> None:
    """
    Uploads a file from the local filesystem to the specified GCS bucket using the full path.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)
        logger.info(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"Failed to upload {source_file} to GCS: {str(e)}")
        raise

def load_and_clean_data_from_gcs(blob_name: str, cleaner_class, bucket_name: str) -> pd.DataFrame:
    """
    Downloads the JSON file from GCS, reads it into a DataFrame,
    and applies the cleaning logic from the provided cleaner_class.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        # Download the blob as text
        json_data = blob.download_as_text()
        # Load the JSON data into a DataFrame using StringIO
        df = pd.read_json(io.StringIO(json_data))
        cleaner = cleaner_class(df)
        cleaned_df = cleaner.clean()
        return cleaned_df
    except Exception as e:
        logger.error(f"Error cleaning data from {blob_name}: {str(e)}")
        raise

def run_cleaning_pipeline(config: dict) -> None:
    logger.info("Starting cleaning pipeline")
    bucket_name = config.get("gcs", {}).get("bucket")
    if not bucket_name:
        logger.error("No GCS bucket configured. Exiting cleaning pipeline.")
        sys.exit(1)

    try:
        # Download and clean each raw data file from GCS
        cleaned_data = {
            "events": load_and_clean_data_from_gcs(config['output_files']['raw']['events'], EventsCleaner, bucket_name),
            "results": load_and_clean_data_from_gcs(config['output_files']['raw']['results'], ResultsCleaner, bucket_name),
            "fighters": load_and_clean_data_from_gcs(config['output_files']['raw']['fighters'], FighterCleaner, bucket_name),
            "rounds": load_and_clean_data_from_gcs(config['output_files']['raw']['rounds'], RoundsCleaner, bucket_name),
        }
    except Exception as e:
        logger.error(f"Error during cleaning process: {str(e)}")
        sys.exit(1)

    # Save cleaned data locally as Parquet files using the configured paths
    for key, df in cleaned_data.items():
        output_path = config['output_files']['clean'][key]
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        try:
            df.to_parquet(output_path, index=False, engine='pyarrow')
            logger.info(f"Saved cleaned {key} data to {output_path}")
        except Exception as e:
            logger.error(f"Error saving cleaned {key} data: {str(e)}")
            sys.exit(1)

    # Upload the cleaned files to GCS using the full file path from config as destination blob name
    for key, local_filepath in config["output_files"]["clean"].items():
        try:
            upload_to_gcs(bucket_name, local_filepath, local_filepath)
        except Exception as e:
            logger.error(f"Failed to upload {local_filepath} to GCS: {str(e)}")

    logger.info("Cleaning pipeline completed successfully")

def main():
    if not os.path.exists(CONFIG_PATH):
        logger.error(f"Configuration file not found: {CONFIG_PATH}")
        sys.exit(1)

    config = load_yaml(CONFIG_PATH)
    logger.info("Configuration loaded successfully.")

    run_cleaning_pipeline(config)

if __name__ == '__main__':
    main()
