# At the very top, adjust sys.path so that modules under src can be imported.
import os
import sys
import pandas as pd
from google.cloud import storage  # Ensure you have installed google-cloud-storage
from pipeline.src.scrape.utils import load_yaml
from pipeline.src.clean.cleaners import (
    EventsCleaner, 
    FighterCleaner, 
    ResultsCleaner, 
    RoundsCleaner)
from pipeline.src.logger import setup_logger

logger = setup_logger(log_file="logs/clean.log", log_level="INFO")


def upload_to_gcs(bucket_name: str, source_file: str, destination_blob_name: str) -> None:
    """
    Uploads a file to the specified Google Cloud Storage bucket.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file)
        logger.info(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error("Failed to upload to GCS", exc_info=True)
        raise


def clean_events(config: dict, root_dir: str) -> pd.DataFrame:
    """Load and clean the raw events data."""
    events_path = os.path.join(root_dir, config['output_files']['raw']['events'])
    df_events = pd.read_json(events_path)
    cleaner = EventsCleaner(df_events)
    return cleaner.clean()


def clean_results(config: dict, root_dir: str) -> pd.DataFrame:
    """Load and clean the raw results data."""
    results_path = os.path.join(root_dir, config['output_files']['raw']['results'])
    df_results = pd.read_json(results_path)
    cleaner = ResultsCleaner(df_results)
    return cleaner.clean()


def clean_fighters(config: dict, root_dir: str) -> pd.DataFrame:
    """Load and clean the raw fighters data."""
    fighters_path = os.path.join(root_dir, config['output_files']['raw']['fighters'])
    df_fighters = pd.read_json(fighters_path)
    cleaner = FighterCleaner(df_fighters)
    return cleaner.clean()


def clean_rounds(config: dict, root_dir: str) -> pd.DataFrame:
    """Load and clean the raw rounds data."""
    rounds_path = os.path.join(root_dir, config['output_files']['raw']['rounds'])
    df_rounds = pd.read_json(rounds_path)
    cleaner = RoundsCleaner(df_rounds)
    return cleaner.clean()


def run_cleaning_pipeline(config: dict, root_dir: str) -> None:
    """
    Run the complete cleaning pipeline, save outputs as Parquet files, and upload to GCS if configured.
    """
    logger.info("Starting cleaning pipeline")
    
    try:
        cleaned_events_df   = clean_events(config, root_dir)
        cleaned_results_df  = clean_results(config, root_dir)
        cleaned_fighters_df = clean_fighters(config, root_dir)
        cleaned_rounds_df   = clean_rounds(config, root_dir)
    except Exception as e:
        logger.error("Error during cleaning process", exc_info=True)
        sys.exit(1)
    
    # Define output file paths for cleaned data.
    events_output   = os.path.join(root_dir, config['output_files']['clean']['events'])
    results_output  = os.path.join(root_dir, config['output_files']['clean']['results'])
    fighters_output = os.path.join(root_dir, config['output_files']['clean']['fighters'])
    rounds_output   = os.path.join(root_dir, config['output_files']['clean']['rounds'])
    
    try:
        cleaned_events_df.to_parquet(events_output, index=False, engine='pyarrow')
        cleaned_results_df.to_parquet(results_output, index=False, engine='pyarrow')
        cleaned_fighters_df.to_parquet(fighters_output, index=False, engine='pyarrow')
        cleaned_rounds_df.to_parquet(rounds_output, index=False, engine='pyarrow')
    except Exception as e:
        logger.error("Error saving cleaned files", exc_info=True)
        sys.exit(1)
    
    logger.info("Cleaned files saved locally successfully")
    
    # Upload cleaned files to GCS if a bucket is configured.
    bucket_name = config.get('gcs', {}).get('bucket')
    if bucket_name:
        files_to_upload = {
            config['output_files']['clean']['events']: events_output,
            config['output_files']['clean']['results']: results_output,
            config['output_files']['clean']['fighters']: fighters_output,
            config['output_files']['clean']['rounds']: rounds_output,
        }
        for destination_blob, local_filepath in files_to_upload.items():
            try:
                upload_to_gcs(bucket_name, local_filepath, destination_blob)
            except Exception as e:
                logger.error(f"Failed to upload {local_filepath} to GCS", exc_info=True)
    else:
        logger.warning("No GCS bucket configured. Skipping GCS upload.")
    
    logger.info("Cleaning pipeline completed successfully")


def main():
    logger.info("Starting cleaning script")
    try:
        config_path = os.path.join(project_root, 'config', 'config.yaml')
        config = load_yaml(config_path)
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)
    
    try:
        run_cleaning_pipeline(config=config, root_dir=project_root)
    except Exception as e:
        logger.error("Cleaning pipeline failed", exc_info=True)
        sys.exit(1)
    
    logger.info("Cleaning script finished successfully")


if __name__ == '__main__':
    main()