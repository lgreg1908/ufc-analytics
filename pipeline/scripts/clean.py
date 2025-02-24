import os
import sys
import pandas as pd
from pipeline.src.utils import load_yaml, load_json_from_gcs, upload_to_gcs
from pipeline.src.clean.cleaners import (
    EventsCleaner, 
    FighterCleaner, 
    ResultsCleaner, 
    RoundsCleaner
)
from pipeline.src.logger import setup_logger

logger = setup_logger(log_file="logs/clean.log", log_level="INFO")

CONFIG_PATH = "/app/config/config.yaml"


def apply_cleaner(df: pd.DataFrame, cleaner_class) -> pd.DataFrame:
    """
    Applies the cleaning logic provided by the cleaner_class to the DataFrame.
    """
    try:
        cleaner = cleaner_class(df)
        cleaned_df = cleaner.clean()
        logger.info(f"Applied cleaner: {cleaner_class.__name__}")
        return cleaned_df
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")
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
            "events": apply_cleaner(
                load_json_from_gcs(config['output_files']['raw']['events'], bucket_name), 
                EventsCleaner),
            "results": apply_cleaner(
                load_json_from_gcs(config['output_files']['raw']['results'], bucket_name), 
                ResultsCleaner),
            "fighters": apply_cleaner(
                load_json_from_gcs(config['output_files']['raw']['fighters'], bucket_name), 
                FighterCleaner),
            "rounds": apply_cleaner(
                load_json_from_gcs(config['output_files']['raw']['rounds'], bucket_name), 
                RoundsCleaner),
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
