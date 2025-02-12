import os
import pandas as pd
import sys

# Get the absolute path of the directory containing script.py
current_dir = os.path.dirname(os.path.abspath(__file__))

# Determine the project root (assuming 'scripts' is directly under the project root)
project_root = os.path.dirname(current_dir)

# Insert the project root at the beginning of sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.scrape.utils import load_yaml
from src.clean.cleaners import (
    EventsCleaner, 
    FighterCleaner, 
    ResultsCleaner, 
    RoundsCleaner)
from src.logger import setup_logger


logger = setup_logger(log_file="logs/clean.log", log_level="INFO")


def clean_events(config: dict, root_dir: str) -> pd.DataFrame:
    """
    Load and clean the raw events data.
    """
    events_path = os.path.join(root_dir, config['output_files']['raw']['events'])
    df_events = pd.read_json(events_path)
    cleaner = EventsCleaner(df_events)
    return cleaner.clean()


def clean_results(config: dict, root_dir: str) -> pd.DataFrame:
    """
    Load and clean the raw results data.
    """
    results_path = os.path.join(root_dir, config['output_files']['raw']['results'])
    df_results = pd.read_json(results_path)
    cleaner = ResultsCleaner(df_results)
    return cleaner.clean()


def clean_fighters(config: dict, root_dir: str) -> pd.DataFrame:
    """
    Load and clean the raw fighters data.
    """
    fighters_path = os.path.join(root_dir, config['output_files']['raw']['fighters'])
    df_fighters = pd.read_json(fighters_path)
    cleaner = FighterCleaner(df_fighters)
    return cleaner.clean()


def clean_rounds(config: dict, root_dir: str) -> pd.DataFrame:
    """
    Load and clean the raw rounds data.
    """
    rounds_path = os.path.join(root_dir, config['output_files']['raw']['rounds'])
    df_rounds = pd.read_json(rounds_path)
    cleaner = RoundsCleaner(df_rounds)
    return cleaner.clean()


def run_cleaning_pipeline(config: dict, root_dir: str) -> None:
    """
    Run the complete cleaning pipeline and save the outputs as Parquet files.
    
    The pipeline:
      1. Load and clean events.
      2. Load and clean results.
      3. Load and clean fighters.
      4. Load and clean rounds.
      5. Save the cleaned DataFrames to Parquet files.
    
    It is assumed that your config contains separate entries for the raw files 
    (under config['output_files']['raw']) and the cleaned output files 
    (under config['output_files']['clean']).
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
    
    logger.info("Cleaning pipeline completed successfully")


def main():
    # Set up the logger.
    logger.info("Starting cleaning script")
    
    # Load configuration.
    try:
        config_path = os.path.join(project_root, 'config', 'config.yaml')
        config = load_yaml(config_path)
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)
    
    # Run the cleaning pipeline.
    try:
        run_cleaning_pipeline(config=config, root_dir=project_root)
    except Exception as e:
        logger.error("Cleaning pipeline failed", exc_info=True)
        sys.exit(1)
    
    logger.info("Cleaning script finished successfully")


if __name__ == '__main__':
    main()
