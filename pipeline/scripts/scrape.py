import os
import sys
from pydantic import BaseModel
from typing import List
from google.cloud import storage

from pipeline.src.scrape.scrapers import (
    EventsScraper, 
    FightersScraper, 
    ResultsScraper, 
    RoundsScraper
)
from pipeline.src.scrape.utils import (
    save_models_to_json, 
    convert_models_to_dicts, 
    get_event_urls, 
    get_fight_urls, 
    get_fighter_urls
)
from pipeline.src.utils import load_yaml
from pipeline.src.logger import setup_logger


# Set up logger (writes to both console and file)
logger = setup_logger(log_file="logs/scrape.log", log_level="INFO")


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


def scrape_events(config: dict) -> List[BaseModel]:
    """Scrape events using the provided configuration."""
    try:
        logger.info("Scraping events...")
        event_scraper = EventsScraper()
        events = event_scraper.scrape_events([config['event_urls']['all']])
        logger.info(f"Scraped {len(events)} events.")
        return events
    except Exception as e:
        logger.error("Error scraping events", exc_info=True)
        raise


def extract_event_urls(events: List[BaseModel]) -> List[str]:
    """Extract event URLs from the list of event models."""
    try:
        urls = get_event_urls(events_data=convert_models_to_dicts(models=events))
        logger.info(f"Extracted {len(urls)} event URLs.")
        return urls
    except Exception as e:
        logger.error("Error extracting event URLs", exc_info=True)
        raise


def scrape_results(event_urls: List[str]) -> List[BaseModel]:
    """Scrape fight results using event URLs."""
    try:
        logger.info("Scraping fight results...")
        result_scraper = ResultsScraper()
        results = result_scraper.scrape_results(event_urls=event_urls)
        logger.info(f"Scraped {len(results)} fight results.")
        return results
    except Exception as e:
        logger.error("Error scraping results", exc_info=True)
        raise


def extract_fight_urls(results: List[BaseModel]) -> List[str]:
    """Extract fight URLs from the results models."""
    try:
        urls = get_fight_urls(results_data=convert_models_to_dicts(models=results))
        logger.info(f"Extracted {len(urls)} fight URLs.")
        return urls
    except Exception as e:
        logger.error("Error extracting fight URLs", exc_info=True)
        raise


def scrape_rounds(fight_urls: List[str]) -> List[BaseModel]:
    """Scrape rounds data using fight URLs."""
    try:
        logger.info("Scraping rounds data...")
        rounds_scraper = RoundsScraper()
        rounds = rounds_scraper.scrape_rounds(fight_urls=fight_urls)
        logger.info(f"Scraped {len(rounds)} rounds records.")
        return rounds
    except Exception as e:
        logger.error("Error scraping rounds", exc_info=True)
        raise


def extract_fighter_urls(results: List[BaseModel]) -> List[str]:
    """Extract fighter URLs from the results models."""
    try:
        urls = get_fighter_urls(results_data=convert_models_to_dicts(models=results))
        logger.info(f"Extracted {len(urls)} fighter URLs.")
        return urls
    except Exception as e:
        logger.error("Error extracting fighter URLs", exc_info=True)
        raise


def scrape_fighters(fighter_urls: List[str]) -> List[BaseModel]:
    """Scrape fighter details using fighter URLs."""
    try:
        logger.info("Scraping fighter details...")
        fighters_scraper = FightersScraper()
        fighters = fighters_scraper.scrape_fighters(fighter_urls=fighter_urls)
        logger.info(f"Scraped {len(fighters)} fighters.")
        return fighters
    except Exception as e:
        logger.error("Error scraping fighters", exc_info=True)
        raise


def run_pipeline(config: dict, root_dir: str) -> None:
    """
    Run the complete scraping pipeline and save the outputs to JSON files.
    Pipeline steps:
      1. Scrape events and extract event URLs.
      2. Scrape results based on event URLs and extract fight URLs.
      3. Scrape rounds using fight URLs.
      4. Extract fighter URLs from results and scrape fighters.
      5. Save all scraped models to their respective JSON files and upload them to GCS.
    """
    try:
        # Step 1: Events
        events = scrape_events(config)
        event_urls = extract_event_urls(events)
        
        # Step 2: Results
        results = scrape_results(event_urls)
        fight_urls = extract_fight_urls(results)
        
        # Step 3: Rounds
        rounds = scrape_rounds(fight_urls)
        
        # Step 4: Fighters
        fighter_urls = extract_fighter_urls(results)
        fighters = scrape_fighters(fighter_urls)
        
        # Step 5: Save models to JSON files
        models_to_save = {
            'events': events,
            'results': results,
            'rounds': rounds,
            'fighters': fighters,
        }
        
        for key, models in models_to_save.items():
            filepath = os.path.join(root_dir, config['output_files']['raw'][key])
            save_models_to_json(models=models, filepath=filepath)
            logger.info(f"Saved {key} data to {filepath}")
            
            # Upload file to GCS if configured
            bucket_name = config.get('gcs', {}).get('bucket')
            if bucket_name:
                upload_to_gcs(bucket_name, filepath, config['output_files']['raw'][key])
            else:
                logger.warning("No GCS bucket configured. Skipping upload for " + key)
    except Exception as e:
        logger.error("Scraping pipeline failed", exc_info=True)
        sys.exit(1)
    
    logger.info("Scraping pipeline completed successfully")


def main():
    try:
        root_dir = os.path.abspath('.')
        config_path = os.path.join(root_dir, 'config', 'config.yaml')
        config = load_yaml(config_path)
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)
    
    run_pipeline(config, root_dir)


if __name__ == '__main__':
    main()
