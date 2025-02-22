import os
import pandas as pd
import sys

from pipeline.src.scrape.utils import load_yaml
from pipeline.src.transform.transformers import ResultsTransformer, FightTransformer
from pipeline.src.logger import setup_logger


logger = setup_logger(log_file="logs/transform.log", log_level="INFO")


def load_clean_data(config: dict, root_dir: str) -> dict:
    """
    Load all cleaned data files (as Parquet) from disk and return a dictionary
    containing the DataFrames.
    """
    data = {
        'events': pd.read_parquet(
            os.path.join(root_dir, config['output_files']['clean']['events']),
            engine='pyarrow'
        ),
        'results': pd.read_parquet(
            os.path.join(root_dir, config['output_files']['clean']['results']),
            engine='pyarrow'
        ),
        'fighters': pd.read_parquet(
            os.path.join(root_dir, config['output_files']['clean']['fighters']),
            engine='pyarrow'
        ),
        'rounds': pd.read_parquet(
            os.path.join(root_dir, config['output_files']['clean']['rounds']),
            engine='pyarrow'
        )
    }
    return data


def transform_results_data(config: dict, root_dir: str) -> pd.DataFrame:
    """
    Load cleaned results (plus events and fighters data), run the results transformer,
    and return the transformed results DataFrame.
    """
    data = load_clean_data(config, root_dir)
    transformer = ResultsTransformer(data['results'])
    transformed_results = transformer.transform(
        events_df=data['events'],
        fighter_df=data['fighters']
    )
    return transformed_results


def transform_fight_data(config: dict, root_dir: str) -> pd.DataFrame:
    """
    Load cleaned rounds (plus events, results, and fighters data), run the fight transformer,
    and return the transformed fight-level DataFrame.
    """
    data = load_clean_data(config, root_dir)
    transformer = FightTransformer(data['rounds'])
    transformed_fights = transformer.transform(
        events_df=data['events'],
        results_df=data['results'],
        fighter_df=data['fighters']
    )
    return transformed_fights


def run_transformation_pipeline(config: dict, root_dir: str) -> None:
    """
    Run the complete transformation pipeline and save the transformed results and fight-level data
    as Parquet files.
    """
    logger.info("Starting transformation pipeline")
    try:
        transformed_results = transform_results_data(config, root_dir)
        transformed_fights = transform_fight_data(config, root_dir)
    except Exception as e:
        logger.error("Error during transformation pipeline", exc_info=True)
        sys.exit(1)

    results_output = os.path.join(root_dir, config['output_files']['transformed']['results'])
    fights_output = os.path.join(root_dir, config['output_files']['transformed']['fights'])

    try:
        transformed_results.to_parquet(results_output, index=False, engine='pyarrow')
        transformed_fights.to_parquet(fights_output, index=False, engine='pyarrow')
    except Exception as e:
        logger.error("Error saving transformed files", exc_info=True)
        sys.exit(1)

    logger.info("Transformation pipeline completed successfully")


def main():
    # Set up logging (both console and file) using our custom logger.
    logger.info("Starting transformation script")

    # Load configuration.
    try:
        config_path = os.path.join(project_root, 'config', 'config.yaml')
        config = load_yaml(config_path)
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)

    # Run the transformation pipeline.
    try:
        run_transformation_pipeline(config, project_root)
    except Exception as e:
        logger.error("Transformation pipeline failed", exc_info=True)
        sys.exit(1)

    logger.info("Transformation script finished successfully")


if __name__ == '__main__':
    main()
