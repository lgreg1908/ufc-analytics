from typing import List, Dict, NamedTuple, Optional
import pandas as pd
from pipeline.src.utils import load_parquet_from_gcs

#-----Data Loading-----
class CleanData(NamedTuple):
    results: Optional[pd.DataFrame] = None
    fighters: Optional[pd.DataFrame] = None
    events: Optional[pd.DataFrame] = None
    rounds: Optional[pd.DataFrame] = None

def load_clean_data(config: Dict[str, str], names: Optional[List[str]] = None) -> CleanData:
    """
    Loads selected dataframes from GCS based on the provided configuration.

    Parameters:
        config (Dict[str, str]): A dictionary containing configuration keys for blob names and bucket details.
        names (Optional[List[str]]): A list of dataframe names to load (e.g., ["results", "fighters"]). 
                                     If None, all available dataframes are loaded.

    Returns:
        CleanData: A NamedTuple containing the selected clean dataframes.
    """
    bucket = config['gcs']['bucket']
    clean_paths = config['output_files']['clean']

    # Filter by requested names if provided
    names_to_load = names if names is not None else clean_paths.keys()

    # Load only the requested data
    loaded_data = {
        key: load_parquet_from_gcs(blob_name=clean_paths[key], bucket_name=bucket) if key in names_to_load else None
        for key in CleanData._fields
    }

    return CleanData(**loaded_data)
