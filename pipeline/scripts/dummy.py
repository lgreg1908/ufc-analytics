import os
import pandas as pd
from pipeline.src.utils import load_yaml, upload_to_gcs


def create_sample_df() -> pd.DataFrame:
    """Create a simple sample DataFrame."""
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.3, 30.7]
    }
    return pd.DataFrame(data)


def save_to_parquet(df: pd.DataFrame, filepath: str) -> None:
    """Save DataFrame to a Parquet file."""
    df.to_parquet(filepath, index=False, engine='pyarrow')
    print(f"Data saved locally to {filepath}")


def main():
    """Main function to run the script."""
    # Load absolute path for config.yaml
    CONFIG_PATH = "/app/config/config.yaml"
    
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_PATH}")

    config = load_yaml(CONFIG_PATH)

    # Create and save DataFrame
    df = create_sample_df()
    OUTPUT_PATH = "/app/sample2.parquet" 
    save_to_parquet(df, OUTPUT_PATH)

    # Upload to GCS
    bucket_name = config.get("gcs", {}).get("bucket")
    if not bucket_name:
        print("No GCS bucket configured. Skipping upload.")
        return

    upload_to_gcs(bucket_name, OUTPUT_PATH, "sample2.parquet")


if __name__ == "__main__":
    main()
