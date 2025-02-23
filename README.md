This project is designed to scrape, clean, and analyze UFC statistics. It leverages Docker and Docker Compose to containerize different components of the pipeline, making it easy to run, develop, and deploy.

## to do
- fix existing testing
- add testing for clean
- move duplicated code in scripts in centrealized location (e.g. utils)
- develop the dash app
- build out ML modules

## Overview

The project is divided into two main components:

- **Pipeline:**  
  Contains scripts for scraping raw UFC data from [UFCStats](http://ufcstats.com), cleaning the data, and uploading both raw and cleaned data to Google Cloud Storage (GCS).  
  Scripts include:
  - **dummy.py:** A test script that creates a sample DataFrame, saves it locally, and uploads it to GCS.
  - **scrape.py:** Scrapes UFC events, results, fighter, and rounds data.
  - **clean.py:** Downloads raw JSON files from GCS, cleans the data, saves it locally as Parquet files, and then uploads the cleaned files to GCS using a folder structure defined in the configuration.

## Prerequisites
- Docker: Install Docker for your operating system.
- Docker Compose: Ensure Docker Compose is installed.
- Google Cloud Credentials: Place your gcs-key.json in the project root (or update the volume paths if located elsewhere).

## Configuration
The configuration file (pipeline/config/config.yaml) defines:

- Output Files: Paths for raw, clean, and transformed data. For example:
output_files:
```yaml
  raw:
    events: "data/raw/ufc_events.json"
    results: "data/raw/ufc_results.json"
    fighters: "data/raw/ufc_fighters.json"
    rounds: "data/raw/ufc_rounds.json"
  clean:
    events: "data/clean/ufc_events_clean.parquet"
    results: "data/clean/ufc_results_clean.parquet"
    fighters: "data/clean/ufc_fighters_clean.parquet"
    rounds: "data/clean/ufc_rounds_clean.parquet"
  transformed:
    results: "data/transformed/ufc_results_transformed.parquet"
    fights: "data/transformed/ufc_fights_transformed.parquet"
event_urls:
  all: "http://ufcstats.com/statistics/events/completed?page=all"
  one: "http://ufcstats.com/statistics/events/completed?page=1"
gcs:
  bucket: ufc-analytics
```
- GCS Bucket: The name of your Google Cloud Storage bucket is specified under gcs.bucket.

## Docker Compose Setup

The project uses Docker Compose to define multiple services. An example `docker-compose.yml` is provided below:

```yaml
version: '3.8'

services:
  pipeline_scrape:
    build:
      context: ./pipeline
      dockerfile: Dockerfile
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/gcs-key.json
    volumes:
      - ./gcs-key.json:/app/gcs-key.json:ro
    command: scripts.scrape

  pipeline_dummy:
    build:
      context: ./pipeline
      dockerfile: Dockerfile
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/gcs-key.json
    volumes:
      - ./gcs-key.json:/app/gcs-key.json:ro
    command: scripts.dummy

  pipeline_clean:
    build:
      context: ./pipeline
      dockerfile: Dockerfile
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/gcs-key.json
    volumes:
      - ./gcs-key.json:/app/gcs-key.json:ro
    command: scripts.clean
```
### How to Use Docker Compose

- Build & Run All Services:
From the project root, run:

```bash docker-compose up --build```
This will build the images and start all defined services.

- Run a Specific Service:
To run a specific pipeline (e.g., the scraping pipeline), execute:

```bash docker-compose up pipeline_scrape```
Similarly, use `pipeline_dummy` or `pipeline_clean` to run the corresponding script.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
