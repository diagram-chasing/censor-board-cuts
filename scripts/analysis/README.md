# Processing and Analysis for Censor Board Cuts

This directory contains scripts for processing, cleaning, and analyzing our raw data.

The analysis pipeline processes raw data (metadata and modifications) through three main stages:
1. **Data Cleaning & Joining** - Cleans raw data files and joins them
2. **Description Processing** - Uses an LLM to analyze censorship descriptions and create metadata tags
3. **TMDB Enrichment** - Adds movie metadata from The Movie Database (TMDB) API

## Scripts

### Main Pipeline

- **main.py** - Orchestrates the full processing pipeline
- **join_and_process.py** - Cleans and joins raw metadata and modifications data
- **process_descriptions.py** - Processes censorship descriptions using Gemini AI
- **add_tmdb_info.py** - Enriches movie data with TMDB API information
- **processed_ids.log** - Tracks which descriptions have been processed
- **tmdb_processed_ids.log** - Tracks which movies have been processed with TMDB

Also included are our older manual R scripts that do the same processing and tagging based on keywords and regex searches in the description content. These are in the `manual-scripts` folder.

## Prerequisites

- Python 3.9+
- Required Python packages:
  ```
  pandas
  numpy
  pyarrow
  tqdm
  python-dotenv
  google-generativeai
  requests
  ```
- A Gemini API key (for description processing)
- A TMDB API key (for movie metadata addition)

## Installation

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install required packages:
   ```bash
   pip install pandas numpy pyarrow tqdm python-dotenv google-generativeai requests
   ```

3. Set up your API keys:
   ```bash
   # Create .env file with your API keys
   echo "GEMINI_API_KEY=your-gemini-api-key-here" > .env
   echo "TMDB_API_KEY=your-tmdb-api-key-here" >> .env
   ```

## Usage

### Full Pipeline

To run the complete pipeline:
```bash
python main.py
```

### Options

- **Skip steps**:
  ```bash
  python main.py --skip-join        # Skip data cleaning & joining
  python main.py --skip-process     # Skip description processing
  python main.py --skip-tmdb        # Skip TMDB addition
  ```

- **Process a limited number of items** (useful for testing):
  ```bash
  python main.py --limit 10
  ```

- **Rebuild log file** from existing processed data:
  ```bash
  python main.py --rebuild-log
  ```

- **Force processing** even if input files haven't changed:
  ```bash
  python main.py --force            # Force full processing
  python join_and_process.py --force # Force just the join step
  python add_tmdb_info.py --force  # Force TMDB addition
  ```

### Individual Scripts

You can also run each script individually:

```bash
# Clean and join raw data
python join_and_process.py

# Process descriptions
python process_descriptions.py --input ../data/metadata_modifications.csv --output ../data/data.csv

# Enrich with TMDB
python add_tmdb_info.py --input ../data/data.csv --output ../data/tmdb_meta.csv
```

## File Paths

The scripts use these default paths:

- **Input Data**:
  - Raw metadata: `../../data/raw/metadata.csv`
  - Raw modifications: `../../data/raw/modifications.csv`

- **Output Data**:
  - Cleaned modifications: `../../data/new_data/modifications_cleaned.csv`
  - Cleaned metadata: `../../data/new_data/metadata_cleaned.csv`
  - Complete data: `../../data/new_data/metadata_modifications.csv`
  - Processed data: `../../data/data.csv`
  - TMDB metadata: `../../data/tmdb_meta.csv`
  - Processed IDs log: `./processed_ids.log`
  - TMDB processed IDs log: `./tmdb_processed_ids.log`

## Workflow

1. **Data Joining**:
   - Cleans metadata (IDs, dates, durations)
   - Cleans modification data (certificate IDs, time formats)
   - Joins the datasets
   - Outputs cleaned CSV files
   - *Automatically skips processing if input files haven't changed*

2. **Description Processing**:
   - Analyzes censorship descriptions using Gemini AI
   - Extracts:
     - Action types (deletion, muting, blurring, etc.)
     - Content types (violence, profanity, etc.)
     - Media elements (dialogue, scenes, etc.)
     - Details about each censored item
   - Maintains a log of processed IDs to enable incremental processing

3. **TMDB Enrichment**:
   - Queries The Movie Database API for each unique movie
   - Extracts:
     - Movie IDs, titles (original and localized)
     - Overview/plot description
     - Release date, popularity, and rating information
     - Genre IDs
     - Poster and backdrop paths
   - Stores the raw API response for future reference
   - Creates a separate `tmdb_meta.csv` file with certificate_id as the key

## Notes

- The `processed_ids.log` and `tmdb_processed_ids.log` files are updated after each item is processed, allowing for safe interruption and resumption.
- The TMDB addition script includes robust retry logic with exponential backoff for API connection issues.
- Use `--rebuild-log` if you have existing processed data files but are missing the log files.
- The `join_and_process.py` script uses file hashing to detect changes in input data files and skips processing if nothing has changed. Use `--force` to override this behavior.
- The R script (`cleaning_script.R`) provides an alternative implementation of the data cleaning logic. 