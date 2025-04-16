# Processing and Analysis for Censor Board Cuts

This directory contains scripts for processing, cleaning, and analyzing our raw data.

The analysis pipeline processes raw data (metadata and modifications) through two main stages:
1. **Data Cleaning & Joining** - Cleans raw data files and joins them
2. **Description Processing** - Uses an LLM to analyze censorship descriptions and create metadata tags

## Scripts

### Main Pipeline

- **main.py** - Orchestrates the full processing pipeline
- **join_and_process.py** - Cleans and joins raw metadata and modifications data
- **process_descriptions.py** - Processes censorship descriptions using Gemini AI
- **processed_ids.log** - Tracks which descriptions have been processed

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
  ```
- A Gemini API key (for description processing)

## Installation

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install required packages:
   ```bash
   pip install pandas numpy pyarrow tqdm python-dotenv google-generativeai
   ```

3. Set up your API key:
   ```bash
   # Create .env file with your Gemini API key
   echo "GEMINI_API_KEY=your-api-key-here" > .env
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
  ```

- **Process a limited number of descriptions** (useful for testing):
  ```bash
  python main.py --limit 10
  ```

- **Rebuild log file** from existing processed data:
  ```bash
  python main.py --rebuild-log
  ```

### Individual Scripts

You can also run each script individually:

```bash
# Clean and join raw data
python join_and_process.py

# Process descriptions
python process_descriptions.py --input ../data/complete_data.csv --output ../data/processed_data.csv
```

## File Paths

The scripts use these default paths:

- **Input Data**:
  - Raw metadata: `../../data/raw/metadata.csv`
  - Raw modifications: `../../data/raw/modifications.csv`

- **Output Data**:
  - Cleaned modifications: `../../data/new_data/modifications_cleaned.csv`
  - Cleaned metadata: `../../data/new_data/metadata_cleaned.csv`
  - Complete data: `../../data/new_data/complete_data.csv`
  - Processed data: `../../data/processed_data.csv`
  - Processed IDs log: `./processed_ids.log`

## Workflow

1. **Data Joining**:
   - Cleans metadata (IDs, dates, durations)
   - Cleans modification data (certificate IDs, time formats)
   - Joins the datasets
   - Outputs cleaned CSV files

2. **Description Processing**:
   - Analyzes censorship descriptions using Gemini AI
   - Extracts:
     - Action types (deletion, muting, blurring, etc.)
     - Content types (violence, profanity, etc.)
     - Media elements (dialogue, scenes, etc.)
     - Details about each censored item
   - Maintains a log of processed IDs to enable incremental processing

## Notes

- The `processed_ids.log` file is updated after each description is processed, allowing for safe interruption and resumption.
- Use `--rebuild-log` if you have an existing processed_data.csv file but are missing the log file.
- The R script (`cleaning_script.R`) provides an alternative implementation of the data cleaning logic. 