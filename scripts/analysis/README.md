# Processing and Analysis for Censor Board Cuts

This directory contains scripts for processing, cleaning, and analyzing our raw data.

The analysis pipeline processes raw data (metadata and modifications) through a data cleaning and joining stage.

## Scripts

### Main Pipeline

- **main.py** - Handles data cleaning and joining of metadata and modifications

## Prerequisites

- Python 3.9+
- Required Python packages:
  ```
  pandas
  numpy
  ```

## Installation

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install required packages:
   ```bash
   pip install pandas numpy
   ```

## Usage

### Running the Cleaning Script

To run the data cleaning and joining process:
```bash
python main.py
```

### Options

- **Force processing** even if input files haven't changed:
  ```bash
  python main.py --force
  ```

## File Paths

The script uses these default paths:

- **Input Data**:
  - Raw metadata: `../../data/raw/metadata.csv`
  - Raw modifications: `../../data/raw/modifications.csv`
  - Raw categories: `../../data/raw/categories.csv`

- **Output Data**:
  - Cleaned modifications: `../../data/individual_files/modifications_cleaned.csv`
  - Cleaned metadata: `../../data/individual_files/metadata_cleaned.csv`
  - Complete data: `../../data/individual_files/metadata_modifications.csv`

## Workflow

**Data Cleaning and Joining**:
   - Cleans metadata (IDs, dates, durations)
   - Cleans modification data (certificate IDs, time formats)
   - Joins the datasets
   - Outputs cleaned CSV files
   - *Automatically skips processing if input files haven't changed*

## Features

- Automatic detection of unchanged input files to skip unnecessary processing
- Standardization of certificate IDs across datasets
- Cleaning of date formats, durations, and time values
- Consolidation of metadata within certificate IDs
- Deduplication of modification rows
- Integration with categories data if available
- Filtering by movie duration (≥ 60 minutes) 