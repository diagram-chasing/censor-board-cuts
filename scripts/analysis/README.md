# censor-board-cuts analysis script

The analysis pipeline processes raw data (metadata and modifications) through a data cleaning and joining stage.

## Usage

To run the data cleaning and joining process:
```bash
python main.py
```

### Options

- **Force processing** even if input files haven't changed:
  ```bash
  python main.py --force
  ```

## Pipeline Workflow

- **Input Data**:
  - Raw metadata: `../../data/raw/metadata.csv`
  - Raw modifications: `../../data/raw/modifications.csv`
  - Raw categories: `../../data/raw/categories.csv`

- **Process**:
  - Cleaning operations:
    - Standardizes certificate IDs across datasets
    - Processes metadata (IDs, dates, durations)
    - Formats modification data (time formats)
    - Cleans date formats and duration values
    - Consolidates metadata within certificate IDs
    - Deduplicates modification rows
  - Integrates with categories data when available
  - Filters movies by duration (≥ 60 minutes)
  - Optimizes processing by skipping unchanged input files
  - Outputs three cleaned CSV files

- **Output Data**:
  - Cleaned modifications: `../../data/individual_files/modifications_cleaned.csv`
  - Cleaned metadata: `../../data/individual_files/metadata_cleaned.csv`
  - Complete data: `../../data/individual_files/metadata_modifications.csv`