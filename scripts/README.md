# scripts

This directory contains a data pipeline for collecting and analyzing CBFC data.

## Structure

The `main.py` script serves as the central entry point for running the complete data pipeline. It orchestrates the execution of multiple processing stages in the correct sequence.

### Usage

- Install dependencies
```bash
pip install -r requirements.txt
```

- Run data pipeline
```bash
python main.py [options]
```

### Options

- `--skip-categories`: Skip the categories data collection stage (optional)
- `--skip-certificates`: Skip the certificates data collection stage (optional)  
- `--skip-processing`: Skip the data processing and analysis stage (optional)
- `--force-processing`: Force the data processing even if input files have not changed (optional)

### Pipeline Stages

The pipeline consists of three main stages that run sequentially:

1. **Categories** (`categories/main.py`): Collects category data for film classifications
2. **Certificates** (`certificates/main.py`): Collects certification data for films
3. **Data Processing** (`analysis/join_and_process.py`): Joins and processes the collected data for analysis

## AI Declaration

Components of this repository, including code and documentation, were written with assistance from Claude AI.