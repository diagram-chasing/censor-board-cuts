# censor-board-cuts scripts

Data pipeline scripts for collecting and analyzing modifications or cuts made by the [Central Board of Film Certification (CBFC), India](https://www.cbfcindia.gov.in/).

The `main.py` script serves as the central entry point for running the complete data pipeline. It orchestrates the execution of multiple processing stages in the correct sequence.

## Usage

- Install dependencies
```bash
pip install -r requirements.txt
```

- Run data pipeline
```bash
python main.py [options]
```

## Options

- `--skip-categories`: Skip the categories data collection stage (optional)
- `--skip-certificates`: Skip the certificates data collection stage (optional)  
- `--skip-processing`: Skip the data processing and analysis stage (optional)
- `--skip-imdb`: Skip the IMDB data collection stage (optional)
- `--skip-llm`: Skip the LLM analysis stage (optional)
- `--skip-join`: Skip the dataset join stage (optional)
- `--force-processing`: Force the data processing to run even if input files have not changed (optional)

## Pipeline Stages

The pipeline consists of six main stages that run sequentially:

1. **Categories** (`categories/main.py`): Collects category data for film classifications
2. **Certificates** (`certificates/main.py`): Collects certification data for films
3. **Data Processing** (`analysis/main.py`): Code-based analysis of the collected data
4. **IMDB** (`imdb/main.py`): Collects additional film metadata from IMDB
5. **LLM** (`llm/main.py`): LLM-based analysis of the cuts of modifications
6. **Join** (`join/main.py`): Joins and combines all collected and processed datasets

## AI Declaration

Components of this repository, including code and documentation, were written with assistance from Claude AI.