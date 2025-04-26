# CBFC Data Collection and Analysis

Collect and analyze film certification data from the Central Board of Film Certification (CBFC) in India.

This project consists of two main components:

- Data Collection: A Python-based scraper that collects film certification data from the CBFC website
- Analysis: Scripts for cleaning and analyzing the collected data.

## Preview Data

- [Modifications](https://flatgithub.com/diagram-chasing/censor-board-cuts?filename=data%2Fmodifications.csv&sha=master)
- [Certifications](https://flatgithub.com/diagram-chasing/censor-board-cuts?filename=data%2Fmetadata.csv&sha=master)

More available in the [data folder](/data/).

## Data Collection

The scraper (data-scripts/certificates/) collects:

- Film metadata (title, language, duration, etc.)
- Certification details
- Content modifications/cuts

More information is available in the [certificates/README.md](data-scripts/certificates/README.md) file. Each row is incrementally saved to the relevant CSV files and the scraper can resume from the last processed ID (the methodology is described in the README).

## Analysis

Python scripts to (data-scripts/analysis/) clean and process the raw data:

- Standardizes duration formats and attempts to pull out timestamps from the descriptions.
- Categorizes modifications based on type (audio, visual, deletion, etc.) and the basic type of content (violence, nudity, etc.) using an LLM.

## TODO:

- [ ] [Fetching IDs for a specific state+year](../../issues/1)
- [ ] [Classifying types of media (movies, trailers, songs, etc.)](../../issues/2)
- [ ] [Cleaner descriptions of modifications](../../issues/3)
- [ ] [Create summary statistics and visualizations](../../issues/4)
- [ ] Better content classification.
- [ ] Create a data dictionary documenting all fields.
- [ ] Create a dashboard for exploring the data.

## Related projects:

- https://github.com/harman28/cbfc
- https://github.com/ananddotiyer/MovieCertifications
