# CBFC Data Collection and Analysis

Collect and analyze film certification data from the Central Board of Film Certification (CBFC) in India.

This project consists of two main components:

- Data Collection: A Python-based scraper that collects film certification data from the CBFC website
- Analysis: R scripts for cleaning and analyzing the collected data

## Data Collection

The scraper (data-scripts/scrape/) collects:

- Film metadata (title, language, duration, etc.)
- Certification details
- Content modifications/cuts

More information is available in the [scrape/README.md](data-scripts/scrape/README.md) file. Each row is incrementally saved to the relevant CSV files and the scraper can resume from the last processed ID (the methodology is described in the README).

## Analysis

The R scripts (data-scripts/analysis/) clean and process the raw data:

- Standardizes duration formats and attempts to pull out timestamps from the descriptions.
- Categorizes modifications based on type (audio, visual, deletion, etc.) and the basic type of content (violence, nudity, etc.).

TODO:

- [ ] Fetching IDs for a specific state+year.
- [ ] Classifying types of media (movies, trailers, songs, etc.).
- [ ] Cleaner descriptions of modifications.
- [ ] Better content classification.
- [ ] Create summary statistics and visualizations.
- [ ] Create a data dictionary documenting all fields.
- [ ] Create a dashboard for exploring the data.
