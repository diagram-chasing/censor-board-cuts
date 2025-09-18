# censor-board-cuts

*Note: Data updates after 12th June 2025 are based on certificate URLs contributed on the [CBFC Watch](https://cbfc.watch) contributions page*

Dataset and related anlysis of modifications or cuts made by the [Central Board of Film Certification (CBFC), India](https://www.cbfcindia.gov.in/).

The dataset consists of two main components:

- Raw Data: Raw category and certificate data from the CBFC website, stored in [`data/raw/`](data/raw/)
- Processed Data: Cleaned up data enhanced with code-based and LLM-based analysis of cuts, stored in [`data/data.csv`](data/data.csv)

## Preview

- [Modifications](https://flatgithub.com/diagram-chasing/censor-board-cuts?filename=data%2Fraw%2Fmodifications.csv) (~20MB)
- [Metadata](https://flatgithub.com/diagram-chasing/censor-board-cuts?filename=data%2Fraw%2Fmetadata.csv) (~40MB)
- [Categories](https://flatgithub.com/diagram-chasing/censor-board-cuts?filename=data%2Fraw%categories.csv) (~100MB)
- [Processed Dataset](https://flatgithub.com/diagram-chasing/censor-board-cuts?filename=data%data.csv) (~100MB)

Further data is available in the [data/](/data/) directory.

## Data Collection

The following scripts fetch data from the CBFC website:
- [`scripts/certificates/`](scripts/certificates/): Film metadata, modifications
- [`scripts/categories/`](scripts/categories): Film categories

The above scripts incrementally fetch new films and append them to the relevant CSV files. After fetching the data from the CBFC website, code-based analysis of the metadata and modifications is done in [`scripts/analysis/`](scripts/analysis/) and LLM-based analysis is done in [`scripts/llm/`](scripts/llm/). Next, [`scripts/imdb/`](scripts/imdb/) further enhances the metadata and all the fetched data is joined together using [`scripts/join/`](scripts/join/) which saves the final data in [`data/data.csv`](data/data.csv).

## Data Analysis

The code-based analysis is done by a Python script [`scripts/analysis/main.py`](scripts/analysis/main.py) that cleans and processes the raw data:
- Standardizes duration formats and attempts to pull out timestamps from the descriptions.
- Categorizes modifications based on type (audio, visual, deletion, etc.) and the basic type of content (violence, nudity, etc.) using an LLM.

## Explore

Explore the data on [CBFC Watch](https://cbfc.watch).

## Related Projects

- https://github.com/harman28/cbfc
- https://github.com/ananddotiyer/MovieCertifications
