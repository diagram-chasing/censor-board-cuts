# Censor Board Cuts - Data Files

This folder contains datasets related to film censorship and modifications made by the certification board.

## Main Dataset

- `data.csv`: Consolidated dataset containing film information, censorship details, and metadata. Includes certificate IDs, film names, languages, descriptions of cuts, modification times, certification dates, along with IMDb data and AI-processed content classifications.

### Data Dictionary

| Variable | Type | Description |
|----------|-----------|-------------|
| `id` | integer | Unique identifier for the certification, used in the ecinepramaan site |
| `certificate_id` | string | Unique identifier for the certification, used in the cbfcindia site |
| `movie_name` | string | Name of the film |
| `language` | string | Language of the film |
| `duration_secs` | float | Film duration in seconds |
| `description` | string | Original description of the modification |
| `cleaned_description` | string | Cleaned version of the description |
| `cut_no` | integer | Sequential number for each modification within a film |
| `deleted_secs` | float | Duration of content removed in seconds |
| `replaced_secs` | float | Duration of content replaced in seconds |
| `inserted_secs` | float | Duration of content added in seconds |
| `total_modified_time_secs` | float | Total duration affected by modifications |
| `cert_date` | date | Date of certification (may be incomplete). Does not reflect date of release. |
| `cert_no` | string | Certification number |
| `applicant` | string | Person/entity applying for certification |
| `certifier` | string | Official who approved the certification |
| `rating` | string | Film rating/certification category |
| `cbfc_file_no` | string | CBFC file number |
| `imdb_id` | string | IMDb identifier for the film search match |
| `imdb_title` | string | Film title according to IMDb search match |
| `imdb_year` | integer | Year of release according to IMDb search match |
| `imdb_genres` | string | Genres listed on IMDb search match |
| `imdb_rating` | float | IMDb search match rating |
| `imdb_votes` | integer | Number of votes on IMDb search match |
| `imdb_directors` | string | Film directors according to IMDb search match |
| `imdb_actors` | string | Main actors according to IMDb search match |
| `imdb_runtime` | integer | Runtime in minutes according to IMDb search match |
| `imdb_countries` | string | Countries of production according to IMDb search match |
| `imdb_languages` | string | Languages according to IMDb search match |
| `imdb_overview` | string | Film synopsis from IMDb search match |
| `imdb_release_date` | date | Release date according to IMDb search match |
| `imdb_writers` | string | Writers/screenwriters according to IMDb search match |
| `imdb_studios` | string | Production studios according to IMDb search match |
| `imdb_poster_url` | string | URL to search match film poster on IMDb |
| `ai_cleaned_description` | string | AI-processed version of the description |
| `ai_reference` | string | Reference to the censored content |
| `ai_action` | string | Action taken on the content |
| `ai_content_types` | string | Types of content being censored |
| `ai_media_element` | string | Specific media element being censored |

Various columns are extracted using scripts available in the [scripts/](../scripts/README.md) folder, including:
- Metadata and modifications: [`analysis/main.py`](../scripts/analysis/main.py)
- `ai_`: [`llm/main.py`](../scripts/llm/main.py)
- `imdb_`: [`imdb/main.py`](../scripts/imdb/main.py)

## Raw Data

- `metadata.csv`: Raw film metadata
- `modifications.csv`: Raw modifications data
- `categories.csv`: Raw categories data
- `imdb.csv`: Raw IMDb search match data
- `llm.csv`: Raw LLM processed modifications data
- `recent.csv`: Recent film certifications

