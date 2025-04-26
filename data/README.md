# Censor Board Cuts - Data Files

This folder contains datasets related to film censorship and modifications made by the certification board.

## Main Dataset

- `data.csv`: Consolidated dataset containing film information, censorship details, and metadata. Includes certificate IDs, film names, languages, descriptions of cuts, modification times, certification dates, and classified content categories.

### Data Dictionary

| Variable | Type | Description |
|----------|-----------|-------------|
| `certificate_id` | string | Unique identifier for the certification, used in the ecinepramaan site |
| `film_name` | string | Short name of the film |
| `film_name_full` | string | Complete film name with language and format details |
| `language` | string | Language of the film |
| `duration_mins` | float | Film duration in minutes |
| `description` | string | Original description of the modification |
| `cut_no` | integer | Sequential number for each modification within a film |
| `deleted_secs_` | float | Duration of content removed in minutes |
| `replaced_secs_` | float | Duration of content replaced in minutes |
| `inserted_secs_` | float | Duration of content added in minutes |
| `total_modified_time_secs` | float | Total duration affected by modifications |
| `cert_date` | date | Date of certification (maybe be incomplete). Does not reflect date of release. |
| `cert_no` | string | Certification number |
| `applicant` | string | Person/entity applying for certification |
| `certifier` | string | Official who approved the certification |
| `ai_cleaned_description` | string | Processed version of the description |
| `ai_action_types` | string | Categorized type of action (deletion, insertion, etc.) |
| `ai_content_types` | string | Categorized content type (violence, language, etc.) |
| `ai_media_elements` | string | Type of media element modified (dialogue, scene, etc.) |
| `ai_reason` | string | Reason for modification |
| `censored_item_index` | integer | Index of censored item |
| `censored_content` | string | Description of specific content censored |
| `censored_reference` | string | Reference to the censored content |
| `censored_action` | string | Action taken on the content |
| `censored_content_types` | string | Types of content being censored |
| `censored_media_element` | string | Specific media element being censored |
| `censored_replacement` | string | What was used to replace censored content (if applicable) |
| `rating` | string | Film rating/certification category |
| `cbfc_file_no` | string | CBFC file number |


We use an LLM to extract metadata (such as the `ai_` columns). The prompt and scripts are available in [the analysis folder](../scripts/analysis/README.md).

## Subfolders

### individual_files
Individual components of the main dataset:
- `metadata_modifications.csv`: Links metadata with specific modifications
- `metadata_cleaned.csv`: Cleaned version of film metadata 
- `modifications_cleaned.csv`: Cleaned version of modification details

### site_data
Data prepared for website:
- `recent_movies.csv`: Dataset of recent films and their certification details. Used on the explorer site. 
- `metadata_modifications.parquet`: Optimized metadata-modifications in Parquet format

### raw
Original unprocessed data:
- `metadata.csv`: Raw film metadata
- `modifications.csv`: Raw modifications data

