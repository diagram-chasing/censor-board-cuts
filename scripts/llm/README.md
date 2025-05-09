# Censor Board Cuts Analysis Tool

This tool processes descriptions of censorship actions in media content using Google's Gemini AI to extract structured information about the censorship cuts. 

## Overview

The script takes descriptions of censorship actions from a CSV file, processes them through the Gemini API, and outputs a new CSV with structured analysis of each censorship action, including:

- Cleaned description (human-readable version without timestamps)
- Reference (specific content being censored)
- Action type (deletion, insertion, replacement, etc.)
- Content types (violence, sexual content, profanity, etc.)
- Media element affected (music, visual scene, text/dialogue, etc.)

## Requirements

- Python 3.6+
- Google Gemini API key (set as environment variable)
- Required packages (install via `pip install -r requirements.txt`):
  - pandas
  - google-generativeai
  - python-dotenv
  - tqdm

## Setup

1. Clone this repository
2. Create a `.env` file in the `llm/` directory with your Gemini API key:
   ```
   GEMINI_API_KEY=your_api_key_here
   ```
3. Ensure `prompt.txt` exists in the `scripts/llm/` directory (contains system instructions for the AI model)

## Usage

Run the script with:

```bash
python scripts/llm/main.py [options]
```

### Options

- `--input`: Path to input CSV file (default: `data/metadata_modifications.csv`)
- `--output`: Path to output CSV file (default: `data/data.csv`)
- `--log`: Path to file tracking processed IDs (default: `.completed.log` in script directory)
- `--limit`: Limit the number of descriptions to process (default: process all)
- `--rebuild-log`: Rebuild the processed IDs log from the output file

### Input Format

The input CSV should contain at minimum:
- `certificate_id`: Unique identifier for the certificate
- `cut_no`: Number identifying the specific cut
- `description`: Text description of the censorship action

### Output Format

The output CSV will contain all original columns plus:
- `ai_cleaned_description`: Rewritten description in clear language
- `ai_reference`: Specific content that was censored
- `ai_action`: Type of censorship action
- `ai_content_types`: Categories of content being censored
- `ai_media_element`: Media element affected by censorship

## Features

- Resume capability: Automatically skips already processed entries if interrupted
- Progress tracking with tqdm
- Detailed logging
- Configurable processing limits
- Handles various input formats and edge cases