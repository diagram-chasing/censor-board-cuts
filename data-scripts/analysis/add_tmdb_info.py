#!/usr/bin/env python3
import os
import sys
import pandas as pd
import requests
import json
import time
import logging
import argparse
import random
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# TMDB API configuration
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_API_BASE_URL = "https://api.themoviedb.org/3"
TMDB_SEARCH_ENDPOINT = f"{TMDB_API_BASE_URL}/search/movie"

# Rate limiting parameters - reduced to avoid connection resets
REQUESTS_PER_SECOND = 1  # More conservative rate limit
REQUEST_DELAY = 1.0 / REQUESTS_PER_SECOND
MAX_RETRIES = 3  # Maximum number of retry attempts

def search_movie(title, year=None, language=None):
    """
    Search for a movie on TMDB API with retry logic
    
    Args:
        title (str): Movie title to search for
        year (str, optional): Release year to help narrow search
        language (str, optional): Language code to help narrow search
        
    Returns:
        dict: First matching movie result or None if no match found
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_API_KEY}"
    }
    
    # Prepare query parameters
    params = {
        "query": title,
        "include_adult": "false",
        "language": "en-US",
        "page": 1
    }
    
    # Add year if provided
    if year and str(year).isdigit():
        params["year"] = year
    
    # Implement retry logic with exponential backoff
    retry_count = 0
    while retry_count <= MAX_RETRIES:
        try:
            # Make the API request
            response = requests.get(TMDB_SEARCH_ENDPOINT, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            
            # Parse the response
            data = response.json()
            
            if data.get("results") and len(data["results"]) > 0:
                # Return the first match (most relevant)
                return data["results"][0]
            else:
                return None
                
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count <= MAX_RETRIES:
                # Calculate exponential backoff time with jitter
                backoff_time = min(2 ** retry_count, 10) + random.uniform(0, 1)
                logger.warning(f"API request failed for '{title}'. Retrying in {backoff_time:.2f}s... (Attempt {retry_count}/{MAX_RETRIES})")
                time.sleep(backoff_time)
            else:
                logger.error(f"Error searching for movie '{title}' after {MAX_RETRIES} retries: {e}")
                return None

def extract_tmdb_data(movie_data):
    """
    Extract relevant fields from TMDB movie data
    
    Args:
        movie_data (dict): Movie data from TMDB API
        
    Returns:
        dict: Dictionary with extracted fields
    """
    if not movie_data:
        return {
            "tmdb_id": None,
            "tmdb_title": None,
            "tmdb_original_title": None,
            "tmdb_original_language": None,
            "tmdb_overview": None,
            "tmdb_release_date": None,
            "tmdb_popularity": None,
            "tmdb_vote_average": None,
            "tmdb_vote_count": None,
            "tmdb_genre_ids": None,
            "tmdb_poster_path": None,
            "tmdb_backdrop_path": None
        }
    
    # Extract genre IDs as a comma-separated string
    genre_ids = ','.join(map(str, movie_data.get("genre_ids", []))) if movie_data.get("genre_ids") else None
    
    return {
        "tmdb_id": movie_data.get("id"),
        "tmdb_title": movie_data.get("title"),
        "tmdb_original_title": movie_data.get("original_title"),
        "tmdb_original_language": movie_data.get("original_language"),
        "tmdb_overview": movie_data.get("overview"),
        "tmdb_release_date": movie_data.get("release_date"),
        "tmdb_popularity": movie_data.get("popularity"),
        "tmdb_vote_average": movie_data.get("vote_average"),
        "tmdb_vote_count": movie_data.get("vote_count"),
        "tmdb_genre_ids": genre_ids,
        "tmdb_poster_path": movie_data.get("poster_path"),
        "tmdb_backdrop_path": movie_data.get("backdrop_path")
    }

def get_processed_ids(processed_log_file):
    """Read the list of already processed IDs from the log file"""
    processed_ids = set()
    
    try:
        if os.path.exists(processed_log_file):
            with open(processed_log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:  # Skip empty lines
                        processed_ids.add(line)
        return processed_ids
    except Exception as e:
        logger.error(f"Error reading processed IDs log: {e}")
        return set()

def update_processed_id_log(processed_log_file, certificate_id):
    """Add a processed ID to the log file"""
    try:
        with open(processed_log_file, 'a', encoding='utf-8') as f:
            f.write(f"{certificate_id}\n")
    except Exception as e:
        logger.error(f"Error updating processed ID log: {e}")

def save_progress(output_file, tmdb_meta_df):
    """Save current progress to avoid losing data in case of interruption"""
    try:
        # Create a backup of existing file if it exists
        if os.path.exists(output_file):
            backup_file = f"{output_file}.bak"
            os.rename(output_file, backup_file)
            
        # Save current data
        tmdb_meta_df.to_csv(output_file, index=False)
        logger.info(f"Progress saved to {output_file}")
        
        # Remove backup if save was successful
        if os.path.exists(f"{output_file}.bak"):
            os.remove(f"{output_file}.bak")
            
    except Exception as e:
        logger.error(f"Error saving progress: {e}")

def enrich_movies_with_tmdb(input_file, output_file, log_file=None, limit=None, force=False):
    """
    Enrich movie data with information from TMDB API
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        log_file (str, optional): Path to log file for processed IDs
        limit (int, optional): Limit processing to this many rows (for testing)
        force (bool): Force processing of all movies even if already processed
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        # Load the input data
        logger.info(f"Loading data from {input_file}")
        df = pd.read_csv(input_file, low_memory=False)
        
        # Get unique certificate_id and film_name combinations
        unique_movies = df[['certificate_id', 'film_name', 'language']].drop_duplicates()
        logger.info(f"Found {len(unique_movies)} unique movies to process")
        
        # Get the list of already processed IDs if log file is provided
        processed_ids = set()
        if log_file and not force:
            processed_ids = get_processed_ids(log_file)
            logger.info(f"Found {len(processed_ids)} movies already processed")
        
        # Load existing TMDB metadata if file exists and not forcing
        tmdb_meta_df = pd.DataFrame()
        if os.path.exists(output_file) and not force:
            try:
                tmdb_meta_df = pd.read_csv(output_file, low_memory=False)
                logger.info(f"Loaded existing TMDB metadata with {len(tmdb_meta_df)} entries")
            except Exception as e:
                logger.warning(f"Could not load existing TMDB metadata: {e}")
                tmdb_meta_df = pd.DataFrame()
        
        # Create output dataframe for tmdb_meta.csv if needed
        if tmdb_meta_df.empty:
            tmdb_meta_columns = [
                "certificate_id", "tmdb_id", "tmdb_title", "tmdb_original_title", 
                "tmdb_original_language", "tmdb_overview", "tmdb_release_date", 
                "tmdb_popularity", "tmdb_vote_average", "tmdb_vote_count", 
                "tmdb_genre_ids", "tmdb_poster_path", "tmdb_backdrop_path",
                "raw_response"
            ]
            tmdb_meta_df = pd.DataFrame(columns=tmdb_meta_columns)
        
        # Apply a limit if specified
        if limit and limit > 0:
            logger.info(f"Limiting processing to {limit} movies")
            unique_movies = unique_movies.head(limit)
        
        # Get already processed certificate_ids from tmdb_meta_df
        existing_cert_ids = set()
        if 'certificate_id' in tmdb_meta_df.columns:
            existing_cert_ids = set(tmdb_meta_df['certificate_id'].astype(str))
        
        # Count movies to process
        movies_to_process = []
        for _, row in unique_movies.iterrows():
            certificate_id = str(row['certificate_id']).strip()
            
            # Skip if already processed and not forced
            if not force and (certificate_id in processed_ids or certificate_id in existing_cert_ids):
                continue
                
            movies_to_process.append(row)
        
        logger.info(f"Processing {len(movies_to_process)} movies")
        
        # Process each unique movie
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        # Process in batches to periodically save progress
        BATCH_SIZE = 20
        
        for i, row in enumerate(tqdm(movies_to_process, desc="Enriching with TMDB data")):
            certificate_id = str(row['certificate_id']).strip()
            movie_title = str(row['film_name']).strip()
            language = row['language']
            
            if not movie_title:
                logger.warning(f"Skipping movie with empty title (ID: {certificate_id})")
                skipped_count += 1
                continue
            
            try:
                # Query TMDB API
                movie_data = search_movie(movie_title, None, language)
                
                if movie_data:
                    # Extract TMDB data
                    tmdb_data = extract_tmdb_data(movie_data)
                    
                    # Create new row for tmdb_meta.csv
                    new_row = {
                        "certificate_id": certificate_id,
                        "raw_response": json.dumps(movie_data)
                    }
                    new_row.update(tmdb_data)
                    
                    # Add row to the tmdb_meta dataframe
                    new_df = pd.DataFrame([new_row])
                    tmdb_meta_df = pd.concat([tmdb_meta_df, new_df], ignore_index=True)
                    
                    success_count += 1
                    
                    # Update the log file
                    if log_file:
                        update_processed_id_log(log_file, certificate_id)
                else:
                    logger.warning(f"No TMDB match found for '{movie_title}' (ID: {certificate_id})")
                    error_count += 1
            
            except Exception as e:
                logger.error(f"Error processing movie '{movie_title}' (ID: {certificate_id}): {e}")
                error_count += 1
            
            # Save progress periodically
            if (i + 1) % BATCH_SIZE == 0:
                save_progress(output_file, tmdb_meta_df)
                
            # Rate limiting
            time.sleep(REQUEST_DELAY)
        
        # Save the final TMDB metadata
        logger.info(f"Saving TMDB metadata to {output_file}")
        tmdb_meta_df.to_csv(output_file, index=False)
        
        logger.info(f"TMDB addition complete: {success_count} successful, {error_count} errors, {skipped_count} skipped")
        return True
        
    except Exception as e:
        logger.error(f"Error in TMDB addition process: {e}")
        return False

def main():
    """Main function to run the TMDB addition process"""
    parser = argparse.ArgumentParser(description='Enrich movie data with TMDB API information')
    parser.add_argument('--input', type=str, help='Path to input CSV file')
    parser.add_argument('--output', type=str, help='Path to output CSV file for TMDB metadata')
    parser.add_argument('--log', type=str, help='Path to log file for processed IDs')
    parser.add_argument('--limit', type=int, help='Limit processing to this many rows (for testing)')
    parser.add_argument('--force', action='store_true', help='Force processing of all movies even if already processed')
    
    args = parser.parse_args()
    
    # Get the current directory (where this script is located)
    current_dir = Path(__file__).parent.absolute()
    
    # Define default paths if not provided
    input_file = args.input or str(current_dir.parent.parent / "data" / "data.csv")
    output_file = args.output or str(current_dir.parent.parent / "data" / "tmdb_meta.csv")
    log_file = args.log or str(current_dir / "tmdb_processed_ids.log")
    
    # Ensure the input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return False
    
    # Run the addition process
    success = enrich_movies_with_tmdb(
        input_file=input_file,
        output_file=output_file,
        log_file=log_file,
        limit=args.limit,
        force=args.force
    )
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 