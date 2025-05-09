#!/usr/bin/env python3
import csv
import os
import time
import json
from imdb import Cinemagoer
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("imdb_fetch.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# File paths
INPUT_FILE = "../../data/individual_files/metadata_modifications.csv"
OUTPUT_FILE = "../../data/raw/imdb.csv"
COMPLETED_FILE = ".completed.json"

def load_completed_ids():
    """Load the set of completed original_ids from .completed.json"""
    if os.path.exists(COMPLETED_FILE):
        try:
            with open(COMPLETED_FILE, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logger.error(f"Error loading completed IDs: {e}")
            return set()
    return set()

def save_completed_ids(completed_ids):
    """Save the set of completed original_ids to .completed.json"""

    # Deduplicate the completed_ids
    completed_ids = sorted(list(set(completed_ids)))

    try:
        with open(COMPLETED_FILE, 'w') as f:
            json.dump(completed_ids, f)
    except Exception as e:
        logger.error(f"Error saving completed IDs: {e}")

def main():
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Load completed IDs
    completed_ids = load_completed_ids()
    logger.info(f"Loaded {len(completed_ids)} completed IDs from {COMPLETED_FILE}")
    
    # Initialize the Cinemagoer
    ia = Cinemagoer()
    
    # Read movie titles and IDs from input file
    movie_data = {}  # Dictionary to store movie_name -> id mapping
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'movie_name' in row and row['movie_name']:
                    movie_name = row['movie_name']
                    movie_id = row.get('id', '')
                    movie_data[movie_name] = movie_id

        # Get unique movie titles
        movie_titles = list(movie_data.keys())
        
        logger.info(f"Read {len(movie_titles)} movie titles from {INPUT_FILE}")
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return
    
    # Define the fields we want to extract from IMDb
    fields = [
        'original_id', 'imdb_id', 'imdb_title', 'imdb_year', 'imdb_genres', 'imdb_rating', 'imdb_votes', 'imdb_directors', 
        'imdb_actors', 'imdb_runtime', 'imdb_countries', 'imdb_languages', 'imdb_overview',
        'imdb_release_date', 'imdb_writers', 'imdb_studios', 'imdb_poster_url'
    ]
    
    # Create or open the output file
    file_exists = os.path.isfile(OUTPUT_FILE)
    
    # Load existing movies to avoid rescraping
    existing_movies = set()
    existing_original_ids = set()
    if file_exists:
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    if 'imdb_id' in row and row['imdb_id']:
                        existing_movies.add(row['imdb_id'])
                    if 'original_id' in row and row['original_id']:
                        existing_original_ids.add(row['original_id'])
            logger.info(f"Found {len(existing_movies)} existing movies and {len(existing_original_ids)} existing original IDs in {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"Error reading existing output file: {e}")
            existing_movies = set()
            existing_original_ids = set()
    
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=';')
        
        # Write header only if the file is new
        if not file_exists:
            writer.writeheader()
        
        # Process each movie title
        for movie_title in tqdm(movie_titles, desc="Fetching IMDb data"):
            try:
                # Skip if original_id already exists in the output file or completed.json
                original_id = movie_data.get(movie_title, '')

                # Add the original_id to the completed_ids set
                completed_ids.add(original_id)
                save_completed_ids(completed_ids)

                if original_id and (original_id in existing_original_ids or original_id in completed_ids):
                    logger.info(f"Skipping movie with existing original_id: {movie_title} (ID: {original_id})")
                    continue

                # Log the movie title
                logger.info(f"Processing movie: {movie_title}")

                # Search for the movie
                search_results = ia.search_movie(movie_title)
                
                if not search_results:
                    logger.warning(f"No results found for: {movie_title}")
                    continue
                
                # Get the first matching movie ID
                movie_id = search_results[0].movieID
                if movie_id in existing_movies:
                    logger.info(f"Skipping already scraped movie: {movie_title} (ID: {movie_id})")
                    continue
                
                # Get the movie details
                movie = ia.get_movie(movie_id)

                # Log the selected movie
                logger.info(f"Selected movie: {movie.get('title', '')} ({movie.get('year', '')})")
                
                # Extract data
                movie_data_entry = {
                    'original_id': original_id,
                    'imdb_id': movie_id,
                    'imdb_title': movie.get('title', ''),
                    'imdb_year': movie.get('year', ''),
                    'imdb_genres': "|".join(movie.get('genres', [])),
                    'imdb_rating': movie.get('rating', ''),
                    'imdb_votes': movie.get('votes', ''),
                    'imdb_directors': "|".join([d.get('name', '') for d in movie.get('directors', [])]),
                    'imdb_actors': "|".join([c.get('name', '') for c in movie.get('cast', [])[:10]]),  # Top 10 cast
                    'imdb_runtime': movie.get('runtimes', [''])[0] if movie.get('runtimes') else '',
                    'imdb_countries': "|".join(movie.get('countries', [])),
                    'imdb_languages': "|".join(movie.get('languages', [])),
                    'imdb_overview': movie.get('plot', [''])[0] if movie.get('plot') else '',
                    'imdb_release_date': movie.get('original air date', '') or movie.get('year', ''),
                    'imdb_writers': "|".join([w.get('name', '') for w in movie.get('writers', [])]),
                    'imdb_studios': "|".join([company.get('name', '') if hasattr(company, 'get') else str(company) for company in movie.get('production companies', [])]),
                    'imdb_poster_url': movie.get('full-size cover url', '') or movie.get('cover url', '')
                }
                
                # Write to CSV
                writer.writerow(movie_data_entry)
                
                # Flush the file to ensure data is written immediately
                f.flush()
                
                # Add the movie to our set of existing movies and completed IDs
                existing_movies.add(movie_id)
                if original_id:
                    existing_original_ids.add(original_id)
                
                # Sleep to avoid hitting rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing {movie_title}: {e}")
                continue
    
    logger.info(f"IMDb data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
