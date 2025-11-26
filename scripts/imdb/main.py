#!/usr/bin/env python3
import csv
import os
import time
import json
from imdbinfo import get_movie, search_title
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
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
OVERRIDE_FILE = "override.csv"

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

def load_overrides():
    """
    Load manual certificate ID to IMDB ID mappings from override.csv
    
    Returns:
        dict: Dictionary mapping certificate_id to imdb_id
    """
    overrides = {}
    
    if not os.path.exists(OVERRIDE_FILE):
        logger.debug("No override.csv file found, skipping manual overrides")
        return overrides
    
    try:
        with open(OVERRIDE_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                if 'certificate_id' in row and 'imdb_id' in row:
                    certificate_id = row['certificate_id'].strip()
                    imdb_id = row['imdb_id'].strip()
                    if certificate_id and imdb_id:
                        overrides[certificate_id] = imdb_id
        
        logger.info(f"Loaded {len(overrides)} manual overrides from override.csv")
        
    except Exception as e:
        logger.error(f"Error reading override.txt: {e}")
    
    return overrides

def fetch_override_imdb_details(overrides):
    """
    Fetch IMDB details for override entries and update the output file
    This function will overwrite any existing entries for the certificate IDs in overrides
    
    Args:
        overrides: Dictionary mapping certificate_id to imdb_id
    """
    if not overrides:
        return
    
    # Define the fields we want to extract from IMDb (matching existing CSV structure)
    fields = [
        'original_id', 'imdb_id', 'title', 'year', 'genres', 'rating', 'votes', 'directors', 
        'actors', 'runtime', 'countries', 'languages', 'overview',
        'release_date', 'writers', 'studios', 'poster_url'
    ]
    
    # Read existing data if file exists
    existing_data = []
    file_exists = os.path.isfile(OUTPUT_FILE)
    
    if file_exists:
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    # Keep all existing rows, we'll replace overridden ones later
                    existing_data.append(row)
            logger.info(f"Loaded {len(existing_data)} existing entries, will overwrite entries for {len(overrides)} certificate IDs")
        except Exception as e:
            logger.error(f"Error reading existing IMDB file: {e}")
            existing_data = []
    
    # Fetch new override data
    override_data = []
    for certificate_id, imdb_id in tqdm(overrides.items(), desc="Fetching override IMDB data"):
        try:
            # Ensure IMDB ID has 'tt' prefix (imdbinfo accepts both formats)
            movie_id = imdb_id if imdb_id.startswith('tt') else f'tt{imdb_id}'
            
            logger.info(f"Fetching IMDB details for override: {certificate_id} -> {imdb_id}")
            
            # Get the movie details
            movie = get_movie(movie_id)
            
            if not movie:
                logger.warning(f"No movie found for IMDB ID: {imdb_id}")
                continue
            
            # Log the selected movie
            logger.info(f"Found movie: {movie.title} ({movie.year})")
            
            # Create the data entry using the same field names as the existing CSV
            movie_data_entry = {}
            
            # If we have existing data, use the same field structure
            if existing_data and len(existing_data) > 0:
                # Use the field names from existing data as template
                template_row = existing_data[0]
                for field_name in template_row.keys():
                    movie_data_entry[field_name] = ''  # Initialize with empty string
            else:
                # Use our default field structure
                for field_name in fields:
                    movie_data_entry[field_name] = ''
            
            # Now populate with actual data (using field names that match existing CSV)
            # Extract movie ID without 'tt' prefix for storage
            stored_movie_id = movie_id.replace('tt', '') if movie_id.startswith('tt') else movie_id
            
            # Extract directors
            directors_list = []
            if hasattr(movie, 'directors') and movie.directors:
                directors_list = [d.name if hasattr(d, 'name') else str(d) for d in movie.directors]
            
            # Extract actors (cast)
            actors_list = []
            if hasattr(movie, 'actors') and movie.actors:
                actors_list = [a.name if hasattr(a, 'name') else str(a) for a in movie.actors[:10]]  # Top 10 cast
            
            # Extract writers
            writers_list = []
            if hasattr(movie, 'writers') and movie.writers:
                writers_list = [w.name if hasattr(w, 'name') else str(w) for w in movie.writers]
            
            # Extract production companies
            studios_list = []
            if hasattr(movie, 'production_companies') and movie.production_companies:
                studios_list = [company.name if hasattr(company, 'name') else str(company) for company in movie.production_companies]
            
            movie_data_entry.update({
                'original_id': certificate_id,
                'imdb_id': stored_movie_id,
                'title': movie.title if hasattr(movie, 'title') else '',
                'year': str(movie.year) if hasattr(movie, 'year') and movie.year else '',
                'genres': "|".join(movie.genres) if hasattr(movie, 'genres') and movie.genres else '',
                'rating': str(movie.rating) if hasattr(movie, 'rating') and movie.rating else '',
                'votes': str(movie.votes) if hasattr(movie, 'votes') and movie.votes else '',
                'directors': "|".join(directors_list),
                'actors': "|".join(actors_list),
                'runtime': str(movie.runtime) if hasattr(movie, 'runtime') and movie.runtime else '',
                'countries': "|".join(movie.countries) if hasattr(movie, 'countries') and movie.countries else '',
                'languages': "|".join(movie.languages) if hasattr(movie, 'languages') and movie.languages else '',
                'overview': movie.plot if hasattr(movie, 'plot') and movie.plot else '',
                'release_date': str(movie.release_date) if hasattr(movie, 'release_date') and movie.release_date else (str(movie.year) if hasattr(movie, 'year') and movie.year else ''),
                'writers': "|".join(writers_list),
                'studios': "|".join(studios_list),
                'poster_url': movie.poster_url if hasattr(movie, 'poster_url') and movie.poster_url else ''
            })
            
            override_data.append(movie_data_entry)
            
            # Sleep to avoid hitting rate limits
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing override {certificate_id} -> {imdb_id}: {e}")
            continue
    
    # Merge existing data with override data, replacing overridden entries
    # Create a dictionary of override data keyed by certificate_id for fast lookup
    override_dict = {entry['original_id']: entry for entry in override_data}
    
    # Build the final data list
    final_data = []
    overridden_count = 0
    
    # Process existing data
    for row in existing_data:
        certificate_id = row.get('original_id', '')
        if certificate_id in override_dict:
            # Replace with override data
            final_data.append(override_dict[certificate_id])
            overridden_count += 1
            # Remove from override_dict so we don't add it again
            del override_dict[certificate_id]
        else:
            # Keep existing data
            final_data.append(row)
    
    # Add any remaining override entries (for certificate IDs that weren't in existing data)
    final_data.extend(override_dict.values())
    
    logger.info(f"Overrode {overridden_count} existing entries, added {len(override_dict)} new entries")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=';')
        writer.writeheader()
        for row in final_data:
            writer.writerow(row)
    
    logger.info(f"Updated IMDB file with {len(override_data)} override entries, total entries: {len(final_data)}")

def main():
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Load completed IDs
    completed_ids = load_completed_ids()
    logger.debug(f"Loaded {len(completed_ids)} completed IDs from {COMPLETED_FILE}")
    
    # Read movie titles and IDs from input file
    movie_data = {}  # Dictionary to store movie_name -> list of ids mapping
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'movie_name' in row and row['movie_name']:
                    movie_name = row['movie_name']
                    movie_id = row.get('id', '')
                    if movie_id not in completed_ids:
                        # Store all IDs for each movie name
                        if movie_name not in movie_data:
                            movie_data[movie_name] = []
                        movie_data[movie_name].append(movie_id)

        # Get unique movie titles
        movie_titles = list(movie_data.keys())
        
        # Count total IDs (some movies have multiple certificate IDs)
        total_ids = sum(len(ids) for ids in movie_data.values())
        logger.debug(f"Read {len(movie_titles)} unique movie titles with {total_ids} total certificate IDs from {INPUT_FILE}")
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return
    
    # Define the fields we want to extract from IMDb (matching existing CSV structure)
    fields = [
        'original_id', 'imdb_id', 'title', 'year', 'genres', 'rating', 'votes', 'directors', 
        'actors', 'runtime', 'countries', 'languages', 'overview',
        'release_date', 'writers', 'studios', 'poster_url'
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
            logger.debug(f"Found {len(existing_movies)} existing movies and {len(existing_original_ids)} existing original IDs in {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"Error reading existing output file: {e}")
            existing_movies = set()
            existing_original_ids = set()

    # Load and process override entries first
    overrides = load_overrides()
    if overrides:
        logger.info("Processing manual overrides...")
        fetch_override_imdb_details(overrides)
        
        # Reload existing data after overrides have been processed
        existing_movies = set()
        existing_original_ids = set()
        if os.path.isfile(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f, delimiter=';')
                    for row in reader:
                        if 'imdb_id' in row and row['imdb_id']:
                            existing_movies.add(row['imdb_id'])
                        if 'original_id' in row and row['original_id']:
                            existing_original_ids.add(row['original_id'])
                logger.debug(f"Reloaded {len(existing_movies)} existing movies and {len(existing_original_ids)} existing original IDs after override processing")
            except Exception as e:
                logger.error(f"Error reloading existing output file: {e}")
                existing_movies = set()
                existing_original_ids = set()

    # If there are no new movies to fetch and no overrides, exit
    if not movie_titles and not overrides:
        logger.info("No new movies found in input files and no overrides. Skipping fetch.")
        return
    
    # Only process regular movie titles if there are any
    if movie_titles:
        with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, delimiter=';')
            
            # Write header only if the file is new
            if not file_exists:
                writer.writeheader()
            
            # Process each movie title
            for movie_title in tqdm(movie_titles, desc="Fetching IMDb data"):
                try:
                    # Get all certificate IDs for this movie
                    original_ids = movie_data.get(movie_title, [])
                    
                    # Filter out IDs that already exist
                    new_ids = [oid for oid in original_ids if oid not in existing_original_ids and oid not in completed_ids]
                    
                    if not new_ids:
                        logger.debug(f"Skipping movie - all certificate IDs already processed: {movie_title}")
                        continue

                    # Log the movie title
                    logger.info(f"Processing movie: {movie_title} (with {len(new_ids)} certificate IDs)")

                    # Search for the movie
                    search_results = search_title(movie_title)
                    
                    if not search_results or not search_results.titles:
                        logger.warning(f"No results found for: {movie_title}")
                        # Mark all IDs as completed even if no results found
                        for original_id in new_ids:
                            completed_ids.add(original_id)
                        save_completed_ids(completed_ids)
                        continue
                    
                    # Get the first matching movie ID
                    first_result = search_results.titles[0]
                    movie_id = first_result.imdb_id
                    # Remove 'tt' prefix for storage consistency
                    stored_movie_id = movie_id.replace('tt', '') if movie_id.startswith('tt') else movie_id
                    
                    if stored_movie_id in existing_movies:
                        logger.info(f"IMDb data already fetched for: {movie_title} (IMDb ID: {stored_movie_id}), writing for new certificate IDs")
                        # If we already have the IMDb data, we should still write rows for new certificate IDs
                        # We'll need to fetch the movie details again to write the rows
                    
                    # Get the movie details
                    movie = get_movie(movie_id)
                    
                    if not movie:
                        logger.warning(f"No movie found for IMDB ID: {movie_id}")
                        # Mark all IDs as completed even if no movie found
                        for original_id in new_ids:
                            completed_ids.add(original_id)
                        save_completed_ids(completed_ids)
                        continue

                    # Log the selected movie
                    logger.info(f"Selected movie: {movie.title} ({movie.year})")
                    
                    # Create base movie data (common for all certificate IDs)
                    # Extract directors
                    directors_list = []
                    if hasattr(movie, 'directors') and movie.directors:
                        directors_list = [d.name if hasattr(d, 'name') else str(d) for d in movie.directors]
                    
                    # Extract actors (cast)
                    actors_list = []
                    if hasattr(movie, 'actors') and movie.actors:
                        actors_list = [a.name if hasattr(a, 'name') else str(a) for a in movie.actors[:10]]  # Top 10 cast
                    
                    # Extract writers
                    writers_list = []
                    if hasattr(movie, 'writers') and movie.writers:
                        writers_list = [w.name if hasattr(w, 'name') else str(w) for w in movie.writers]
                    
                    # Extract production companies
                    studios_list = []
                    if hasattr(movie, 'production_companies') and movie.production_companies:
                        studios_list = [company.name if hasattr(company, 'name') else str(company) for company in movie.production_companies]
                    
                    base_movie_data = {
                        'imdb_id': stored_movie_id,
                        'title': movie.title if hasattr(movie, 'title') else '',
                        'year': str(movie.year) if hasattr(movie, 'year') and movie.year else '',
                        'genres': "|".join(movie.genres) if hasattr(movie, 'genres') and movie.genres else '',
                        'rating': str(movie.rating) if hasattr(movie, 'rating') and movie.rating else '',
                        'votes': str(movie.votes) if hasattr(movie, 'votes') and movie.votes else '',
                        'directors': "|".join(directors_list),
                        'actors': "|".join(actors_list),
                        'runtime': str(movie.runtime) if hasattr(movie, 'runtime') and movie.runtime else '',
                        'countries': "|".join(movie.countries) if hasattr(movie, 'countries') and movie.countries else '',
                        'languages': "|".join(movie.languages) if hasattr(movie, 'languages') and movie.languages else '',
                        'overview': movie.plot if hasattr(movie, 'plot') and movie.plot else '',
                        'release_date': str(movie.release_date) if hasattr(movie, 'release_date') and movie.release_date else (str(movie.year) if hasattr(movie, 'year') and movie.year else ''),
                        'writers': "|".join(writers_list),
                        'studios': "|".join(studios_list),
                        'poster_url': movie.poster_url if hasattr(movie, 'poster_url') and movie.poster_url else ''
                    }
                    
                    # Write one row per certificate ID
                    for original_id in new_ids:
                        movie_data_entry = {
                            'original_id': original_id,
                            **base_movie_data
                        }
                        
                        # Write to CSV
                        writer.writerow(movie_data_entry)
                        
                        # Mark as completed
                        completed_ids.add(original_id)
                        if original_id:
                            existing_original_ids.add(original_id)
                    
                    # Flush the file to ensure data is written immediately
                    f.flush()
                    
                    # Save completed IDs
                    save_completed_ids(completed_ids)
                    
                    # Add the movie to our set of existing movies
                    existing_movies.add(stored_movie_id)
                    
                    # Sleep to avoid hitting rate limits
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing {movie_title}: {e}")
                    continue
    
    logger.info(f"IMDb data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
