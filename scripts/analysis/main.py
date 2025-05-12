import os
import pandas as pd
import numpy as np
import re
import time
import hashlib
import json
import logging
import warnings
import argparse

from utils import cleanup_language, cleanup_movie_name

# Suppress specific warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas.core.strings.object_array')
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas.core.frame')
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas.core.indexers')
warnings.filterwarnings('ignore', category=FutureWarning, message='.*downcasting.*')
warnings.filterwarnings('ignore', category=FutureWarning, message='.*will attempt to set the values inplace.*')
warnings.filterwarnings('ignore', category=FutureWarning, message='.*incompatible dtype.*')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
RAW_MODIFICATIONS_PATH = '../../data/raw/modifications.csv'
RAW_METADATA_PATH = '../../data/raw/metadata.csv'
RAW_CATEGORIES_PATH = '../../data/raw/categories.csv'

# Changed back to the default output directories to match R script
BASE_OUTPUT_DIR = "../../data/"
INDIVIDUAL_DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "individual_files")
SITE_DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "site_data")

CLEANED_MODS_OUTPUT_PATH = os.path.join(INDIVIDUAL_DATA_DIR, "modifications_cleaned.csv")
CLEANED_META_OUTPUT_PATH = os.path.join(INDIVIDUAL_DATA_DIR, "metadata_cleaned.csv")
COMPLETE_DATA_CSV_PATH = os.path.join(INDIVIDUAL_DATA_DIR, "metadata_modifications.csv")

# File for tracking data file hashes
HASH_CACHE_PATH = os.path.join(".processed.json")

def calculate_file_hash(file_path):
    """
    Calculate MD5 hash of a file
    
    Args:
        file_path: Path to the file
        
    Returns:
        MD5 hash string of the file
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def should_skip_processing():
    """
    Check if processing can be skipped because input files haven't changed
    
    Returns:
        Tuple of (bool, dict): Whether to skip processing, and current file hashes
    """
    if not os.path.exists(RAW_MODIFICATIONS_PATH) or not os.path.exists(RAW_METADATA_PATH):
        return False, {}
        
    # Calculate current file hashes
    current_hashes = {
        "modifications": calculate_file_hash(RAW_MODIFICATIONS_PATH),
        "metadata": calculate_file_hash(RAW_METADATA_PATH)
    }
    
    # Add categories file hash if it exists
    if os.path.exists(RAW_CATEGORIES_PATH):
        current_hashes["categories"] = calculate_file_hash(RAW_CATEGORIES_PATH)
    
    # Load previous file hashes if they exist
    if os.path.exists(HASH_CACHE_PATH):
        try:
            with open(HASH_CACHE_PATH, 'r') as f:
                previous_hashes = json.load(f)
                
            # Check if output files exist
            outputs_exist = (
                os.path.exists(CLEANED_MODS_OUTPUT_PATH) and
                os.path.exists(CLEANED_META_OUTPUT_PATH) and
                os.path.exists(COMPLETE_DATA_CSV_PATH)

            )
            
            # Check if hashes match
            files_unchanged = (
                current_hashes["modifications"] == previous_hashes.get("modifications", "") and
                current_hashes["metadata"] == previous_hashes.get("metadata", "")
            )
            
            # Also check categories file if it exists
            if "categories" in current_hashes:
                files_unchanged = files_unchanged and (
                    current_hashes["categories"] == previous_hashes.get("categories", "")
                )
            
            if files_unchanged and outputs_exist:
                return True, current_hashes
                
        except Exception as e:
            logger.warning(f"Error reading hash cache: {str(e)}")
    
    return False, current_hashes

def save_file_hashes(hashes):
    """
    Save file hashes to cache file
    
    Args:
        hashes: Dictionary of file hashes
    """
    try:
        with open(HASH_CACHE_PATH, 'w') as f:
            json.dump(hashes, f)
    except Exception as e:
        logger.warning(f"Error saving hash cache: {str(e)}")


def safe_str_replace(series, pattern, replacement, regex=True):
    """
    Safely apply string replace operations, handling non-string values
    
    Args:
        series: pandas Series to operate on
        pattern: regex or string pattern to replace
        replacement: replacement string
        regex: whether to use regex or plain string replacement
        
    Returns:
        pandas Series with replacements applied to string values only
    """
    # First convert to string type, but keep NA as NA
    str_series = series.astype(str).replace('nan', np.nan)
    
    # Apply string methods only to non-NA values
    mask = str_series.notna()
    if mask.any():
        str_series.loc[mask] = str_series.loc[mask].str.replace(pattern, replacement, regex=regex).str.strip()
    
    return str_series


def clean_metadata(df):
    """
    Cleans basic metadata columns. Standardizes ID to character, trimmed, leading zeros removed.
    
    Args:
        df: DataFrame containing metadata
    
    Returns:
        DataFrame with cleaned metadata columns
    """
    start_time = time.time()
    logger.info("Cleaning metadata...")
    
    # Clean id column
    if 'id' in df.columns:
        df['id'] = df['id'].astype(str).str.strip()
        df['id'] = df['id'].apply(lambda x: '0' if x == '0' else re.sub('^0+', '', x) if pd.notna(x) else np.nan)
    else:
        logger.warning("Column 'id' not found in metadata.")
    
    # Clean and calculate duration_secs
    if 'duration' in df.columns:
        def parse_duration(duration_str):
            if pd.isna(duration_str):
                return np.nan
            
            duration_str = str(duration_str).strip()
            
            # Try the format seen in the data: "000.33 MM.SS"
            mmss_match = re.search(r'(\d+)\.(\d+)\s*MM\.SS', duration_str)
            if mmss_match:
                minutes = float(mmss_match.group(1))
                seconds = float(mmss_match.group(2))
                return minutes * 60 + seconds  # Convert to seconds
            
            # Try numeric format
            duration_raw = re.search(r'\d+\.\d+', duration_str)
            duration_numeric = float(duration_raw.group(0)) if duration_raw else np.nan
            
            # Try ISO format
            iso_pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?'
            iso_match = re.search(iso_pattern, duration_str)
            
            if iso_match:
                hrs = float(iso_match.group(1) or 0)
                mins = float(iso_match.group(2) or 0)
                secs = float(iso_match.group(3) or 0)
                iso_secs = hrs * 3600 + mins * 60 + secs
                return iso_secs if iso_secs > 0 else duration_numeric * 60
            
            return duration_numeric * 60  # Convert to seconds
        
        df['duration_secs'] = df['duration'].apply(parse_duration)
    else:
        df['duration_secs'] = np.nan
    
    # Handle empty strings as NA
    for col in ['applicant', 'certifier']:
        if col in df.columns:
            df[col] = df[col].replace('', np.nan)
    
    # Simple date parsing for cert_date - just use what's there
    if 'cert_date' in df.columns:
        def parse_ddmmyyyy(date_str):
            if pd.isna(date_str) or str(date_str).strip() == '':
                return pd.NaT
                
            date_str = str(date_str).strip()
            
            # Remove decimal point if present (e.g., "27012022.0" -> "27012022")
            date_str = re.sub(r'\.0$', '', date_str)
            
            # Format from categories.csv: "09-NOV-99 00:00:00"
            categories_match = re.match(r'(\d{1,2})-([A-Za-z]{3})-(\d{2})\s+\d{2}:\d{2}:\d{2}', date_str)
            if categories_match:
                day = int(categories_match.group(1))
                month_str = categories_match.group(2).upper()
                year_str = categories_match.group(3)
                
                # Convert month name to number
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                
                if month_str in month_map:
                    month = month_map[month_str]
                    
                    # Handle 2-digit year
                    if len(year_str) == 2:
                        year = int(year_str)
                        # Assume 20xx for years 00-99
                        if year < 100:
                            year += 2000 if year < 50 else 1900
                    else:
                        year = int(year_str)
                    
                    # Validate basic date components
                    if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                        return pd.Timestamp(f"{year:04d}-{month:02d}-{day:02d}")
            
            # Format example: "29012025" -> "2025-01-29"
            if re.match(r'^\d{8}$', date_str):
                try:
                    day = int(date_str[0:2])
                    month = int(date_str[2:4])
                    year = int(date_str[4:8])
                    
                    # Validate basic date components
                    if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                        return pd.Timestamp(f"{year:04d}-{month:02d}-{day:02d}")
                except Exception:
                    pass
            
            # If not 8-digit format, try basic ISO format
            try:
                return pd.to_datetime(date_str)
            except:
                return pd.NaT
        
        # Simply convert cert_date to proper format
        df['cert_date_parsed'] = df['cert_date'].apply(parse_ddmmyyyy)
        
        # Report on date parsing
        unparsed = df['cert_date_parsed'].isna() & df['cert_date'].notna()
        if unparsed.any():
            logger.warning(f"Could not parse {unparsed.sum()} date values from cert_date column")
    else:
        df['cert_date_parsed'] = pd.NaT
    
    elapsed = time.time() - start_time
    logger.info(f"Metadata cleaning completed in {elapsed:.2f} seconds")
    return df


def clean_modifications(df, description_col='description'):
    """
    Cleans modification descriptions, standardizes certificate_id, calculates times.
    
    Args:
        df: DataFrame containing modification details
        description_col: Name of the column with descriptions
    
    Returns:
        DataFrame with cleaned columns and added tags/times
    """
    start_time = time.time()
    logger.info("Cleaning modifications...")
    
    if description_col not in df.columns:
        raise ValueError(f"Column '{description_col}' not found in the dataframe.")
    
    # Clean certificate_id
    if 'certificate_id' in df.columns:
        df['certificate_id'] = df['certificate_id'].astype(str).str.strip()
        df['certificate_id'] = df['certificate_id'].apply(lambda x: '0' if x == '0' else re.sub('^0+', '', x) if pd.notna(x) else np.nan)
    else:
        raise ValueError("Critical column 'certificate_id' not found in modifications data.")
    
    # Note: Skip tagging patterns since process_descriptions.py handles that now
    
    # Ensure description column is string type and handle empty strings
    if description_col in df.columns:
        df[description_col] = df[description_col].astype(str).str.strip()
        df[description_col] = df[description_col].replace('', np.nan)
        df[description_col] = df[description_col].replace('nan', np.nan)
    
    # Clean cut_no if it exists
    if 'cut_no' in df.columns:
        df['cut_no'] = pd.to_numeric(df['cut_no'].astype(str).str.strip(), errors='coerce')
    
    # Convert time columns - fixed to handle "00.00" format seen in real data
    def convert_time_column(time_series):
        def convert_time(time_val):
            if pd.isna(time_val) or str(time_val).strip() == '':
                return 0
            
            time_str = str(time_val).strip()
            
            # Format: MM.SS or M.SS or M.S - includes formats like "00.00" seen in the data
            if re.match(r'^\d+\.\d{1,2}$', time_str):
                parts = time_str.split('.')
                mins = float(parts[0])
                secs_str = parts[1] + '0' if len(parts[1]) == 1 else parts[1]
                secs = float(secs_str)
                return mins * 60 + secs  # Convert to seconds
            
            # Format: 00:00 (minutes:seconds)
            if re.match(r'^\d+:\d{2}$', time_str):
                parts = time_str.split(':')
                mins = float(parts[0])
                secs = float(parts[1])
                return mins * 60 + secs  # Convert to seconds
            
            # Numeric value
            if re.match(r'^\d+(\.\d+)?$', time_str):
                num_val = float(time_str)
                # Heuristic: if > 1000, likely seconds; otherwise, assume minutes and convert
                return num_val if num_val > 1000 else num_val * 60
            
            return 0
        
        return time_series.apply(convert_time).clip(lower=0)
    
    # Apply time conversion
    for col in ['deleted', 'replaced', 'inserted']:
        if col in df.columns:
            df[f'{col}_secs'] = convert_time_column(df[col])
            logger.debug(f"Converted '{col}' to seconds")
        else:
            df[f'{col}_secs'] = 0
    
    # Calculate total modified time
    df['total_modified_time_secs'] = df['deleted_secs'].fillna(0) + df['replaced_secs'].fillna(0) + df['inserted_secs'].fillna(0)
    df['total_modified_time_secs'] = df['total_modified_time_secs'].round(2)
    
    # Remove original time columns
    for col in ['deleted', 'replaced', 'inserted']:
        if col in df.columns:
            df = df.drop(col, axis=1)
    
    elapsed = time.time() - start_time
    logger.info(f"Modifications cleaning completed in {elapsed:.2f} seconds")
    return df


def clean_embedded_content(df):
    """
    Cleans HTML/CSS, extracts basic info from film names.
    Uses the existing language column without trying to extract from film name.
    
    Args:
        df: DataFrame with film data
    
    Returns:
        DataFrame with cleaned columns
    """
    start_time = time.time()
    logger.info("Cleaning embedded content and extracting film data...")
    
    # Create a copy to avoid modifying the original during processing
    result = df.copy()
    
    # Ensure columns exist and convert to string safely
    cols_to_ensure_char = ['film_name_full', 'description', 'language', 'cleaned_description', 'film_name']
    for col in cols_to_ensure_char:
        if col not in result.columns:
            result[col] = np.nan
        # Safe conversion to string, preserving NaN values
        if col in result.columns:
            # First ensure the column is object type for string operations
            if not pd.api.types.is_object_dtype(result[col]):
                result[col] = result[col].astype(object)
            # Then convert non-NA values to string
            mask = result[col].notna()
            if mask.any():
                result.loc[mask, col] = result.loc[mask, col].astype(str)
            # Handle common NA strings
            result[col] = result[col].replace('nan', np.nan)
            result[col] = result[col].replace('None', np.nan)
            result[col] = result[col].replace('', np.nan)
    
    # Clean CSS/HTML from text columns using safe string replace
    html_css_regex = r'<style.*?/style>|<.*?>|qr-redirect-endorsment.*?EndorsementFile No\.'
    cols_to_clean_html = ['film_name_full', 'description', 'cleaned_description', 'film_name']
    for col in cols_to_clean_html:
        if col in result.columns:
            # Safely apply string replace to handle mixed types
            result[col] = safe_str_replace(result[col], html_css_regex, '')
            result[col] = result[col].replace('', np.nan)
    
    # Identify records with embedded tables
    if 'description' in result.columns:
        # Only apply string contains to non-NA values
        mask = result['description'].notna()
        result['has_embedded_table'] = False  # Default value
        if mask.any():
            result.loc[mask, 'has_embedded_table'] = result.loc[mask, 'description'].str.contains(
                r'Cut\s+No\.\s+Description.*Deleted.*Replaced.*Inserted', 
                regex=True, 
                na=False
            )
    else:
        result['has_embedded_table'] = False
    
    # Extract base film name
    if 'movie_name' in result.columns:
        # Apply regex replace only to non-NA values
        mask = result['movie_name'].notna()
        result['film_base_name'] = np.nan  # Default value
        if mask.any():
            result.loc[mask, 'film_base_name'] = result.loc[mask, 'movie_name'].str.replace(
                r'\s*\(.*$', '', regex=True
            ).str.strip()
        
        # Replace empty strings with NA
        result['film_base_name'] = result['film_base_name'].replace('', np.nan)
    else:
        result['film_base_name'] = np.nan
    
    elapsed = time.time() - start_time
    logger.info(f"Embedded content cleaning completed in {elapsed:.2f} seconds")
    return result


def normalize_cert_no(cert_no):
    """
    Normalizes certificate numbers by removing slashes and standardizing format.
    
    Args:
        cert_no: Certificate number in various formats
        
    Returns:
        Normalized certificate number
    """
    if pd.isna(cert_no):
        return np.nan
        
    cert_no = str(cert_no).strip()
    
    # If already in normalized format (no slashes), return as is
    if '/' not in cert_no:
        return cert_no
    
    # Simple approach: remove all slashes
    normalized = cert_no.replace('/', '')
    
    
    
    return normalized


def main():
    """
    Main function to orchestrate data cleaning, joining, and saving.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process and join censor board data')
    parser.add_argument('--force', action='store_true', help='Force processing even if files have not changed')
    args = parser.parse_args()
    
    start_time = time.time()

    
    # Check if processing can be skipped
    if not args.force:
        should_skip, current_hashes = should_skip_processing()
        if should_skip:
            logger.info("No changes found in input files. Skipping processing.")
            return
    else:
        # Still calculate hashes if forcing
        _, current_hashes = should_skip_processing()
        logger.info("Forcing processing even though input files may not have changed.")
    
    # Check if raw files exist
    if not os.path.exists(RAW_MODIFICATIONS_PATH):
        raise FileNotFoundError(f"Raw modifications file not found at: {RAW_MODIFICATIONS_PATH}")
    if not os.path.exists(RAW_METADATA_PATH):
        raise FileNotFoundError(f"Raw metadata file not found at: {RAW_METADATA_PATH}")
    
    # Read raw data files
    try:
        modifications_raw = pd.read_csv(RAW_MODIFICATIONS_PATH, dtype={'certificate_id': str}, na_values=['', 'NA', 'N/A', 'NULL'])
        logger.info(f"Loaded {len(modifications_raw):,} rows from modifications data")
    except Exception as e:
        raise RuntimeError(f"Failed to load modifications data: {str(e)}")
    
    try:
        metadata_raw = pd.read_csv(RAW_METADATA_PATH, dtype={'id': str}, na_values=['', 'NA', 'N/A', 'NULL'])
        logger.info(f"Loaded {len(metadata_raw):,} rows from metadata data")
    except Exception as e:
        raise RuntimeError(f"Failed to load metadata data: {str(e)}")
    
    # Load categories data if available
    categories_data = None
    if os.path.exists(RAW_CATEGORIES_PATH):
        try:
            categories_data = pd.read_csv(RAW_CATEGORIES_PATH, na_values=['', 'NA', 'N/A', 'NULL'])
            logger.info(f"Loaded {len(categories_data):,} rows from categories data")
            
            # Normalize certificate numbers in categories data
            if 'Certificate No' in categories_data.columns:
                categories_data['normalized_cert_no'] = categories_data['Certificate No'].apply(normalize_cert_no)
                
                # Log a few examples for debugging
                sample_size = min(3, len(categories_data))
                sample_certs = categories_data[['Certificate No', 'normalized_cert_no']].head(sample_size)
                
                
                # Check for any that didn't normalize properly
                not_normalized = categories_data[
                    ~categories_data['normalized_cert_no'].str.match(r'^[A-Z]+\d+\d{4}-[A-Z]+$', na=False)
                ]
                
        except Exception as e:
            logger.warning(f"Failed to load categories data: {str(e)}")
            categories_data = None
    else:
        logger.warning(f"Categories file not found at: {RAW_CATEGORIES_PATH}")
    
    # Perform initial cleaning
    metadata_cleaned = clean_metadata(metadata_raw)
    
    # Join with categories data if available
    if categories_data is not None and 'normalized_cert_no' in categories_data.columns:
        logger.info("Joining metadata with categories data...")
        
        # Normalize certificate numbers in metadata if cert_no column exists
        if 'cert_no' in metadata_cleaned.columns:
            metadata_cleaned['normalized_cert_no'] = metadata_cleaned['cert_no'].apply(normalize_cert_no)
           
            
            # Log a few examples for debugging
            sample_size = min(3, len(metadata_cleaned))
            sample_certs = metadata_cleaned[['cert_no', 'normalized_cert_no']].head(sample_size)
            
            
            # Check for any that didn't normalize properly
            not_normalized = metadata_cleaned[
                ~metadata_cleaned['normalized_cert_no'].str.match(r'^[A-Z]+\d+\d{4}-[A-Z]+$', na=False)
            ]
           
            
            # Create a mapping for language, rating, and cbfc_file_no
            language_map = {}
            rating_map = {}
            cbfc_file_no_map = {}
            cert_date_map = {}
            movie_name_map = {}
            
            for _, row in categories_data.iterrows():
                if pd.notna(row['normalized_cert_no']):
                    # Map language if available (remove subtitles language)
                    if pd.notna(row['Movie Language']):
                        language = cleanup_language(row['Movie Language'])
                        language_map[row['normalized_cert_no']] = language

                    # Map rating if available
                    if pd.notna(row['Movie Category']):
                        rating_map[row['normalized_cert_no']] = row['Movie Category']
                    
                    # Map movie name if available
                    if pd.notna(row['Movie Name']):
                        movie_name = cleanup_movie_name(row['Movie Name']) 
                        movie_name_map[row['normalized_cert_no']] = movie_name
                    
                    # Map cbfc_file_no if available
                    if pd.notna(row['source_file']):
                        # Remove .html extension
                        cbfc_file_no = row['source_file'].replace('.html', '')
                        cbfc_file_no_map[row['normalized_cert_no']] = cbfc_file_no
                    
                    # Map cert_date if available
                    if pd.notna(row['Certificate Date']):
                        cert_date_map[row['normalized_cert_no']] = row['Certificate Date']
            
            # Create rating column if it doesn't exist
            if 'rating' not in metadata_cleaned.columns:
                metadata_cleaned['rating'] = np.nan
                
            # Create cbfc_file_no column if it doesn't exist
            if 'cbfc_file_no' not in metadata_cleaned.columns:
                metadata_cleaned['cbfc_file_no'] = np.nan
            
            # Apply mappings to metadata
            for idx, row in metadata_cleaned.iterrows():
                cert_no = row['normalized_cert_no']
                
                # Use language directly from categories file
                if cert_no in language_map and pd.notna(language_map[cert_no]):
                    metadata_cleaned.at[idx, 'language'] = language_map[cert_no]
                
                # Add rating from categories
                if cert_no in rating_map and pd.notna(rating_map[cert_no]):
                    metadata_cleaned.at[idx, 'rating'] = rating_map[cert_no]
                
                # Add movie name from categories
                if cert_no in movie_name_map and pd.notna(movie_name_map[cert_no]):
                    metadata_cleaned.at[idx, 'movie_name'] = movie_name_map[cert_no]
                
                # Add cbfc_file_no from categories
                if cert_no in cbfc_file_no_map and pd.notna(cbfc_file_no_map[cert_no]):
                    metadata_cleaned.at[idx, 'cbfc_file_no'] = cbfc_file_no_map[cert_no]
                
                # Override cert_date if available in categories
                if cert_no in cert_date_map and pd.notna(cert_date_map[cert_no]):
                    metadata_cleaned.at[idx, 'cert_date'] = cert_date_map[cert_no]
            
            logger.info("Applied categories data to metadata")
            
            # Drop the temporary normalized_cert_no column
            metadata_cleaned = metadata_cleaned.drop('normalized_cert_no', axis=1)
        else:
            logger.warning("cert_no column not found in metadata_cleaned, cannot join with categories data")
            # Create a dummy normalized_cert_no column to avoid errors later
            metadata_cleaned['normalized_cert_no'] = np.nan
    
    if 'description' not in modifications_raw.columns:
        logger.warning("Column 'description' not found in modifications data. Modification cleaning partially skipped.")
        modifications_cleaned = modifications_raw.copy()
        
        # Ensure certificate_id is cleaned
        if 'certificate_id' in modifications_cleaned.columns:
            modifications_cleaned['certificate_id'] = modifications_cleaned['certificate_id'].astype(str).str.strip()
            modifications_cleaned['certificate_id'] = modifications_cleaned['certificate_id'].apply(
                lambda x: '0' if x == '0' else re.sub('^0+', '', x) if pd.notna(x) else np.nan
            )
        else:
            raise ValueError("Critical column 'certificate_id' not found in modifications data.")
        
        # Add expected columns as NA/0
        for col in ['mod_tags', 'content_tags', 'type_tags', 'cleaned_description']:
            if col not in modifications_cleaned.columns:
                modifications_cleaned[col] = np.nan
        for col in ['deleted_secs', 'replaced_secs', 'inserted_secs', 'total_modified_time_secs']:
            if col not in modifications_cleaned.columns:
                modifications_cleaned[col] = 0
    else:
        modifications_cleaned = clean_modifications(modifications_raw, description_col='description')
    
    # Check ID columns
    for df_name, df, id_col in [('metadata', metadata_cleaned, 'id'), 
                               ('modifications', modifications_cleaned, 'id')]:
        if id_col not in df.columns:
            raise ValueError(f"Required ID column '{id_col}' not found in {df_name} data.")
        if not pd.api.types.is_string_dtype(df[id_col]):
            logger.warning(f"{df_name} '{id_col}' column is not string type. Converting.")
            df[id_col] = df[id_col].astype(str)
    
    # Join datasets
    logger.info("Joining modifications and metadata...")
    
    # Define columns to select from each table
    cols_from_meta = ['id', 'certificate_id', 'movie_name', 'film_name', 'film_name_full', 'language', 'duration_secs',
                    'cert_date_parsed', 'cert_no', 'category', 'format', 'applicant', 'certifier',
                    'rating', 'cbfc_file_no']
    cols_from_meta = [col for col in cols_from_meta if col in metadata_cleaned.columns]
    
    # Perform the left join
    censorship_data = pd.merge(
        modifications_cleaned,
        metadata_cleaned[cols_from_meta],
        left_on='id',
        right_on='id',
        how='left'
    )
    logger.info(f"Rows after join: {len(censorship_data):,}")
    
    # Apply post-join cleaning
    try:
        censorship_data = clean_embedded_content(censorship_data)
    except Exception as e:
        logger.error(f"Error during embedded content cleaning: {str(e)}")
        raise
    
    # Consolidate potentially duplicated metadata within certificate IDs
    logger.info("Consolidating metadata within certificate IDs...")
    metadata_cols = ['id', 'certificate_id', 'movie_name', 'film_name', 'film_base_name', 'film_name_full', 'language',
                    'primary_language', 'duration_secs', 'cert_date_parsed', 'cert_no',
                    'category', 'format', 'applicant', 'certifier', 'rating', 'cbfc_file_no']
    metadata_cols = [col for col in metadata_cols if col in censorship_data.columns]
    
    if len(metadata_cols) > 0 and 'certificate_id' in censorship_data.columns:
        for col in metadata_cols:
            if col in censorship_data.columns:
                # Group by certificate_id and take the first non-NA value
                first_values = censorship_data.groupby('certificate_id')[col].first()
                
                # Apply values to all rows with the same certificate_id
                for cert_id in censorship_data['certificate_id'].unique():
                    if pd.notna(first_values.get(cert_id, np.nan)):
                        mask = (censorship_data['certificate_id'] == cert_id)
                        censorship_data.loc[mask, col] = first_values.get(cert_id)
    
    # Remove truly duplicate modifications
    logger.info("Removing duplicate modification rows...")
    if 'certificate_id' in censorship_data.columns and 'description' in censorship_data.columns:
        original_rows = len(censorship_data)
        
        # Create deduplication key columns
        dedup_cols = ['certificate_id', 'description']  # Use ORIGINAL description
        
        # Include cut_no in deduplication key if it exists
        if 'cut_no' in censorship_data.columns:
            dedup_cols.append('cut_no')
            logger.info(f"Using {', '.join(dedup_cols)} for deduplication")
        else:
            logger.info(f"Using {', '.join(dedup_cols)} for deduplication")
        
        # Create temporary columns for deduplication that handle NAs
        censorship_data['temp_description'] = censorship_data['description'].fillna('__NA_PLACEHOLDER__')
        if 'cut_no' in dedup_cols:
            censorship_data['temp_cut_no'] = censorship_data['cut_no'].fillna(-999)
            censorship_data = censorship_data.drop_duplicates(subset=['certificate_id', 'temp_description', 'temp_cut_no'])
            censorship_data = censorship_data.drop(['temp_description', 'temp_cut_no'], axis=1)
        else:
            censorship_data = censorship_data.drop_duplicates(subset=['certificate_id', 'temp_description'])
            censorship_data = censorship_data.drop('temp_description', axis=1)
        
        rows_removed = original_rows - len(censorship_data)
        logger.info(f"Removed {rows_removed:,} duplicate modification rows")
    
    # Select and rename final columns
    logger.info("Selecting and reformatting final columns...")
    final_cols = [
        'id', 'certificate_id_x', 'movie_name', 'movie_base_name',
        'language',
        'duration_secs',
        'mod_tags', 'content_tags', 'type_tags',
        'description',
        'cleaned_description',
        'cut_no', 'deleted_secs', 'replaced_secs', 'inserted_secs', 'total_modified_time_secs',
        'cert_date_parsed', 'cert_no',
        'applicant', 'certifier', 'rating', 'cbfc_file_no'
    ]
    print(censorship_data.columns)
    final_cols = [col for col in final_cols if col in censorship_data.columns]
    censorship_data = censorship_data[final_cols]
    
    # Rename columns
    rename_dict = {
        'cert_date_parsed': 'cert_date',
        'movie_base_name': 'film_base_name',
        'certificate_id_x': 'certificate_id'
    }
    for old_name, new_name in rename_dict.items():
        if old_name in censorship_data.columns:
            censorship_data = censorship_data.rename(columns={old_name: new_name})
    
    # Ensure cert_date is properly formatted before saving
    if 'cert_date' in censorship_data.columns:
        # Convert to datetime objects first to ensure consistent format
        censorship_data['cert_date'] = pd.to_datetime(censorship_data['cert_date'], errors='coerce')
        # Convert to string in ISO format (YYYY-MM-DD)
        censorship_data['cert_date'] = censorship_data['cert_date'].dt.strftime('%Y-%m-%d')
        
        # Check if cert_date is present after processing
        if censorship_data['cert_date'].notna().sum() == 0:
            logger.warning("cert_date column is empty after formatting - check date parsing logic")
    
    # Convert specific columns to factors (after consolidation and renaming)
    for col in ['mod_tags', 'content_tags', 'type_tags', 'language', 'category', 'format']:
        if col in censorship_data.columns:
            # Clean values first
            censorship_data[col] = censorship_data[col].replace('', np.nan)
            censorship_data[col] = censorship_data[col].replace('nan', np.nan)
            # Convert to category if not all NA
            if censorship_data[col].notna().any():
                censorship_data[col] = censorship_data[col].astype('category')
    
    # Numeric columns
    for col in ['duration_secs', 'deleted_secs', 'replaced_secs', 'inserted_secs', 
                'total_modified_time_secs', 'cut_no']:
        if col in censorship_data.columns and not pd.api.types.is_numeric_dtype(censorship_data[col]):
            censorship_data[col] = pd.to_numeric(censorship_data[col], errors='coerce')
            censorship_data[col] = censorship_data[col].replace([np.inf, -np.inf], np.nan)
    
    # Filter to keep only movies (duration >= 60 minutes or NA)
    if 'duration_secs' in censorship_data.columns:
        original_rows = len(censorship_data)
        # Convert duration to minutes for filtering and round to 2 decimal places
        duration_mins = (censorship_data['duration_secs'] / 60).round(2)
        censorship_data = censorship_data[duration_mins.isna() | (duration_mins >= 60)]
        rows_filtered = original_rows - len(censorship_data)
        if rows_filtered > 0:
            logger.info(f"Filtered out {rows_filtered:,} rows with duration < 60 minutes (likely not movies)")
    
    # Create output directories if they don't exist
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
    os.makedirs(SITE_DATA_DIR, exist_ok=True)
    
    # Validate data before saving
    logger.info("=" * 50)
    logger.info("Data validation summary:")
    
    # Check cert_date
    if 'cert_date' in censorship_data.columns:
        date_null_count = censorship_data['cert_date'].isna().sum()
        date_valid_count = censorship_data['cert_date'].notna().sum()
        date_percent = (date_valid_count / len(censorship_data)) * 100 if len(censorship_data) > 0 else 0
        logger.info(f"cert_date: {date_valid_count:,} valid dates ({date_percent:.1f}%), {date_null_count:,} null values")
    
    # Check language
    if 'language' in censorship_data.columns:
        lang_null_count = censorship_data['language'].isna().sum()
        lang_valid_count = censorship_data['language'].notna().sum()
        lang_percent = (lang_valid_count / len(censorship_data)) * 100 if len(censorship_data) > 0 else 0
        logger.info(f"language: {lang_valid_count:,} valid values ({lang_percent:.1f}%), {lang_null_count:,} null values")
        
        # Show language distribution
        if lang_valid_count > 0:
            top_langs = censorship_data['language'].value_counts().head(10)
            logger.info("Top languages:")
            for lang, count in top_langs.items():
                logger.info(f"  {lang}: {count:,} rows")
    
    # Check for unique modifications
    if 'certificate_id' in censorship_data.columns and 'cut_no' in censorship_data.columns:
        unique_films = censorship_data['certificate_id'].nunique()
        total_rows = len(censorship_data)
        avg_mods = total_rows / unique_films if unique_films > 0 else 0
        logger.info(f"Unique films: {unique_films:,}, Total modifications: {total_rows:,}, Avg mods per film: {avg_mods:.2f}")
    
    # Save outputs
    logger.info("=" * 50)
    logger.info("Saving output files...")
    
    # 1. Save cleaned modifications CSV
    try:
        modifications_cleaned.to_csv(CLEANED_MODS_OUTPUT_PATH, index=False)
        logger.info(f"Saved cleaned modifications data ({len(modifications_cleaned):,} rows) to {CLEANED_MODS_OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"Error saving cleaned modifications CSV: {str(e)}")
    
    # 2. Save cleaned metadata CSV
    try:
        metadata_cleaned.to_csv(CLEANED_META_OUTPUT_PATH, index=False)
        logger.info(f"Saved cleaned metadata data ({len(metadata_cleaned):,} rows) to {CLEANED_META_OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"Error saving cleaned metadata CSV: {str(e)}")
    
    # 3. Save complete joined and cleaned data CSV
    try:
        censorship_data.to_csv(COMPLETE_DATA_CSV_PATH, index=False)
        logger.info(f"Saved complete cleaned data ({len(censorship_data):,} rows) to {COMPLETE_DATA_CSV_PATH}")
    except Exception as e:
        logger.error(f"Error saving complete cleaned data CSV: {str(e)}")
    
    total_elapsed = time.time() - start_time
    logger.info("=" * 80)
    logger.info(f"Data processing completed in {total_elapsed:.2f} seconds")
    logger.info("=" * 80)
    
    # Save file hashes after successful processing
    save_file_hashes(current_hashes)


if __name__ == "__main__":
    main()