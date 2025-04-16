import logging
import json
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Tuple, Set
from getCookies import get_tokens
from scraper import CBFCScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_completed_ids() -> Set[str]:
    """
    Load the set of certificate IDs that have already been processed.
    
    Returns:
        Set of completed certificate IDs
    """
    completed_file = Path('data/raw/completed.json')
    if completed_file.exists():
        try:
            with open(completed_file, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logger.error(f"Error loading completed IDs: {str(e)}")
            return set()
    else:
        # Create the file with an empty array if it doesn't exist
        save_completed_ids(set())
        return set()

def save_completed_ids(completed_ids: Set[str]) -> None:
    """
    Save the set of completed certificate IDs to a JSON file.
    
    Args:
        completed_ids: Set of completed certificate IDs
    """
    completed_file = Path('data/raw/completed.json')
    # Create parent directories if they don't exist
    completed_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(completed_file, 'w') as f:
            json.dump(list(completed_ids), f)
    except Exception as e:
        logger.error(f"Error saving completed IDs: {str(e)}")

def process_region_year(scraper: CBFCScraper, region: int, year: int, max_seq: int = 100000, max_failures: int = 5) -> Tuple[List[Dict], List[Dict]]:
    """
    Process certificates for a specific region and year with early termination.
    
    Args:
        scraper: The scraper instance
        region: Region code (1-9)
        year: Year to process
        max_seq: Maximum sequence number to try
        max_failures: Number of consecutive failures before terminating
    
    Returns:
        Tuple of (metadata_records, modification_records)
    """
    year_code = year + 900  # Convert year to required format
    consecutive_failures = 0
    all_metadata = []
    all_modifications = []
    
    # Load already processed IDs
    completed_ids = load_completed_ids()
    logger.info(f"Loaded {len(completed_ids)} already processed IDs")
    
    # Process in batches for efficiency
    batch_size = 10
    current_batch = []
    
    for seq in range(1, max_seq + 1):
        certificate_id = f"1000{region}0{year_code}{seq:08d}"
        
        # Skip if this ID has already been processed
        if certificate_id in completed_ids:
            logger.debug(f"Skipping already processed ID: {certificate_id}")
            continue
            
        current_batch.append(certificate_id)
        
        # Process when batch is full or we've reached the max sequence
        if len(current_batch) >= batch_size or seq == max_seq:
            # Process the batch
            metadata, modifications = scraper.process_certificates(current_batch)
            
            # Check which IDs were actually valid by examining the HTML files
            valid_ids = set()
            for cert_id in current_batch:
                html_path = Path(f'data/raw/html/{cert_id}.html')
                if html_path.exists():
                    try:
                        with open(html_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            if "This certificate does not exist in our database" not in content and len(content) > 100:
                                valid_ids.add(cert_id)
                    except Exception as e:
                        logger.error(f"Error reading HTML file for {cert_id}: {str(e)}")
            
            # Only mark valid IDs as completed
            completed_ids.update(valid_ids)
            save_completed_ids(completed_ids)
            
            # Update consecutive failures based on valid certificates
            if valid_ids:
                # Reset consecutive failures if any valid certificate was found
                consecutive_failures = 0
                logger.info(f"Found {len(valid_ids)} valid certificates in batch of {len(current_batch)}")
                
                # The metadata and modifications should already contain only valid records
                all_metadata.extend(metadata)
                all_modifications.extend(modifications)
            else:
                # Increment consecutive failures by 1 since no valid certificates were found in this batch of 10 immediate consecutive certificates
                if int(current_batch[-1]) - int(current_batch[0]) <= batch_size:
                    consecutive_failures += 1
                    logger.info(f"No valid certificates found in batch ({current_batch[0]} to {current_batch[-1]})")
                    
                    # Check if we've hit the maximum failures
                    if consecutive_failures >= max_failures:
                        logger.info(f"Terminating processing for Region {region}, Year {year} after {consecutive_failures} consecutive unsuccessful batches")
                        break
            
            # Log progress
            logger.info(f"Region {region}, Year {year}, Processed through sequence {seq}/{max_seq}, Consecutive unsuccessful batches: {consecutive_failures}")
            logger.info(f"Updated completed IDs list, now contains {len(completed_ids)} IDs")
            
            # Clear the batch
            current_batch = []
                
    return all_metadata, all_modifications

def main():
    # Setup command line argument parser
    parser = argparse.ArgumentParser(description='CBFC Certificate Scraper')
    parser.add_argument('--region', type=int, help='Region code (1-9)')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2023)')
    parser.add_argument('--max-seq', type=int, default=100000, help='Maximum sequence number to try (default: 100000)')
    parser.add_argument('--max-failures', type=int, default=5, help='Number of consecutive failures before terminating (default: 50)')
    parser.add_argument('--all', action='store_true', help='Process all regions and years (2025-2024)')

    # Parse arguments
    args = parser.parse_args()

    # First, get fresh tokens
    logger.info("Getting fresh tokens...")
    if not get_tokens():
        logger.error("Failed to get tokens. Exiting.")
        sys.exit(1)

    # Initialize scraper
    scraper = CBFCScraper()
    
    # Process all regions and years
    all_metadata = []
    all_modifications = []
    
    # Check the arguments
    if args.all or (args.region is None and args.year is None):
        # Process all regions and years
        logger.info(f"Processing all regions for years 2025-2024 (max_seq={args.max_seq}, max_failures={args.max_failures})")
        
        for year in range(2025, 2024, -1):  # 2025 to 2024 in descending order
            for region in range(1, 10):  # 1 to 9 for all regions
                logger.info(f"Processing region {region} for year {year}")
                metadata, modifications = process_region_year(scraper, region, year, args.max_seq, args.max_failures)
                all_metadata.extend(metadata)
                all_modifications.extend(modifications)
    elif args.region is not None and args.year is not None:
        # Process specific region and year
        logger.info(f"Processing region {args.region} for year {args.year} (max_seq={args.max_seq}, max_failures={args.max_failures})")
        metadata, modifications = process_region_year(scraper, args.region, args.year, args.max_seq, args.max_failures)
        all_metadata.extend(metadata)
        all_modifications.extend(modifications)
    else:
        # If only one of region or year is provided
        parser.print_help()
        logger.error("Both --region and --year must be provided together.")
        sys.exit(1)
    
    logger.info(f"Processing complete! Processed {len(all_metadata)} certificates with {len(all_modifications)} modifications.")

if __name__ == "__main__":
    main()
