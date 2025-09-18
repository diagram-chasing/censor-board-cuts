import logging
import json
import sys
import argparse
from pathlib import Path
from typing import Set
from getCookies import get_tokens
from scraper import CBFCScraper
from parse import CBFCParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_completed_ids() -> Set[str]:
    """
    Load the set of certificate IDs that have already been processed.
    
    Returns:
        Set of completed certificate IDs
    """
    completed_file = Path('.completed.json')
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
    completed_file = Path('.completed.json')
    # Create parent directories if they don't exist
    completed_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(completed_file, 'w') as f:
            json.dump(list(sorted(completed_ids)), f)
    except Exception as e:
        logger.error(f"Error saving completed IDs: {str(e)}")

def process_region_year(scraper: CBFCScraper, region: int, year: int, max_seq: int = 100000, max_failures: int = 5) -> Set[str]:
    """
    Process certificates for a specific region and year with early termination.
    
    Args:
        scraper: The scraper instance
        region: Region code (1-9)
        year: Year to process
        max_seq: Maximum sequence number to try
        max_failures: Number of consecutive failures before terminating
    
    Returns:
        Set of valid certificate IDs
    """
    year_code = year + 900  # Convert year to required format
    consecutive_failures = 0
    
    # Load already processed IDs
    completed_ids = load_completed_ids()
    logger.debug(f"Loaded {len(completed_ids)} already processed IDs")
    
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

            # Check which IDs were actually valid by examining the HTML files
            valid_ids = set()
            for cert_id in current_batch:
                # Use scraper's method to check if HTML exists and is valid
                html_exists, _ = scraper.html_exists_and_valid(cert_id)
                
                # Fetch certificate if it doesn't exist locally
                if not html_exists:
                    logger.debug(f"Fetching certificate ID: {cert_id}")
                    result = scraper.get_certificate_details(cert_id)
                    if result:
                        valid_ids.add(cert_id)
                else:
                    # HTML exists and is valid
                    valid_ids.add(cert_id)
            
            # Only mark valid IDs as completed
            completed_ids.update(valid_ids)
            save_completed_ids(completed_ids)
            
            # Update consecutive failures based on valid certificates
            if valid_ids:
                # Reset consecutive failures if any valid certificate was found
                consecutive_failures = 0
                logger.debug(f"Found {len(valid_ids)} valid certificates in batch of {len(current_batch)}")
            else:
                # Increment consecutive failures by 1 since no valid certificates were found in this batch of 10 immediate consecutive certificates
                if int(current_batch[-1]) - int(current_batch[0]) <= batch_size:
                    consecutive_failures += 1
                    logger.debug(f"No valid certificates found in batch ({current_batch[0]} to {current_batch[-1]})")
                    
                    # Check if we've hit the maximum failures
                    if consecutive_failures >= max_failures:
                        logger.info(f"Terminating processing for Region {region}, Year {year} after {consecutive_failures} consecutive unsuccessful batches")
                        break
            
            # Log progress
            logger.debug(f"Region {region}, Year {year}, Processed through sequence {seq}/{max_seq}, Consecutive unsuccessful batches: {consecutive_failures}")
            logger.debug(f"Updated completed IDs list, now contains {len(completed_ids)} IDs")
            
            # Clear the batch
            current_batch = []
                
    return valid_ids

def load_certificate_urls_from_file() -> Set[str]:
    """
    Load certificate URLs from certificates.txt and extract certificate IDs.
    
    Returns:
        Set of certificate IDs extracted from URLs
    """
    certificates_file = Path('certificates.txt')
    certificate_ids = set()
    
    if not certificates_file.exists():
        logger.error("certificates.txt file not found")
        return certificate_ids
    
    try:
        with open(certificates_file, 'r') as f:
            for line in f:
                line = line.strip().strip('"')  # Remove quotes and whitespace
                if line and line.startswith('http'):
                    # Extract certificate ID from URL
                    # URLs can be in format: ...&i=CERTIFICATE_ID or ...&i=ENCODED_ID
                    if '&i=' in line:
                        cert_id = line.split('&i=')[-1]
                        certificate_ids.add(cert_id)
    except Exception as e:
        logger.error(f"Error reading certificates.txt: {str(e)}")
    
    logger.info(f"Loaded {len(certificate_ids)} certificate IDs from certificates.txt")
    return certificate_ids

def main():
    # Setup command line argument parser
    parser = argparse.ArgumentParser(description='CBFC Certificate Scraper')
    parser.add_argument('--region', type=int, help='Region code (1-9)')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2023)')
    parser.add_argument('--max-seq', type=int, default=100000, help='Maximum sequence number to try (default: 100000)')
    parser.add_argument('--max-failures', type=int, default=5, help='Number of consecutive failures before terminating (default: 50)')
    parser.add_argument('--all', action='store_true', help='Process all regions and years (2025-2024)')
    parser.add_argument('--parse-only', action='store_true', help='Skip scraping and only parse existing HTML files')
    parser.add_argument('--skip-parse', action='store_true', help='Skip parsing after scraping')
    parser.add_argument('--generate-ids', action='store_true', help='Generate certificate IDs using region/year pattern instead of using certificates.txt')

    # Parse arguments
    args = parser.parse_args()

    # If parse-only flag is set, skip scraping and only do parsing
    if args.parse_only:
        logger.info("Skipping scraping, only parsing existing HTML files")
        parse_certificates()
        return

    # First, get fresh tokens
    logger.debug("Getting fresh tokens...")
    if not get_tokens():
        logger.error("Failed to get tokens. Exiting.")
        sys.exit(1)

    # Initialize scraper
    scraper = CBFCScraper()
    
    valid_ids = []
    
    # Check if we should generate IDs using region/year pattern or use certificates.txt
    if args.generate_ids:
        # Use existing ID generation logic
        logger.info("Using ID generation based on region/year pattern")
        
        # Check the arguments for ID generation
        if args.all or (args.region is None and args.year is None):
            # Process all regions and years
            logger.debug(f"Processing all regions for years 2025-2024 (max_seq={args.max_seq}, max_failures={args.max_failures})")
            
            for year in range(2025, 2024, -1):  # 2025 to 2024 in descending order
                for region in range(1, 10):  # 1 to 9 for all regions
                    logger.info(f"Processing region {region} for year {year}")
                    region_ids = process_region_year(scraper, region, year, args.max_seq, args.max_failures)
                    valid_ids.extend(region_ids)
        elif args.region is not None and args.year is not None:
            # Process specific region and year
            logger.info(f"Processing region {args.region} for year {args.year} (max_seq={args.max_seq}, max_failures={args.max_failures})")
            region_ids = process_region_year(scraper, args.region, args.year, args.max_seq, args.max_failures)
            valid_ids.extend(region_ids)
        else:
            # If only one of region or year is provided
            parser.print_help()
            logger.error("Both --region and --year must be provided together when using --generate-ids.")
            sys.exit(1)
    else:
        # Use certificate IDs from certificates.txt (default behavior)
        logger.info("Using certificate IDs from certificates.txt")
        certificate_ids = load_certificate_urls_from_file()
        
        if not certificate_ids:
            logger.error("No valid certificate IDs found in certificates.txt. Use --generate-ids to generate IDs instead.")
            sys.exit(1)
        
        # Process each certificate ID from the file
        logger.info(f"Processing {len(certificate_ids)} certificate IDs from certificates.txt")
        for cert_id in certificate_ids:
            logger.debug(f"Processing certificate ID: {cert_id}")
            result = scraper.get_certificate_details(cert_id)
            if result:
                valid_ids.append(result)
    
    logger.debug(f"Scraping complete! Processed {len(valid_ids)} certificates.")
    
    # Parse certificates after scraping unless --skip-parse flag is set
    if not args.skip_parse:
        logger.info("Proceeding to parse certificates...")
        parse_certificates()

def parse_certificates():
    """Parse HTML files into CSV files using the CBFCParser"""
    logger.info("Starting to parse certificate HTML files...")
    parser = CBFCParser()
    meta_count, mod_count = parser.process_all_certificates()
    logger.info(f"Parsing complete. Generated {meta_count} metadata records and {mod_count} modification records.")

if __name__ == "__main__":
    main()
