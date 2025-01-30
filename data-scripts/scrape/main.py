import logging
import json
import sys
from pathlib import Path
from typing import List
from getCookies import get_tokens
from scraper import CBFCScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# List of certificate IDs to process
CERTIFICATE_IDS = [
    "100090292400000155",
    # Add more IDs here
]

def main():
    # First, get fresh tokens
    logger.info("Getting fresh tokens...")
    if not get_tokens():
        logger.error("Failed to get tokens. Exiting.")
        sys.exit(1)

    # Initialize scraper
    scraper = CBFCScraper()
    
    # Use command line arguments if provided, otherwise use CERTIFICATE_IDS
    certificate_ids = sys.argv[1:] if len(sys.argv) > 1 else CERTIFICATE_IDS
    
    # Process certificates
    logger.info(f"Processing {len(certificate_ids)} certificate(s)...")
    metadata_records, modification_records = scraper.process_certificates(certificate_ids)

    # Save results
    if metadata_records:
        scraper.save_to_csv(metadata_records, 'metadata.csv')
    if modification_records:
        scraper.save_to_csv(modification_records, 'modifications.csv')

    logger.info("Processing complete!")

if __name__ == "__main__":
    main()