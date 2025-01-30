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

def generate_certificate_ids() -> List[str]:
    """Generate certificate IDs for all regions from 2025 to 2015."""
    certificate_ids = []
    regions = range(1, 10)  # 1 to 9 for all regions
    
    for region in regions:
        for year in range(2025, 2014, -1):
            year_code = year + 900  # Convert year to required format
            # Generate 1000 sequential numbers for each region and year
            for seq in range(1, 1001):
                certificate_id = f"1000{region}0{year_code}{seq:08d}"
                certificate_ids.append(certificate_id)
    
    logger.info(f"Generated {len(certificate_ids)} certificate IDs")
    return certificate_ids

def main():
    # First, get fresh tokens
    logger.info("Getting fresh tokens...")
    if not get_tokens():
        logger.error("Failed to get tokens. Exiting.")
        sys.exit(1)

    # Initialize scraper
    scraper = CBFCScraper()
    
    # Use command line arguments if provided, otherwise generate certificate IDs
    certificate_ids = sys.argv[1:] if len(sys.argv) > 1 else generate_certificate_ids()
    
    # Process and save certificates
    logger.info(f"Processing {len(certificate_ids)} certificate(s)...")
    metadata_records, modification_records = scraper.process_certificates(certificate_ids)

    logger.info(f"Processing complete! Processed {len(metadata_records)} certificates with {len(modification_records)} modifications.")

if __name__ == "__main__":
    main()
