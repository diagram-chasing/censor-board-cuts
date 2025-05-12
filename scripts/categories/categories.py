import csv
import base64
import logging
import os
import requests
import time
from urllib.parse import urlparse, parse_qs
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_recid(url):
    """Extract and decode the recid parameter from URL."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    recid = query_params.get('recid', [''])[0]
    try:
        # Decode base64 and convert to string
        decoded = base64.b64decode(recid).decode('utf-8')
        # Replace forward slashes with underscores
        decoded = decoded.replace('/', '_')
        return decoded
    except:
        logger.error(f"Error decoding recid for URL: {url}")
        return None

def fetch_and_save(url, output_dir):
    """Fetch URL content and save to file."""
    try:
        recid = extract_recid(url)
        if not recid:
            return False
            
        filename = f"{recid}.html"
        filepath = os.path.join(output_dir, filename)
        
        # Skip if file already exists
        if os.path.exists(filepath):
            logger.debug(f"File {filename} already exists, skipping...")
            return True
        
        response = requests.get(url)
        response.raise_for_status()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)
            
        # Sleep for 0.1 seconds after successful request
        time.sleep(0.1)
        return True
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        return False

def main():
    # Create output directory if it doesn't exist
    output_dir = Path("raw/categories")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read CSV and process URLs
    with open('../../data/raw/recent.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        total = 0
        success = 0
        
        for row in reader:
            url = row.get('URL')
            if url:
                total += 1
                if fetch_and_save(url, output_dir):
                    success += 1
                
                # Print progress every 100 files
                if total % 100 == 0:
                    logger.debug(f"Processed {total} files, {success} successful")
    
    logger.debug(f"\nProcessing complete!")
    logger.debug(f"Total files processed: {total}")
    logger.debug(f"Successfully saved: {success}")

if __name__ == "__main__":
    main()
