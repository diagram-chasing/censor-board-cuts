#!/usr/bin/env python3
import os
import csv
import logging
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Output CSV file
output_file = '../../data/raw/recent.csv'

# Create a set to store unique film entries
unique_films = set()

# Process each HTML file
for filename in sorted(os.listdir('raw/')):
    if not filename.endswith('.html'):
        continue
        
    logger.debug(f"Processing {filename}...")
    
    try:
        # Skip small HTML files (they appear to be error pages)
        file_size = os.path.getsize(os.path.join('raw/', filename))
        if file_size < 1000:  # Skip files smaller than 1KB
            logger.debug(f"Skipping {filename} (too small, likely an error page)")
            continue
            
        with open(os.path.join('raw/', filename), 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
            
        # Parse HTML
        soup = BeautifulSoup(content, 'html.parser')
        
        # Find the table with films
        table = soup.find('table', id='example')
        if not table:
            logger.debug(f"No film table found in {filename}")
            continue
            
        # Process each row
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 2:  # Make sure row has enough cells
                # Extract film name and URL
                film_cell = cells[0]
                link = film_cell.find('a')
                
                if link:
                    film_name = link.text.strip()
                    url = link.get('href', '')
                    url = "https://cbfcindia.gov.in/cbfcAdmin/" + url
                    
                    # Extract year from second column
                    year = cells[1].text.strip()
                    
                    # Add to unique films set (as a tuple since sets require hashable elements)
                    unique_films.add((film_name, year, url))
                    
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")

# Write unique entries to CSV
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Film Name', 'Year', 'URL'])
    
    # Sort by film name and year for consistent output
    for film_name, year, url in sorted(unique_films):
        writer.writerow([film_name, year, url])

logger.debug(f"Extraction complete. {len(unique_films)} unique films saved to {output_file}")
