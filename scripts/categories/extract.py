#!/usr/bin/env python3
import os
import csv
import logging
import time
import argparse
import pandas as pd
import base64
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
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

def extract_table_data(html_content):
    """Extract data from table with appropriate classes"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the table with any of these classes
    table = soup.find('table', class_=lambda c: c and any(cls in c for cls in ['table-bordered', 'table-responsive']))
    if not table:
        return None
    
    # Create a single record from the table
    record = {}
    
    # Skip the first row if it's just a header spanning multiple columns
    rows = table.find_all('tr')
    start_idx = 0
    
    first_row = rows[0] if rows else None
    if first_row and first_row.find('td', colspan=True):
        start_idx = 1
    
    # Extract key-value pairs from each row
    for tr in rows[start_idx:]:
        cells = tr.find_all('td')
        if len(cells) >= 2:  # Ensure we have a key-value pair
            key = cells[0].text.strip().rstrip(':').strip()
            value = cells[1].text.strip()
            
            # If value starts with a colon, take only the string after the colon
            if value.startswith(':'):
                value = value[1:].strip()
            
            # Skip commented out or empty rows
            if key and not key.startswith('<!--'):
                record[key] = value
    
    return record

def save_data_to_csv(all_data, output_file, append=False):
    """Save all data to CSV file with complete fieldnames"""
    # If no data, return
    if not all_data:
        return 0
    
    # Determine all possible fields across all records
    all_fields = set()
    for record in all_data:
        all_fields.update(record.keys())
    
    # Define the priority columns in order
    priority_columns = [
        "Certificate No", 
        "Movie Name", 
        "Movie Language", 
        "Movie Category", 
        "Certificate Date", 
        "Certified Length", 
        "Certified by Regional Office", 
        "Name of Applicant", 
        "Name of Producer"
    ]
    
    # Create ordered fieldnames with priority columns first
    fieldnames = []
    
    # Add priority columns first (if they exist in data)
    for col in priority_columns:
        if col in all_fields:
            fieldnames.append(col)
            all_fields.remove(col)
    
    # Add remaining fields in sorted order
    fieldnames.extend(sorted(list(all_fields)))
    
    # Write mode based on append flag
    mode = 'a' if append else 'w'
    
    # Write header only if creating a new file or not appending
    write_header = not (append and os.path.exists(output_file) and os.path.getsize(output_file) > 0)
    
    # Write all data at once
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(all_data)
    
    return len(all_data)

def save_failed_files(failed_files_list, output_file):
    """Save the list of files with no appropriate table to a file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        for file_path in failed_files_list:
            f.write(f"{file_path}\n")
    return len(failed_files_list)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Extract data from HTML files and save to CSV.')
    parser.add_argument('--input-dir', type=str, default='raw/categories',
                        help='Directory containing HTML files (default: raw/categories)')
    parser.add_argument('--output-file', type=str, default='../../data/raw/categories.csv',
                        help='Output CSV file path (default: ../../data/raw/categories.csv)')
    parser.add_argument('--failed-files', type=str, default='.failed_files.txt',
                        help='Output file for paths with no appropriate table (default: .failed_files.txt)')
    parser.add_argument('--limit', type=int, default=0,
                        help='Maximum number of files to process (default: 0 (no))')
    parser.add_argument('--recent-file', type=str, default='../../data/raw/recent.csv',
                        help='CSV file containing recent recids to process (default: ../../data/raw/recent.csv)')
    parser.add_argument('--append', action='store_true', default=True,
                        help='Append to existing output file instead of overwriting')
    parser.add_argument('--all', action='store_true', default=False,
                        help='Process all HTML files in input directory, ignoring the recent-file filter')
    return parser.parse_args()

def deduplicate_and_sort_csv(csv_path):
    """Deduplicate and sort the CSV file"""
    if not os.path.exists(csv_path):
        logger.error(f"Error: {csv_path} does not exist")
        return
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Get row count before deduplication
        original_count = len(df)
        
        # Deduplicate based on all columns
        df = df.drop_duplicates()
        
        # Get row count after deduplication
        deduplicated_count = len(df)
        
        # Sort by Movie Name and Certificate No in descending order if the column exists
        if "Movie Name" in df.columns and "Certificate No" in df.columns:
            df = df.sort_values(by=["Movie Name", "Certificate No"], ascending=False)
        
        # Write back to the same file
        df.to_csv(csv_path, index=False)
        
        logger.debug(f"Deduplicated {original_count - deduplicated_count} rows")
        logger.debug(f"Sorted by 'Movie Name' and 'Certificate No' in descending order")
        logger.debug(f"Final CSV contains {deduplicated_count} rows")
    
    except Exception as e:
        logger.error(f"Error during deduplication and sorting: {e}")

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

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Directory containing HTML files
    input_dir = Path(args.input_dir)
    
    # Output CSV file
    output_file = args.output_file
    
    # Output failed files list
    failed_files_output = args.failed_files
    
    # Recent recids file
    recent_file = args.recent_file
    
    # Maximum number of files to process (0 means no limit)
    max_files = args.limit
    
    # Check if directory exists
    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(f"Error: Directory {input_dir} does not exist")
        return
    
    # Get all HTML files or filter by recent recids
    html_files = []
    
    if args.all:
        # Process all HTML files in the input directory
        html_files = list(input_dir.glob('*.html'))
        logger.debug(f"Found {len(html_files)} HTML files in directory")
        
        total_files = len(html_files)
        
        if total_files == 0:
            logger.debug("No HTML files found, nothing to process")
            return
            
        # Apply file limit if specified
        if max_files > 0 and total_files > max_files:
            logger.debug(f"Limiting to {max_files} files out of {total_files} available")
            html_files = html_files[:max_files]
            total_files = len(html_files)
    else:
        # Check if recent file exists
        if not os.path.exists(recent_file):
            logger.error(f"Error: Recent file {recent_file} does not exist")
            return
        
        # Load recent recids from the CSV file
        recent_recids = set()
        try:
            with open(recent_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if 'recid' in reader.fieldnames:
                    for row in reader:
                        if 'recid' in row and row['recid']:
                            recent_recids.add(row['recid'])
                elif 'URL' in reader.fieldnames:
                    for row in reader:
                        if 'URL' in row and row['URL']:
                            recid = extract_recid(row['URL'])
                            if recid:
                                recent_recids.add(recid)
                else:
                    logger.error(f"Error: Neither 'recid' nor 'URL' column found in {recent_file}")
                    return
            
            logger.debug(f"Loaded {len(recent_recids)} recent recids from {recent_file}")
        except Exception as e:
            logger.error(f"Error loading recent recids: {e}")
            return
        
        if not recent_recids:
            logger.debug("No recent recids found, nothing to process")
            return
        
        # Filter HTML files to only those matching recent recids
        html_files = []
        for recid in recent_recids:
            file_path = input_dir / f"{recid}.html"
            if file_path.exists():
                html_files.append(file_path)
        
        total_files = len(html_files)
        logger.debug(f"Found {total_files} HTML files matching recent recids")
        
        if total_files == 0:
            logger.debug("No matching HTML files found, nothing to process")
            return
        
        # Apply file limit if specified
        if max_files > 0 and total_files > max_files:
            logger.debug(f"Limiting to {max_files} files out of {total_files} available")
            html_files = html_files[:max_files]
            total_files = len(html_files)
    
    # Create directories if they don't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Counters for reporting
    processed_files = 0
    failed_files = 0
    start_time = time.time()
    
    # Store all data in memory before writing to CSV
    all_data = []
    failed_files_list = []
    
    # Process all files first
    for i, file_path in enumerate(html_files, 1):
        try:
            if i % 50 == 0 or i == 1 or i == total_files:
                logger.debug(f"Processing file {i}/{total_files} ({i/total_files*100:.1f}%)...")
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                content = file.read()
            
            record = extract_table_data(content)
            if not record:
                failed_files_list.append(str(file_path))
                failed_files += 1
                continue
            
            # Add filename as a field
            record['source_file'] = file_path.name
            all_data.append(record)
            
            processed_files += 1
            
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")
            failed_files += 1
    
    # Now write all data to CSV
    logger.debug("\nWriting data to CSV...")
    total_records = save_data_to_csv(all_data, output_file, args.append)
    
    # Deduplicate and sort the output file
    logger.debug("\nDeduplicating and sorting the output file...")
    deduplicate_and_sort_csv(output_file)
    
    # Save failed files list
    if failed_files_list:
        logger.debug(f"Saving list of {len(failed_files_list)} files with no appropriate table...")
        save_failed_files(failed_files_list, failed_files_output)
    
    # Final statistics
    elapsed_time = time.time() - start_time
    logger.debug("\nExtraction complete:")
    logger.debug(f"  Total files processed: {processed_files}/{total_files}")
    logger.debug(f"  Failed files: {failed_files}")
    logger.debug(f"  Files with no table: {len(failed_files_list)}")
    logger.debug(f"  Total new records extracted: {total_records}")
    logger.debug(f"  Total processing time: {elapsed_time:.2f} seconds")
    logger.debug(f"  Average processing time per file: {elapsed_time/total_files:.4f} seconds")
    logger.debug(f"  Data saved to: {output_file}")
    logger.debug(f"  Failed files list saved to: {failed_files_output}")

if __name__ == "__main__":
    main()
