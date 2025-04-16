#!/usr/bin/env python3
import os
import csv
import time
import argparse
from bs4 import BeautifulSoup
from pathlib import Path

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

def save_data_to_csv(all_data, output_file):
    """Save all data to CSV file with complete fieldnames"""
    # If no data, return
    if not all_data:
        return
    
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
    
    # Write all data at once
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
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
    parser.add_argument('--output-file', type=str, default='csv/categories.csv',
                        help='Output CSV file path (default: csv/categories.csv)')
    parser.add_argument('--failed-files', type=str, default='csv/failed_files.txt',
                        help='Output file for paths with no appropriate table (default: csv/failed_files.txt)')
    parser.add_argument('--limit', type=int, default=10000,
                        help='Maximum number of files to process (default: 10000, use 0 for no limit)')
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Directory containing HTML files
    input_dir = Path(args.input_dir)
    
    # Output CSV file
    output_file = args.output_file
    
    # Output failed files list
    failed_files_output = args.failed_files
    
    # Maximum number of files to process (0 means no limit)
    max_files = args.limit
    
    # Check if directory exists
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Error: Directory {input_dir} does not exist")
        return
    
    # Get all HTML files
    html_files = list(input_dir.glob('*.html'))
    total_files = len(html_files)
    
    # Apply file limit if specified
    if max_files > 0 and total_files > max_files:
        print(f"Limiting to {max_files} files out of {total_files} available")
        html_files = html_files[:max_files]
        total_files = len(html_files)
    else:
        print(f"Found {total_files} HTML files to process")
    
    # Create csv directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    os.makedirs(os.path.dirname(failed_files_output), exist_ok=True)
    
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
                print(f"Processing file {i}/{total_files} ({i/total_files*100:.1f}%)...")
            
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
            print(f"Error processing {file_path.name}: {e}")
            failed_files += 1
    
    # Now write all data to CSV
    print("\nWriting all data to CSV...")
    total_records = save_data_to_csv(all_data, output_file)
    
    # Save failed files list
    if failed_files_list:
        print(f"Saving list of {len(failed_files_list)} files with no appropriate table...")
        save_failed_files(failed_files_list, failed_files_output)
    
    # Final statistics
    elapsed_time = time.time() - start_time
    print("\nExtraction complete:")
    print(f"  Total files processed: {processed_files}/{total_files}")
    print(f"  Failed files: {failed_files}")
    print(f"  Files with no table: {len(failed_files_list)}")
    print(f"  Total records extracted: {total_records}")
    print(f"  Total processing time: {elapsed_time:.2f} seconds")
    print(f"  Average processing time per file: {elapsed_time/total_files:.4f} seconds")
    print(f"  Data saved to: {output_file}")
    print(f"  Failed files list saved to: {failed_files_output}")

if __name__ == "__main__":
    main()
