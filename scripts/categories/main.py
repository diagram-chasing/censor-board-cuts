#!/usr/bin/env python3
import os
import subprocess
import sys
import time
import logging
import argparse
from pathlib import Path
import datetime  # Added import for date handling

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

def read_last_fetched_date():
    """Read the date from .last-fetched-date file if it exists"""
    try:
        if os.path.exists('.last-fetched-date'):
            with open('.last-fetched-date', 'r') as f:
                date_str = f.read().strip()
                logging.debug(f"Last fetched date: {date_str}")
                return date_str
        else:
            logging.warning("No .last-fetched-date file found, using empty date (fetches all data)")
            return ''
    except Exception as e:
        logging.error(f"Error reading last fetched date: {e}")
        return ''

def run_script(script_name, args=None):
    """Run a script and return whether it succeeded"""
    try:
        cmd = [sys.executable, script_name]
        if args:
            cmd.extend(args)
            
        logging.debug(f"Running {script_name} {' '.join(args) if args else ''}")
        start_time = time.time()
        
        # Run the script and wait for it to complete
        result = subprocess.run(cmd, check=True)
        
        end_time = time.time()
        logging.debug(f"Successfully completed {script_name} in {end_time - start_time:.2f} seconds")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error running {script_name}: {e}")
        return False

def save_last_fetched_date():
    """Save the first date of the previous month in DD/MM/YYYY format"""
    try:
        today = datetime.datetime.now()
        # Get the 1st day of the current month
        first_of_current = today.replace(day=1)
        # Subtract one day to get the last day of the previous month
        last_of_previous = first_of_current - datetime.timedelta(days=1)
        # Get the 1st day of the previous month
        first_of_previous = last_of_previous.replace(day=1)
        # Format as DD/MM/YYYY
        date_str = first_of_previous.strftime("%d/%m/%Y")
        
        # Write to .last-fetched-date file
        with open('.last-fetched-date', 'w') as f:
            f.write(date_str)
        
        logging.info(f"Saved last fetched date: {date_str}")
    except Exception as e:
        logging.error(f"Error saving last fetched date: {e}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run the data processing pipeline')
    parser.add_argument('--skip-fetch', action='store_true', help='Skip the data fetching step')
    parser.add_argument('--characters', default='A-Z', help='Character range to fetch (default: A-Z)')
    parser.add_argument('--output-dir', default='raw', help='Output directory for fetched data (default: raw)')
    args = parser.parse_args()
    
    # Get the current directory (where this script is located)
    script_dir = Path(__file__).parent.absolute()
    os.chdir(script_dir)
    
    # Get the last fetched date to use as from_date for fetch.py
    from_date = read_last_fetched_date()
    
    # Step 1: Fetch film data from CBFC website (unless skipped)
    if args.skip_fetch:
        logging.info("Skipping data fetch step as requested")
        fetch_success = True
    else:
        # Include from_date in arguments when running fetch.py
        fetch_args = ["--characters", args.characters, "--output-dir", args.output_dir]
        if from_date:
            fetch_args.extend(["--from-date", from_date])
            
        fetch_success = run_script("fetch.py", fetch_args)
        if not fetch_success:
            logging.error("Data fetching failed. Pipeline stopped.")
            return

    # Step 2: Parse the fetched HTML files and extract film information
    parse_success = run_script("parse.py")
    if not parse_success:
        logging.error("HTML parsing failed. Pipeline stopped.")
        return

    # Step 3: Fetch and process categories for each film
    categories_success = run_script("categories.py")
    if not categories_success:
        logging.error("Category data fetching failed. Pipeline stopped.")
        return

    # Step 4: Extract structured data from category HTML files
    extract_success = run_script("extract.py")
    if not extract_success:
        logging.error("Data extraction failed. Pipeline stopped.")
        return

    logging.info("Data processing pipeline completed successfully!")
    
    # Save the first date of the previous month
    save_last_fetched_date()

if __name__ == "__main__":
    main() 
