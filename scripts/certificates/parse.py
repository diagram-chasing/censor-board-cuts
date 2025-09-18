import json
import re
import csv
import logging
import pandas as pd
from typing import Dict, Optional, Tuple, List
from bs4 import BeautifulSoup
from pathlib import Path
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('parse.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CBFCParser:
    def __init__(self):
        self.html_dir = Path('raw/html')
        self.csv_dir = Path('../../data/raw/')
        self.csv_dir.mkdir(exist_ok=True)
        
        # Track processed files
        self.processed_file = Path('.processed.json')
        self.processed_ids = set()
        if self.processed_file.exists():
            try:
                with open(self.processed_file, 'r') as f:
                    self.processed_ids = set(json.load(f))
                logger.debug(f"Loaded {len(self.processed_ids)} previously processed IDs")
            except Exception as e:
                logger.error(f"Error loading processed IDs: {str(e)}")

    def _sanitize_filename(self, certificate_id: str) -> str:
        """Sanitize certificate ID for use as filename by replacing problematic characters"""
        return certificate_id.replace('/', '_').replace('=', '_eq_').replace('+', '_plus_')
    
    def _unsanitize_filename(self, filename: str) -> str:
        """Convert sanitized filename back to original certificate ID"""
        # Remove .html extension first
        cert_id = filename.replace('.html', '')
        # Reverse the sanitization in correct order (most specific first)
        cert_id = cert_id.replace('_plus_', '+').replace('_eq_', '=').replace('_', '/')
        return cert_id

    def clean_text(self, text: str) -> str:
        """Clean text by removing HTML and normalizing whitespace"""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Normalize whitespace
        text = ' '.join(text.split())
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,;:()\-]', '', text)
        return text.strip()

    def parse_credits_section(self, html_content: str) -> Dict:
        """Parse the credits section using BeautifulSoup"""
        credits = {}
        if not html_content:
            return credits
            
        soup = BeautifulSoup(html_content, 'html.parser')
        credit_divs = soup.find_all('div', id='castCredit')
        
        for div in credit_divs:
            type_div = div.find('div', id='castCreditType')
            desc_div = div.find('div', id='castCreditDescription')
            
            if type_div and desc_div:
                role = type_div.get_text().strip().rstrip(':')
                value = desc_div.get_text().strip()
                credits[f'credit_{role.lower().replace(" ", "_")}'] = value
                
        return credits

    def parse_modifications_table(self, soup: BeautifulSoup) -> list:
        """Parse the modifications/cuts table"""
        modifications = []
        table = soup.find('table')
        if not table:
            return modifications

        rows = table.find_all('tr')[1:]  # Skip header row
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 5:
                modification = {
                    'cut_no': int(cells[0].get_text().strip()),
                    'description': self.clean_text(cells[1].get_text()),
                    'deleted': self.clean_text(cells[2].get_text()),
                    'replaced': self.clean_text(cells[3].get_text()),
                    'inserted': self.clean_text(cells[4].get_text())
                }
                modifications.append(modification)
        return modifications

    def parse_endorsement_section(self, html_content: str) -> Dict:
        """Parse the endorsement section using BeautifulSoup"""
        endorsement = {}
        if not html_content:
            return endorsement
            
        soup = BeautifulSoup(html_content, 'html.parser')
        endorsement_div = soup.find('div', id='qr-redirect-endorsment')
        
        if endorsement_div:
            divs = endorsement_div.find_all('div', recursive=False)
            for div in divs:
                text = self.clean_text(div.get_text())
                if 'File No.' in text:
                    endorsement['file_no'] = text.split(':')[-1].strip()
                elif 'Film Name' in text:
                    endorsement['film_name_full'] = text.split(':')[-1].strip()
                elif 'Cert No.' in text:
                    match = re.search(r'Cert No\.\s+([^\s]+)\s+Dated\s+([^\s]+)', text)
                    if match:
                        endorsement['cert_no'] = match.group(1).strip()
                        endorsement['cert_date'] = match.group(2).strip()
                elif 'Actual Duration' in text:
                    match = re.search(r'will be\s+([^\s]+)\s+MM\.SS', text)
                    if match:
                        endorsement['final_duration'] = match.group(1).strip()

            modifications = self.parse_modifications_table(endorsement_div)
            if modifications:
                endorsement['modifications'] = modifications

        return endorsement

    def extract_main_data(self, data_array: list) -> Dict:
        """Extract basic information from the main data array"""
        main_info = {}
        
        for item in data_array:
            if isinstance(item, list) and len(item) > 5:
                main_data = item
                break
        else:
            return main_info

        required_fields = {
            'certificate_id': -1,
            'title': None,
            'category': None,
            'language': None,
            'format': None,
            'duration': None,
            'applicant': None,
            'certifier': None,
            'synopsis': None
        }

        for i, item in enumerate(main_data):
            if isinstance(item, str):
                if item.endswith('MM.SS'):
                    required_fields['duration'] = i
                elif any(x in item.lower() for x in ['theatrical', 'video']):
                    required_fields['category'] = i
                elif any(x in item.lower() for x in ['malayalam', 'hindi', 'tamil']):
                    required_fields['language'] = i
                elif 'long' in item.lower():
                    required_fields['format'] = i
                elif '(' in item and ')' in item and len(item) > 20:
                    required_fields['applicant'] = i
                elif 'E.O.' in item or 'CBFC' in item:
                    required_fields['certifier'] = i
                elif len(item) > 50:
                    required_fields['synopsis'] = i
                elif item.isupper() and len(item) < 30:
                    required_fields['title'] = i

        for key, index in required_fields.items():
            if index is not None and index < len(main_data):
                main_info[key] = main_data[index]

        return main_info

    def is_html_valid(self, html_content: str) -> bool:
        """Check if HTML content is valid and contains necessary data"""
        if not html_content or len(html_content) < 100:  # Too small to be valid
            return False
            
        if "//OK" not in html_content:
            return False
        
        if "This certificate does not exist in our database" in html_content:
            return False
            
        return True

    def parse_certificate_details(self, certificate_id: str) -> Optional[Dict]:
        """Parse certificate details from a local HTML file"""
        try:
            safe_filename = self._sanitize_filename(certificate_id)
            html_path = self.html_dir / f"{safe_filename}.html"
            
            if not html_path.exists():
                logger.warning(f"HTML file not found for certificate ID: {certificate_id}")
                return None
                
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            if not self.is_html_valid(html_content):
                logger.warning(f"Invalid HTML content for certificate ID: {certificate_id}")
                return None
                
            # Parse the data
            data_parts = html_content.split('//OK')[1].strip()
            parsed_data = eval(data_parts)
            
            certificate_info = self.extract_main_data(parsed_data)
            
            for item in parsed_data:
                if isinstance(item, list):
                    for subitem in item:
                        if isinstance(subitem, str):
                            if 'castCredit' in subitem:
                                credits = self.parse_credits_section(subitem)
                                certificate_info.update(credits)
                            elif 'endorsementHeading' in subitem:
                                endorsement = self.parse_endorsement_section(subitem)
                                certificate_info.update(endorsement)

            if certificate_info.get('certificate_id') and certificate_info.get('title'):
                logger.debug(f"Successfully parsed: {certificate_info['title']} (Certificate ID: {certificate_id})")
                return certificate_info
            elif certificate_info.get('certificate_id') and certificate_info.get('film_name'):
                logger.debug(f"Successfully parsed: {certificate_info['film_name']} (Certificate ID: {certificate_id})")
                return certificate_info
            elif certificate_info.get('certificate_id') and certificate_info.get('film_name_full'):
                logger.debug(f"Successfully parsed: {certificate_info['film_name_full']} (Certificate ID: {certificate_id})")
                return certificate_info
            else:
                logger.debug(f"Incomplete data for certificate ID {certificate_id}")
                logger.debug(f"Following certificate details extracted: {certificate_info}")
                return None

        except Exception as e:
            logger.error(f"Error parsing certificate ID {certificate_id}: {str(e)}")
            return None

    def get_already_processed_ids(self) -> set:
        """Get the set of certificate IDs that have already been processed from metadata.csv"""
        metadata_path = self.csv_dir / 'metadata.csv'
        ids_in_csv = set()
        
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if 'id' in row and row['id']:
                            ids_in_csv.add(row['id'])
                logger.debug(f"Found {len(ids_in_csv)} certificate IDs in existing metadata.csv")
            except Exception as e:
                logger.error(f"Error reading metadata CSV: {str(e)}")
        
        return ids_in_csv

    def sort_and_deduplicate_csv(self, csv_path: str, sort_fields: List[str]):
        """Sort and deduplicate a CSV file by a specific field"""
        if not Path(csv_path).exists():
            logger.warning(f"CSV file {csv_path} does not exist, cannot sort/deduplicate")
            return
            
        try:
            # Read all records
            df = pd.read_csv(csv_path)

            # Store number of rows before deduplication
            rows_before = len(df)

            # Deduplicate rows based on all fields
            df = df.drop_duplicates()

            # Sort by the specified field
            df = df.sort_values(by=sort_fields, ascending=False)

            # Write dataframe to file
            df.to_csv(csv_path, index=False)
                
            logger.debug(f"Sorted and deduplicated {csv_path} by {sort_fields} (removed {rows_before - len(df)} duplicates)")
            
        except Exception as e:
            logger.error(f"Error sorting/deduplicating {csv_path}: {str(e)}")

    def process_all_certificates(self) -> Tuple[int, int]:
        """Process all HTML files in the data directory"""
        # Output files
        metadata_path = self.csv_dir / 'metadata.csv'
        modifications_path = self.csv_dir / 'modifications.csv'
        
        # Define base set of columns
        base_metadata_fields = [
            'id', 'certificate_id', 'title', 'category', 'language', 'format', 'duration',
            'applicant', 'certifier', 'synopsis', 'file_no', 'film_name_full',
            'cert_no', 'cert_date', 'final_duration'
        ]
        
        # Get already processed IDs from metadata.csv and processed.json
        csv_processed_ids = self.get_already_processed_ids()
        self.processed_ids.update(csv_processed_ids)
        
        # Get all metadata fields including credit fields from existing CSV
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    metadata_fields = header
            except Exception as e:
                logger.error(f"Error reading metadata CSV header: {str(e)}")
                metadata_fields = base_metadata_fields
        else:
            metadata_fields = base_metadata_fields
        
        modification_fields = [
            'id', 'certificate_id', 'film_name', 'cut_no', 'description',
            'deleted', 'replaced', 'inserted'
        ]
        
        # Get all HTML files
        html_files = list(self.html_dir.glob('*.html'))
        total_files = len(html_files)
        
        # Filter out already processed files
        files_to_process = [f for f in html_files if self._unsanitize_filename(f.name) not in self.processed_ids]
        logger.debug(f"Found {total_files} HTML files, {len(files_to_process)} not yet processed")
        
        metadata_records = []
        modification_records = []
        processed_count = 0
        
        # Process each HTML file
        for html_file in files_to_process:
            # Convert sanitized filename back to original certificate ID
            certificate_id = self._unsanitize_filename(html_file.name)
            
            result = self.parse_certificate_details(certificate_id)
            if not result:
                continue
            
            # Clean all text fields
            for key, value in result.items():
                if isinstance(value, str):
                    result[key] = self.clean_text(value)
            
            # Separate modifications from metadata
            modifications = result.pop('modifications', [])

            # Add id field to metadata
            result['id'] = certificate_id
            
            # Update metadata fields with any new fields found in this certificate
            new_fields = [key for key in result.keys() if key not in metadata_fields]
            if new_fields:
                metadata_fields.extend(new_fields)
                logger.debug(f"Added {len(new_fields)} new fields to metadata: {', '.join(new_fields)}")
            
            # Ensure all required fields exist
            for field in metadata_fields:
                if field not in result:
                    result[field] = ''
            
            # Write metadata to CSV - if new fields were added, rewrite the entire file
            if new_fields and metadata_path.exists():
                # Read existing data
                existing_data = []
                with open(metadata_path, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Add empty values for new fields
                        for field in new_fields:
                            row[field] = ''
                        existing_data.append(row)
                
                # Write everything back with new header
                with open(metadata_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=metadata_fields)
                    writer.writeheader()
                    for row in existing_data:
                        writer.writerow(row)
                    writer.writerow(result)
            else:
                # Normal append operation
                write_header = not metadata_path.exists()
                with open(metadata_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=metadata_fields)
                    if write_header:
                        writer.writeheader()
                    writer.writerow(result)
            
            metadata_records.append(result)
            
            # Handle modifications if present
            if modifications:
                write_header = not modifications_path.exists()
                for mod in modifications:
                    mod_record = {
                        'id': certificate_id,
                        'certificate_id': result.get('certificate_id', ''),
                        'film_name': result.get('title', ''),
                        **mod
                    }
                    
                    # Ensure all required fields exist
                    for field in modification_fields:
                        if field not in mod_record:
                            mod_record[field] = ''
                    
                    with open(modifications_path, 'a', newline='', encoding='utf-8') as modifications_file:
                        modifications_writer = csv.DictWriter(modifications_file, fieldnames=modification_fields)
                        if write_header:
                            modifications_writer.writeheader()
                            write_header = False
                        modifications_writer.writerow(mod_record)
                    modification_records.append(mod_record)
            
            # Mark as processed
            self.processed_ids.add(certificate_id)
            with open(self.processed_file, 'w') as f:
                json.dump(list(self.processed_ids), f)
            
            processed_count += 1
            if processed_count % 100 == 0:
                logger.debug(f"Processed {processed_count}/{len(files_to_process)} files")
        
        # Sort and deduplicate the CSV files
        if metadata_path.exists():
            self.sort_and_deduplicate_csv(str(metadata_path), ['film_name_full', 'certificate_id'])
        
        if modifications_path.exists():
            self.sort_and_deduplicate_csv(str(modifications_path), ['film_name', 'certificate_id', 'cut_no'])
        
        logger.debug(f"Completed processing. Parsed {len(metadata_records)} certificates with {len(modification_records)} modifications.")
        return len(metadata_records), len(modification_records)

def main():
    parser = CBFCParser()
    meta_count, mod_count = parser.process_all_certificates()
    logger.info(f"Parsing complete. Generated {meta_count} metadata records and {mod_count} modification records.")

if __name__ == "__main__":
    main()
