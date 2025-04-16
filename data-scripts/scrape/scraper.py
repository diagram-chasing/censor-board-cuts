import requests
import json
import re
import csv
import logging
import uuid
import time
import urllib3
from typing import Dict, Optional, Tuple, List
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class CBFCScraper:
    def __init__(self, cookies_dir: str = None):
        self.session = requests.Session()
        # Disable SSL verification
        self.session.verify = False
        self.base_url = "https://www.ecinepramaan.gov.in"
        
        # Get the directory containing the cookies and headers files
        cookies_dir = cookies_dir or Path(__file__).parent
        cookies_path = Path(cookies_dir) / 'cookies.json'
        headers_path = Path(cookies_dir) / 'headers.json'
        
        # Load cookies and headers
        try:
            with open(cookies_path, 'r') as f:
                cookies = json.load(f)
            with open(headers_path, 'r') as f:
                headers = json.load(f)
        except FileNotFoundError:
            raise Exception("Cookies or headers files not found. Please run getCookies.py first.")
        
        # Headers copied directly from working curl request
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0',
            'Content-Type': 'text/x-gwt-rpc; charset=utf-8',
            'X-GWT-Permutation': '1',
            'DTMN_SERVICE': 'TRUE',
            'DTMN_SESSIONID': headers['headers']['DTMN_SESSIONID'],
            'DTMN_SESSION_VALIDATION': '0',
            'Origin': 'https://www.ecinepramaan.gov.in',
        })
        
        # Disable automatic Accept-Encoding header addition
        self.session.headers.pop('Accept-Encoding', None)

        # Set cookies from cookies.json
        for cookie_name, cookie_value in cookies['cookies'].items():
            self.session.cookies.set(cookie_name, cookie_value, domain='ecinepramaan.gov.in')

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
            'id': -1,
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

    def _to_curl(self, url: str, payload: str) -> str:
        """Convert request to curl command for debugging"""
        # Convert cookies to Cookie header
        cookie_header = '; '.join([f"{k}={v}" for k, v in self.session.cookies.items()])
        headers = dict(self.session.headers)
        if cookie_header:
            headers['Cookie'] = cookie_header
            
        headers_str = ' '.join([f"-H '{k}: {v}'" for k, v in headers.items()])
        return f"curl -X POST '{url}' {headers_str} -d '{payload}'"

    def is_html_valid(self, html_content: str) -> bool:
        """Check if HTML content is valid and contains necessary data"""
        if not html_content or len(html_content) < 100:  # Too small to be valid
            return False
            
        if "//OK" not in html_content:
            return False
        
        if "This certificate does not exist in our database" in html_content:
            return False
            
        return True
            
    def html_exists_and_valid(self, certificate_id: str) -> Tuple[bool, Optional[str]]:
        """Check if HTML for the certificate ID exists and is valid"""
        html_path = Path('data/raw/html') / f"{certificate_id}.html"
        
        if not html_path.exists():
            return False, None
            
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            if self.is_html_valid(html_content):
                logger.info(f"Using existing valid HTML for certificate ID: {certificate_id}")
                return True, html_content
            else:
                logger.warning(f"Existing HTML for certificate ID {certificate_id} is invalid")
                return False, None
        except Exception as e:
            logger.error(f"Error reading existing HTML for certificate ID {certificate_id}: {str(e)}")
            return False, None

    def get_certificate_details(self, certificate_id: str) -> Optional[Dict]:
        """Fetch and parse certificate details for a given certificate ID"""
        try:
            # Check if valid HTML already exists
            html_exists, existing_html = self.html_exists_and_valid(certificate_id)
            
            if not html_exists:
                logger.debug(f"Fetching details for certificate ID: {certificate_id}")
                
                url = f"{self.base_url}/cbfc/cbfc/certificate/qrRedirect/client/QRRedirect"
                payload = f'7|0|6|{self.base_url}/cbfc/cbfc.Cbfc/|A425282E16D492E942BAD73170B377F8|cbfc.certificate.qrRedirect.shared.QRRedirect_Srv|getDefaultValues|java.lang.String/2004016611|{certificate_id}|1|2|3|4|1|5|6|'
                
                logger.debug("Equivalent curl command:")
                logger.debug(self._to_curl(url, payload))
                
                response = self.session.post(url, data=payload)
                response.raise_for_status()
                logger.debug(response.text)
                
                # Save the raw HTML response
                html_dir = Path('data/raw/html')
                html_dir.mkdir(parents=True, exist_ok=True)
                html_file = html_dir / f"{certificate_id}.html"
                with open(html_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Saved raw HTML for certificate ID: {certificate_id}")
                
                if "//OK" not in response.text:
                    logger.error(f"Certificate ID {certificate_id}: Did not receive OK response")
                    logger.debug(f"Response content: {response.text[:500]}")  # Log first 500 chars
                    return None
                    
                data_parts = response.text.split('//OK')[1].strip()
            else:
                # Use existing valid HTML
                data_parts = existing_html.split('//OK')[1].strip()
            
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

            if certificate_info.get('id') and certificate_info.get('title'):
                logger.info(f"Successfully scraped: {certificate_info['title']} (ID: {certificate_info['id']})")
                return certificate_info
            else:
                logger.warning(f"Incomplete data for certificate ID {certificate_id}")
                return None

        except Exception as e:
            logger.error(f"Error processing certificate ID {certificate_id}: {str(e)}")
            return None

    def process_certificates(self, certificate_ids: List[str]) -> Tuple[List[Dict], List[Dict]]:
        """Process a list of certificate IDs and save results"""
        metadata_records = []
        modification_records = []
        
        # Create output directories
        output_dir = Path('data/raw')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load completed IDs (separate from progress tracking)
        completed_file = output_dir / 'completed.json'
        completed_ids = set()
        if completed_file.exists():
            try:
                with open(completed_file, 'r') as f:
                    completed_ids = set(json.load(f))
            except Exception as e:
                logger.error(f"Error loading completed IDs: {str(e)}")
        
        # Update remaining_ids to exclude already completed ones
        remaining_ids = [cert_id for cert_id in certificate_ids if cert_id not in completed_ids]
        
        # Check if there are any certificates left to process after filtering
        if len(remaining_ids) != len(certificate_ids):
            logger.info(f"Skipping {len(certificate_ids) - len(remaining_ids)} already processed certificates")
        
        if not remaining_ids:
            logger.info("All certificate IDs in this batch have already been processed")
            return metadata_records, modification_records
        
        # Load progress (for resumption within a batch)
        last_processed_id = self._load_progress(output_dir)
        if last_processed_id and last_processed_id in remaining_ids:
            try:
                start_idx = remaining_ids.index(last_processed_id) + 1
                remaining_ids = remaining_ids[start_idx:]
            except ValueError:
                pass  # If not found, process all remaining
        
        logger.info(f"Resuming from after ID: {last_processed_id}")
        logger.info(f"Remaining certificates to process: {len(remaining_ids)}")
        
        # Further filter out IDs that already have valid HTML files
        filtered_ids = []
        for cert_id in remaining_ids:
            exists, _ = self.html_exists_and_valid(cert_id)
            if not exists:
                filtered_ids.append(cert_id)
            else:
                # If HTML exists but certificate not marked as completed,
                # we should still process it to extract the data
                if cert_id not in completed_ids:
                    filtered_ids.append(cert_id)
                    logger.info(f"HTML exists for {cert_id} but not marked as completed. Will process.")

        skipped_count = len(remaining_ids) - len(filtered_ids)
        if skipped_count > 0:
            logger.info(f"Skipping {skipped_count} certificates that already have valid HTML files and are completed")

        remaining_ids = filtered_ids
        
        metadata_path = output_dir / 'metadata.csv'
        modifications_path = output_dir / 'modifications.csv'
        
        # Define base set of columns
        base_metadata_fields = [
            'id', 'title', 'category', 'language', 'format', 'duration',
            'applicant', 'certifier', 'synopsis', 'file_no', 'film_name_full',
            'cert_no', 'cert_date', 'final_duration'
        ]
        
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
            'certificate_id', 'film_name', 'cut_no', 'description',
            'deleted', 'replaced', 'inserted'
        ]
        
        # Process each certificate
        for cert_id in remaining_ids:
            try:
                result = self.get_certificate_details(cert_id)
                if not result:
                    continue
                
                # Clean all text fields
                for key, value in result.items():
                    if isinstance(value, str):
                        result[key] = self.clean_text(value)
                
                # Separate modifications from metadata
                modifications = result.pop('modifications', [])
                
                # Update metadata fields with any new fields found in this certificate
                new_fields = [key for key in result.keys() if key not in metadata_fields]
                if new_fields:
                    metadata_fields.extend(new_fields)
                    logger.info(f"Added {len(new_fields)} new fields to metadata: {', '.join(new_fields)}")
                
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
                            'certificate_id': result.get('id', ''),
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
                
                # Log progress
                logger.info(f"Processed certificate ID: {cert_id}")
                
                # Update completed ids
                completed_ids.add(cert_id)
                with open(completed_file, 'w') as f:
                    json.dump(list(completed_ids), f)
                
                # After successful processing and saving to CSV:
                with open(output_dir / 'progress.json', 'w') as f:
                    json.dump({'last_id': cert_id}, f)
                
            except Exception as e:
                logger.error(f"Error processing certificate ID {cert_id}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                continue
        
        logger.info(f"Processed {len(metadata_records)} certificates with {len(modification_records)} modifications")
        return metadata_records, modification_records

    def _load_progress(self, output_dir: Path) -> str:
        """Load last processed certificate ID"""
        progress_file = output_dir / 'progress.json'
        if progress_file.exists():
            with open(progress_file, 'r') as f:
                return json.load(f).get('last_id', '')
        return ''

def main():
    scraper = CBFCScraper()
    
    # Test with a single certificate ID first
    certificate_id = "100090292400000109"
    result = scraper.get_certificate_details(certificate_id)
    
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("Failed to get certificate details")

if __name__ == "__main__":
    main()