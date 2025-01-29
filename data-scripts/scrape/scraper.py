import requests
import json
import re
import csv
import logging
import uuid
import time
from typing import Dict, Optional, Tuple, List
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

# Load cookies from cookies.json
with open('cookies.json', 'r') as f:
    cookies = json.load(f)
with open('headers.json', 'r') as f:
    headers = json.load(f)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class CBFCScraper:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://www.ecinepramaan.gov.in"
        
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
                    'description': cells[1].get_text().strip(),
                    'deleted': cells[2].get_text().strip(),
                    'replaced': cells[3].get_text().strip(),
                    'inserted': cells[4].get_text().strip()
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
                text = div.get_text().strip()
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

    def get_certificate_details(self, certificate_id: str) -> Optional[Dict]:
        """Fetch and parse certificate details for a given certificate ID"""
        try:
            logger.debug(f"Fetching details for certificate ID: {certificate_id}")
            
            url = f"{self.base_url}/cbfc/cbfc/certificate/qrRedirect/client/QRRedirect"
            payload = f'7|0|6|{self.base_url}/cbfc/cbfc.Cbfc/|A425282E16D492E942BAD73170B377F8|cbfc.certificate.qrRedirect.shared.QRRedirect_Srv|getDefaultValues|java.lang.String/2004016611|{certificate_id}|1|2|3|4|1|5|6|'
            
            logger.debug("Equivalent curl command:")
            logger.debug(self._to_curl(url, payload))
            
            response = self.session.post(url, data=payload)
            response.raise_for_status()
            logger.debug(response.text)
            
            if "//OK" not in response.text:
                logger.error(f"Certificate ID {certificate_id}: Did not receive OK response")
                logger.debug(f"Response content: {response.text[:500]}")  # Log first 500 chars
                return None
                
            data_parts = response.text.split('//OK')[1].strip()
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
        """Process a list of certificate IDs and return metadata and modifications"""
        metadata_records = []
        modification_records = []
        
        for cert_id in certificate_ids:
            result = self.get_certificate_details(cert_id)
            if not result:
                continue
                
            # Separate modifications from metadata
            modifications = result.pop('modifications', [])
            
            # Add metadata record
            metadata_records.append(result)
            
            # Add modification records
            for mod in modifications:
                mod_record = {
                    'certificate_id': result.get('id', ''),
                    'film_name': result.get('title', ''),
                    **mod
                }
                modification_records.append(mod_record)
                
        return metadata_records, modification_records

    def save_to_csv(self, data: List[Dict], filename: str, directory: str = 'output'):
        """Save data to CSV file"""
        if not data:
            logger.warning(f"No data to save for {filename}")
            return
            
        Path(directory).mkdir(parents=True, exist_ok=True)
        filepath = Path(directory) / filename
        
        fieldnames = list(data[0].keys())
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            logger.info(f"Successfully saved {len(data)} records to {filepath}")
        except Exception as e:
            logger.error(f"Error saving to {filepath}: {str(e)}")


def main():
    scraper = CBFCScraper()
    
    # Test with a single certificate ID first
    certificate_id = "100090292400000155"
    result = scraper.get_certificate_details(certificate_id)
    
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("Failed to get certificate details")

if __name__ == "__main__":
    main()