import requests
import json
import logging
import urllib3
from typing import Dict, Optional, Tuple
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
        cookies_path = Path(cookies_dir) / '.cookies.json'
        headers_path = Path(cookies_dir) / '.headers.json'
        
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
            
    def _sanitize_filename(self, certificate_id: str) -> str:
        """Sanitize certificate ID for use as filename by replacing problematic characters"""
        return certificate_id.replace('/', '_').replace('=', '_eq_').replace('+', '_plus_')

    def html_exists_and_valid(self, certificate_id: str) -> Tuple[bool, Optional[str]]:
        """Check if HTML for the certificate ID exists and is valid"""
        safe_filename = self._sanitize_filename(certificate_id)
        html_path = Path('raw/html') / f"{safe_filename}.html"
        
        if not html_path.exists():
            return False, None
            
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            if self.is_html_valid(html_content):
                logger.debug(f"Using existing valid HTML for certificate ID: {certificate_id}")
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
                
                # Check if the response is valid before saving
                if not self.is_html_valid(response.text):
                    logger.error(f"Certificate ID {certificate_id}: Invalid HTML response")
                    logger.debug(f"Response content: {response.text[:500]}")  # Log first 500 chars
                    return None
                
                # Save the raw HTML response
                html_dir = Path('raw/html')
                html_dir.mkdir(parents=True, exist_ok=True)
                safe_filename = self._sanitize_filename(certificate_id)
                html_file = html_dir / f"{safe_filename}.html"
                with open(html_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.debug(f"Saved raw HTML for certificate ID: {certificate_id}")
                
                data_parts = response.text.split('//OK')[1].strip()
            else:
                # Use existing valid HTML
                data_parts = existing_html.split('//OK')[1].strip()
            
            parsed_data = eval(data_parts)

            if parsed_data:
                logger.debug(f"Successfully scraped {certificate_id}")
                return certificate_id
            else:
                logger.warning(f"Incomplete data for certificate ID {certificate_id}")
                return None

        except Exception as e:
            logger.error(f"Error processing certificate ID {certificate_id}: {str(e)}")
            return None
def main():
    scraper = CBFCScraper()
    
    # Test with a single certificate ID first
    certificate_id = "100090292400000109"
    result = scraper.get_certificate_details(certificate_id)
    
    if result:
        print("Fetched certificate details for {result}")
    else:
        print("Failed to get certificate details")

if __name__ == "__main__":
    main()