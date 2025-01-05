import requests
import json
import re
from typing import Dict, Optional
from bs4 import BeautifulSoup
from datetime import datetime

class CBFCScraper:
    def __init__(self, debug=False):
        self.debug = debug
        self.base_url = "https://www.ecinepramaan.gov.in"
        
        # Headers and cookies remain the same
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'text/x-gwt-rpc; charset=utf-8',
            'X-GWT-Permutation': '859E251D1A7860A54635D369EBA63343',
            'X-GWT-Module-Base': 'https://www.ecinepramaan.gov.in/cbfc/cbfc.Cbfc/',
            'DTMN_SERVICE': 'TRUE',
            'DTMN_SESSIONID': 'dc6a7d7d-37c8-4349-9b22-f593b20f965a',
            'DTMN_SESSION_VALIDATION': '0',
            'Origin': 'https://www.ecinepramaan.gov.in',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-GPC': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
        
        self.cookies = {
            'JSESSIONID': 'dfe_FiCBOw6WS4X7Vn5FFY_L1RvAivy-owhGXWlQ.cbfc-prod-sys-app2',
            'TS01978ffa': '01415ded823231fb101457e8520cb8968b07f4590f5c050edbdb5adec5ca0f0387dd3bd2dbd80f68fd1b7c595963d700ced531bdbb184bd66167e8553fcaf63ca3695f4860dae33f24806e147ecd2e9ecb0793274e',
            'TS0184077e': '01415ded82976eef546885b0f4618b46563f27926c79f6e48f76c6489cc59012366db815d8fc3f379886db351131ca89f614252328e7092f59b83ab1ff26ec0a212acb5b1e',
            'ecinepramaan_cookie': '!86FwJXh1f0JEOLVbMRJgDS1zFjsUALxFCQK/lmXZUJKzFoJ/SkKtzSRL+qYQikcUgCiIDP97+V6cAvwDO64L/Qycp/FSwDR9xaB20YQV'
        }

    def debug_print(self, message: str, data=None):
        if self.debug:
            print(f"\n=== DEBUG: {message} ===\n")
            if data:
                print(data)
            print("\n")

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
                # Extract role name by removing the colon
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
            if len(cells) == 5:  # Regular row
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
            # Extract basic information
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

            # Extract modifications
            modifications = self.parse_modifications_table(endorsement_div)
            if modifications:
                endorsement['modifications'] = modifications

        return endorsement

    def extract_main_data(self, data_array: list) -> Dict:
        """Extract basic information from the main data array"""
        main_info = {}
        
        # Find the nested array that contains the actual data
        for item in data_array:
            if isinstance(item, list) and len(item) > 5:
                main_data = item
                break
        else:
            return main_info

        # Map indices to keys for the data we know we need
        required_fields = {
            'id': -1,  # Last element
            'title': None,
            'category': None,
            'language': None,
            'format': None,
            'duration': None,
            'applicant': None,
            'certifier': None,
            'synopsis': None
        }

        # Find the indices of our required fields
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
                elif '(' in item and ')' in item and len(item) > 20:  # Likely applicant
                    required_fields['applicant'] = i
                elif 'E.O.' in item or 'CBFC' in item:  # Likely certifier
                    required_fields['certifier'] = i
                elif len(item) > 50:  # Likely synopsis
                    required_fields['synopsis'] = i
                elif item.isupper() and len(item) < 30:  # Likely title
                    required_fields['title'] = i

        # Extract the data we found
        for key, index in required_fields.items():
            if index is not None and index < len(main_data):
                main_info[key] = main_data[index]

        return main_info

    def get_certificate_details(self, certificate_id: str) -> Optional[Dict]:
        """
        Fetch and parse certificate details for a given certificate ID
        """
        url = f"{self.base_url}/cbfc/cbfc/certificate/qrRedirect/client/QRRedirect"
        self.headers['Referer'] = f"https://www.ecinepramaan.gov.in/cbfc/?a=Certificate_Detail&i={certificate_id}"
        payload = f'7|0|6|{self.base_url}/cbfc/cbfc.Cbfc/|A425282E16D492E942BAD73170B377F8|cbfc.certificate.qrRedirect.shared.QRRedirect_Srv|getDefaultValues|java.lang.String/2004016611|{certificate_id}|1|2|3|4|1|5|6|'
            
        try:
            response = requests.post(url, headers=self.headers, cookies=self.cookies, data=payload)
            
            if "//OK" not in response.text:
                print("Did not receive OK response from server")
                return None
                
            data_parts = response.text.split('//OK')[1].strip()
            self.debug_print("Raw Data Parts", data_parts)
            
            parsed_data = eval(data_parts)
            
            # Extract basic information
            certificate_info = self.extract_main_data(parsed_data)
            
            # Find the credits and endorsement HTML in the data array
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

            return certificate_info

        except Exception as e:
            print(f"Error parsing response data: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return None

def main():
    # Example usage
    scraper = CBFCScraper(debug=True)
    
    # Test with multiple certificate IDs
    certificate_ids = [
        "100090292400000155",  # First example
        "100090292400000120"   # Second example
    ]
    
    for cert_id in certificate_ids:
        print(f"\nFetching details for certificate ID: {cert_id}")
        print("-" * 50)
        
        result = scraper.get_certificate_details(cert_id)
        if result:
            print("\nCertificate Details:")
            print("-" * 50)
            for key, value in result.items():
                if key == 'modifications':
                    print(f"\n{key.title()}:")
                    for mod in value:
                        print(f"  Cut {mod['cut_no']}: {mod['description']}")
                else:
                    print(f"{key.replace('_', ' ').title()}: {value}")
        else:
            print("Failed to fetch certificate details")

if __name__ == "__main__":
    main()