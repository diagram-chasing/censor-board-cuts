import requests
import json
import re
from typing import Dict, Optional

class CBFCScraper:
    def __init__(self, debug=False):
        self.debug = debug
        self.base_url = "https://www.ecinepramaan.gov.in"
        
        # Exactly matching the working curl command headers
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
        
        # Session cookies from the curl command
        self.cookies = {
            'JSESSIONID': 'dfe_FiCBOw6WS4X7Vn5FFY_L1RvAivy-owhGXWlQ.cbfc-prod-sys-app2',
            'TS01978ffa': '01415ded823231fb101457e8520cb8968b07f4590f5c050edbdb5adec5ca0f0387dd3bd2dbd80f68fd1b7c595963d700ced531bdbb184bd66167e8553fcaf63ca3695f4860dae33f24806e147ecd2e9ecb0793274e',
            'TS0184077e': '01415ded82976eef546885b0f4618b46563f27926c79f6e48f76c6489cc59012366db815d8fc3f379886db351131ca89f614252328e7092f59b83ab1ff26ec0a212acb5b1e',
            'ecinepramaan_cookie': '!86FwJXh1f0JEOLVbMRJgDS1zFjsUALxFCQK/lmXZUJKzFoJ/SkKtzSRL+qYQikcUgCiIDP97+V6cAvwDO64L/Qycp/FSwDR9xaB20YQV'
        }

    def debug_print(self, message: str, data=None):
        """Print debug information if debug mode is enabled"""
        if self.debug:
            print(f"DEBUG: {message}")
            if data:
                print(f"DATA: {data}")

    def get_certificate_details(self, certificate_id: str) -> Optional[Dict]:
        """
        Fetch certificate details for a given certificate ID
        """
        url = f"{self.base_url}/cbfc/cbfc/certificate/qrRedirect/client/QRRedirect"
        
        # Adding referer header dynamically based on certificate ID
        self.headers['Referer'] = f"https://www.ecinepramaan.gov.in/cbfc/?a=Certificate_Detail&i={certificate_id}"
        
        # Construct the GWT-RPC payload - exactly matching the curl command
        payload = f'7|0|6|{self.base_url}/cbfc/cbfc.Cbfc/|A425282E16D492E942BAD73170B377F8|cbfc.certificate.qrRedirect.shared.QRRedirect_Srv|getDefaultValues|java.lang.String/2004016611|{certificate_id}|1|2|3|4|1|5|6|'
        
        try:
            # Make request with both headers and cookies
            response = requests.post(
                url, 
                headers=self.headers, 
                cookies=self.cookies,
                data=payload
            )
            response.raise_for_status()
            
            self.debug_print("Raw response:", response.text)
            
            # First verify we got a successful response with data
            if "//OK" not in response.text:
                print("Did not receive OK response from server")
                return None
            
            # Extract the actual data part
            data_parts = response.text.split('//OK')[1].strip()
            self.debug_print("Data parts:", data_parts)
            
            try:
                # Parse the JSON-like response
                data = eval(data_parts)  # Using eval since the response is already in Python list format
                
                # Find the index containing the HTML-like string with film details
                film_details_str = None
                for item in data[0]:
                    if isinstance(item, str) and 'Film Name' in item:
                        film_details_str = item
                        break
                
                if not film_details_str:
                    print("Could not find film details in response")
                    return None
                
                # Extract film details using regex
                certificate_info = {}
                
                # Extract basic info
                file_no_match = re.search(r'File No\. : ([^<]+)', film_details_str)
                if file_no_match:
                    certificate_info['file_no'] = file_no_match.group(1).strip()
                
                film_name_match = re.search(r'Film Name : ([^<]+)', film_details_str)
                if film_name_match:
                    certificate_info['film_name'] = film_name_match.group(1).strip()
                
                # Extract cast and crew info
                for role in ['Director', 'Producer', 'Main Actors', 'Story', 'Music']:
                    pattern = f"{role}</span></b></div><div id='castCreditDescription'>([^<]+)"
                    match = re.search(pattern, film_details_str)
                    if match:
                        certificate_info[role.lower().replace(' ', '_')] = match.group(1).strip()
                
                # Extract endorsements
                endorsements_match = re.search(r'<div class=\'endorsementHeading\' >([^<]+)', film_details_str)
                if endorsements_match:
                    certificate_info['endorsements'] = endorsements_match.group(1).strip()
                
                return certificate_info
                
            except Exception as e:
                print(f"Error parsing response data: {e}")
                if self.debug:
                    import traceback
                    traceback.print_exc()
                return None
            
        except requests.RequestException as e:
            print(f"Error fetching certificate details: {e}")
            return None

def main():
    # Example usage
    scraper = CBFCScraper(debug=True)  # Enable debug mode
    certificate_id = "100090292400000155"
    
    result = scraper.get_certificate_details(certificate_id)
    if result:
        print("\nCertificate Details:")
        print("-" * 50)
        for key, value in result.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
    else:
        print("Failed to fetch certificate details")

if __name__ == "__main__":
    main()