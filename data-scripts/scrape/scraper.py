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
            # Make the HTTP request
            response = requests.post(url, headers=self.headers, cookies=self.cookies, data=payload)
            
            # First verify we got a successful response with data
            if "//OK" not in response.text:
                print("Did not receive OK response from server")
                return None
                
            # Extract the actual data part by removing the //OK prefix
            data_parts = response.text.split('//OK')[1].strip()
            self.debug_print("Data parts:", data_parts)
            
            # Parse the outer array structure
            parsed_data = eval(data_parts)  # Using eval since it's already in Python list format
            
            # The main data array is at index 28
            if not isinstance(parsed_data, list) or len(parsed_data) < 29:
                print("Invalid data structure")
                return None
                
            main_data = parsed_data[28]
            if not isinstance(main_data, list):
                print("Invalid main data structure")
                return None

            # Now we can create our structured data
            certificate_info = {
                'type': main_data[0],
                'model_type': main_data[2],
                'applicant': main_data[3],
                'duration': main_data[5],
                'certifier': main_data[7],
                'category': main_data[8],
                'title': main_data[9],
                'classification': main_data[10],
                'language': main_data[11],
                'format': main_data[12],
                'certifier_full': main_data[13],
                'synopsis': main_data[14],
                'id': main_data[15]
            }

            # Parse credits HTML (position 4)
            credits_html = main_data[4]
            for role in ['Director', 'Producer', 'Main Actors', 'Story', 'Music']:
                pattern = f"{role}</span></b></div><div id='castCreditDescription'>([^<]+)"
                match = re.search(pattern, credits_html)
                if match:
                    certificate_info[f'credit_{role.lower().replace(" ", "_")}'] = match.group(1).strip()

            # Parse endorsement HTML (position 6)
            endorsement_html = main_data[6]
            
            # Extract basic endorsement info
            file_no_match = re.search(r'File No\. : ([^<]+)', endorsement_html)
            if file_no_match:
                certificate_info['file_no'] = file_no_match.group(1).strip()
            
            film_name_match = re.search(r'Film Name : ([^<]+)', endorsement_html)
            if film_name_match:
                certificate_info['film_name_full'] = film_name_match.group(1).strip()
                
            cert_no_match = re.search(r'Cert No\.([^<]+)Dated([^<]+)', endorsement_html)
            if cert_no_match:
                certificate_info['cert_no'] = cert_no_match.group(1).strip()
                certificate_info['cert_date'] = cert_no_match.group(2).strip()

            # Extract modifications table
            modifications = []
            table_pattern = r'<tr><td[^>]*>(\d+)</td><td>([^<]+)</td><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td><td[^>]*>([^<]+)</td></tr>'
            for match in re.finditer(table_pattern, endorsement_html):
                modifications.append({
                    'cut_no': int(match.group(1)),
                    'description': match.group(2).strip(),
                    'deleted': match.group(3).strip(),
                    'replaced': match.group(4).strip(),
                    'inserted': match.group(5).strip()
                })
            
            if modifications:
                certificate_info['modifications'] = modifications

            return certificate_info

        except Exception as e:
            print(f"Error parsing response data: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
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