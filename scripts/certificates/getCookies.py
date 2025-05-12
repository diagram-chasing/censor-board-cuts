import json
import logging
import requests
import uuid
from pathlib import Path
import urllib3

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_tokens(output_dir: str = None):
    try:
        # Ensure output directory exists
        output_dir = output_dir or Path(__file__).parent
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        session = requests.Session()
        # Disable SSL verification
        session.verify = False
        base_url = "https://www.ecinepramaan.gov.in/cbfc"
        
        # Common headers used across requests
        common_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0',
            'DTMN_SERVICE': 'TRUE',
            'DTMN_SESSION_VALIDATION': '0',
            'DTMN_SESSIONID': 'BLANK'  # Start with BLANK as in original code
        }
        
        # Step 1: Initial ribbon request
        logger.debug("Making initial ribbon request...")
        ribbon_headers = {
            **common_headers,
            'Content-Type': 'text/x-gwt-rpc; charset=utf-8',
            'X-GWT-Permutation': '1'
        }
        
        ribbon_data = '7|0|5|https://www.ecinepramaan.gov.in/cbfc/cbfc.Cbfc/|89CE7FBB2D78EB7BFEAA9F7D546C3CF1|cbfc.ribbon.shared.Ribbon_Srv|freeUser|Z|1|2|3|4|1|5|0|'
        response = session.post(f'{base_url}/cbfc/ribbon/client/Ribbon', 
                              headers=ribbon_headers, 
                              data=ribbon_data)
        logger.debug(f"Ribbon response status: {response.status_code}")
        logger.debug(f"Ribbon response cookies: {dict(response.cookies)}")
        logger.debug(f"Ribbon response text: {response.text}")
        
        # Update DTMN_SESSIONID if present in response headers
        if 'DTMN_SESSIONID' in response.headers:
            common_headers['DTMN_SESSIONID'] = response.headers['DTMN_SESSIONID']
            ribbon_headers['DTMN_SESSIONID'] = response.headers['DTMN_SESSIONID']
            logger.debug(f"Updated DTMN_SESSIONID to: {response.headers['DTMN_SESSIONID']}")
        
        # Step 2: Certificate detail page request
        logger.debug("\nMaking certificate detail page request...")
        cert_response = session.get(f'{base_url}/?a=Certificate_Detail&i=100030292000001074',
                                  headers=common_headers)
        logger.debug(f"Certificate page response status: {cert_response.status_code}")
        logger.debug(f"Certificate page cookies: {dict(cert_response.cookies)}")
        logger.debug(f"Certificate page response text: {cert_response.text}")
        
        # Step 3: Default connection request
        logger.debug("\nMaking default connection request...")
        connection_data = '7|0|6|https://www.ecinepramaan.gov.in/cbfc/cbfc.Cbfc/|321A65B985A18A576D88D94E74164E75|cbfc.self.shared.Cbfc_Srv|getDefaultConnection|java.lang.String/2004016611|Certificate|1|2|3|4|1|5|6|'
        connection_response = session.post(f'{base_url}/cbfc/self/client/Cbfc',
                                         headers=ribbon_headers,
                                         data=connection_data)
        logger.debug(f"Connection response status: {connection_response.status_code}")
        logger.debug(f"Connection response cookies: {dict(connection_response.cookies)}")
        logger.debug(f"Connection response text: {connection_response.text}")
        
        # Step 4: User permission request
        logger.debug("\nMaking user permission request...")
        permission_data = '7|0|4|https://www.ecinepramaan.gov.in/cbfc/cbfc.Cbfc/|321A65B985A18A576D88D94E74164E75|cbfc.self.shared.Cbfc_Srv|getUserPermission|1|2|3|4|0|'
        permission_response = session.post(f'{base_url}/cbfc/self/client/Cbfc',
                                         headers=ribbon_headers,
                                         data=permission_data)
        logger.debug(f"Permission response status: {permission_response.status_code}")
        logger.debug(f"Permission response cookies: {dict(permission_response.cookies)}")
        logger.debug(f"Permission response text: {permission_response.text}")
        
        # Extract and save cookies
        logger.debug("\nExtracting final session cookies...")
        cookies = {}
        raw_cookies = session.cookies.get_dict()
        # Remove JSESSIONID if present
        raw_cookies.pop('JSESSIONID', None)
        cookies['cookies'] = raw_cookies
        logger.debug(f"Final cookies to save: {raw_cookies}")
        
        # Save cookies to the specified directory
        cookies_path = output_path / '.cookies.json'
        with open(cookies_path, 'w') as f:
            json.dump(cookies, f, indent=4)
            
        # Save important headers
        headers_to_save = {
            'headers': {
                'DTMN_SESSIONID': common_headers['DTMN_SESSIONID']
            }
        }
        logger.debug(f"Headers to save: {headers_to_save}")
        
        # Save headers to the specified directory
        headers_path = output_path / '.headers.json'
        with open(headers_path, 'w') as f:
            json.dump(headers_to_save, f, indent=4)
            
        logger.debug(f"\nSuccessfully saved cookies to {cookies_path}")
        logger.debug(f"Headers saved to {headers_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error getting tokens: {str(e)}")
        return False

if __name__ == "__main__":
    if get_tokens():
        logger.debug("Token retrieval process completed successfully")
    else:
        logger.error("Token retrieval process failed")