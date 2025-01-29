import json
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_tokens():
    try:
        url = "https://www.ecinepramaan.gov.in/cbfc/cbfc/certificate/qrRedirect/client/QRRedirect"
        headers = {
            'DTMN_SERVICE': 'TRUE',
            'DTMN_SESSIONID': 'BLANK',
            'DTMN_SESSION_VALIDATION': '0',
            'User-Agent': 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0'
        }
        
        response = requests.get(url, headers=headers)
        
        # Extract cookies from response
        cookies = {}
        raw_cookies = response.cookies.get_dict()
        # Remove JSESSIONID if present
        raw_cookies.pop('JSESSIONID', None)
        cookies['cookies'] = raw_cookies
        
        # Save cookies to file
        with open('cookies.json', 'w') as f:
            json.dump(cookies, f, indent=4)
            
        # Extract and save DTMN_SESSIONID
        headers_to_save = {}
        headers_to_save['headers'] = {}
        if 'DTMN_SESSIONID' in response.headers:
            headers_to_save['headers']['DTMN_SESSIONID'] = response.headers['DTMN_SESSIONID']
            
        with open('headers.json', 'w') as f:
            json.dump(headers_to_save, f, indent=4)
            
        logger.info("Successfully retrieved and saved cookies to cookies.json")
        if headers_to_save:
            logger.info("DTMN_SESSIONID header saved to headers.json")
        return True
        
    except Exception as e:
        logger.error(f"Error getting tokens: {str(e)}")
        return False

if __name__ == "__main__":
    if get_tokens():
        logger.info("Token retrieval process completed successfully")
    else:
        logger.error("Token retrieval process failed")