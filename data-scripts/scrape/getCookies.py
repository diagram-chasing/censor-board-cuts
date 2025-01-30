import undetected_chromedriver as uc
import json
import logging
from pathlib import Path
import time

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_gwt_permutation(driver, base_url):
    """Extract GWT permutation using multiple methods"""
    try:
        # Method 1: Try to get it from the window object directly
        gwt_perm = driver.execute_script("return window.__gwt_getProperty && window.__gwt_getProperty('strongName');")
        if gwt_perm and len(gwt_perm) == 32:
            return gwt_perm
            
        # Method 2: Try to get it from the meta tags
        meta_tags = driver.find_elements('tag name', 'meta')
        for tag in meta_tags:
            if tag.get_attribute('name') == 'gwt:property' and 'strongName' in tag.get_attribute('content'):
                value = tag.get_attribute('content').split('=')[1]
                if len(value) == 32:
                    return value
                    
        # Method 3: Analyze the nocache.js file
        nocache_url = f"{base_url}cbfc.Cbfc/cbfc.Cbfc.nocache.js"
        driver.get(nocache_url)
        js_content = driver.page_source
        
        patterns = [
            r"strongName\s*=\s*'([A-F0-9]{32})'",
            r"propertyValue\s*:\s*'([A-F0-9]{32})'",
            r"permutationId\s*=\s*'([A-F0-9]{32})'",
            r"([A-F0-9]{32})"  # Last resort: look for any 32-char hex string
        ]
        
        import re
        for pattern in patterns:
            matches = re.findall(pattern, js_content)
            if matches:
                for match in matches:
                    # Verify it's a valid hex string
                    if all(c in '0123456789ABCDEF' for c in match):
                        return match
                        
        # Method 4: Look in the network requests
        requests = driver.execute_script("""
            return window.performance.getEntries().map(e => e.name);
        """)
        
        for url in requests:
            if isinstance(url, str):
                match = re.search(r'/([A-F0-9]{32})\.cache\.js', url)
                if match:
                    return match.group(1)
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting GWT permutation: {str(e)}")
        return None

def get_dtm_session(driver, base_url):
    """Get DTM session ID by hitting relevant endpoints"""
    try:
        # First visit the main page and wait for it to load
        driver.get("https://www.ecinepramaan.gov.in/cbfc/")
        time.sleep(5)
        
        # Add required headers
        driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
            'headers': {
                'DTMN_SERVICE': 'TRUE',
                'DTMN_SESSION_VALIDATION': '0',
                'X-GWT-Module-Base': f"{base_url}cbfc.Cbfc/",
                'Origin': 'https://www.ecinepramaan.gov.in',
                'DNT': '1'
            }
        })
        
        # Try the QR redirect request that's known to work
        response = driver.execute_async_script("""
            var callback = arguments[arguments.length - 1];
            fetch('/cbfc/certificate/qrRedirect/client/QRRedirect', {
                method: 'POST',
                headers: {
                    'DTMN_SERVICE': 'TRUE',
                    'DTMN_SESSION_VALIDATION': '0',
                    'Content-Type': 'text/x-gwt-rpc; charset=utf-8',
                    'X-GWT-Module-Base': window.location.href + 'cbfc.Cbfc/',
                    'X-GWT-Permutation': window.__gwt_getProperty ? window.__gwt_getProperty('strongName') : ''
                },
                body: '7|0|6|/cbfc/cbfc.Cbfc/|A425282E16D492E942BAD73170B377F8|cbfc.certificate.qrRedirect.shared.QRRedirect_Srv|getDefaultValues|java.lang.String/2004016611|100090292400000155|1|2|3|4|1|5|6|'
            }).then(response => {
                callback({
                    ok: response.ok,
                    status: response.status,
                    headers: Object.fromEntries(response.headers),
                    cookies: document.cookie
                });
            }).catch(error => {
                callback({error: error.toString()});
            });
        """)
        
        logger.info(f"QR redirect response: {response}")
        
        # Check if request was successful and extract DTMN session from headers
        if response and not response.get('error'):
            # Try to get DTM session from response headers
            if 'headers' in response and 'dtmn_sessionid' in response['headers']:
                dtm_session = response['headers']['dtmn_sessionid']
                logger.info("Successfully obtained DTM session ID from headers")
                return dtm_session
        
        logger.warning("Could not obtain DTM session ID from QR redirect request")
        return None
        
    except Exception as e:
        logger.error(f"Error getting DTM session: {str(e)}")
        return None

def get_tokens():
    """Get authentication tokens using undetected-chromedriver"""
    try:
        # Setup chrome options
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Create driver
        driver = uc.Chrome(options=options)
        
        try:
            # Visit pages to get cookies
            cbfc_url = "https://www.ecinepramaan.gov.in/cbfc/"
            driver.get(cbfc_url)
            
            # Wait for page load
            time.sleep(2)  # Give JavaScript time to execute
            
            # Get DTM session
            dtm_session = get_dtm_session(driver, cbfc_url)
            if not dtm_session:
                logger.warning("Could not find DTM session ID")
                return False
                
            # Get GWT permutation
            gwt_perm = get_gwt_permutation(driver, cbfc_url)
            if not gwt_perm:
                logger.warning("Could not find GWT permutation")
                return False
                
            # Get certificate page for additional cookies
            driver.get(f"{cbfc_url}?a=Certificate_Detail")
            
            # Extract all cookies
            cookies = {
                cookie['name']: cookie['value'] 
                for cookie in driver.get_cookies()
            }
            
            # Add DTM session to headers if not in cookies
            if 'DTMN_SESSIONID' not in cookies and dtm_session:
                cookies['DTMN_SESSIONID'] = dtm_session
            
            # Compile tokens
            tokens = {
                'cookies': cookies,
                'headers': {
                    'X-GWT-Permutation': gwt_perm,
                    'X-GWT-Module-Base': f"{cbfc_url}cbfc.Cbfc/"
                }
            }
            
            # Save to file
            with open('tokens.json', 'w') as f:
                json.dump(tokens, f, indent=2)
            
            # Print summary
            logger.info("\nToken Summary:")
            logger.info("=============")
            for cookie_name in ['JSESSIONID', 'TS01978ffa', 'TS0184077e', 'ecinepramaan_cookie', 'DTMN_SESSIONID']:
                value = cookies.get(cookie_name, 'Not found')
                logger.info(f"{cookie_name}: {value[:30]}..." if value != 'Not found' else f"{cookie_name}: {value}")
            logger.info(f"GWT Permutation: {gwt_perm}")
            
            return True
            
        finally:
            driver.quit()
            
    except Exception as e:
        logger.error(f"Failed to get tokens: {str(e)}")
        return False

if __name__ == "__main__":
    if get_tokens():
        logger.info("\nTokens saved to tokens.json")
    else:
        logger.error("Failed to get tokens")