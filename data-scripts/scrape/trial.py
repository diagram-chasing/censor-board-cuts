from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time
import logging
from typing import Dict, List, Optional
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CBFCCookieAnalyzer:
    def __init__(self):
        self.base_url = "https://www.ecinepramaan.gov.in/cbfc/"
        self.driver = self._setup_driver()
        
    def _setup_driver(self) -> webdriver.Firefox:
        """Setup Selenium WebDriver with necessary options"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Enable performance logging
        options.set_preference('devtools.console.stdout.content', True)
        options.set_preference('browser.dom.window.dump.enabled', True)
        
        # Create driver
        driver = webdriver.Firefox(options=options)
        
        # Set script timeout
        driver.set_script_timeout(20)
        
        return driver
        
    def analyze_network_requests(self) -> List[Dict]:
        """Analyze network requests and store details"""
        # Execute JavaScript to get performance data
        performance_entries = self.driver.execute_script("""
            return window.performance.getEntries().map(entry => ({
                name: entry.name,
                entryType: entry.entryType,
                startTime: entry.startTime,
                duration: entry.duration
            }));
        """)
        return performance_entries
        
    def get_local_storage(self) -> Dict:
        """Get all items in localStorage"""
        return self.driver.execute_script("""
            let items = {};
            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                items[key] = localStorage.getItem(key);
            }
            return items;
        """)
        
    def get_session_storage(self) -> Dict:
        """Get all items in sessionStorage"""
        return self.driver.execute_script("""
            let items = {};
            for (let i = 0; i < sessionStorage.length; i++) {
                const key = sessionStorage.key(i);
                items[key] = sessionStorage.getItem(key);
            }
            return items;
        """)
        
    def analyze_cookies(self) -> List[Dict]:
        """Analyze all cookies and their properties"""
        cookies = self.driver.get_cookies()
        
        # Add some metadata about when each cookie appeared
        for cookie in cookies:
            cookie['discovered_at'] = time.time()
            cookie['url_when_set'] = self.driver.current_url
            
        return cookies
        
    def find_script_sources(self) -> List[str]:
        """Find and analyze script sources"""
        scripts = self.driver.find_elements(By.TAG_NAME, 'script')
        sources = []
        
        for script in scripts:
            src = script.get_attribute('src')
            if src:
                sources.append(src)
                
        return sources
        
    def analyze_gwt_module(self) -> Dict:
        """Analyze GWT specific elements and metadata"""
        gwt_info = {}
        
        # Look for GWT meta tags
        meta_tags = self.driver.find_elements(By.TAG_NAME, 'meta')
        for tag in meta_tags:
            name = tag.get_attribute('name')
            if name and 'gwt:' in name:
                gwt_info[name] = tag.get_attribute('content')
                
        # Look for GWT permutation strong name
        try:
            gwt_info['permutation'] = self.driver.execute_script("""
                return window.__gwt_getProperty && window.__gwt_getProperty('strongName');
            """)
        except:
            pass
            
        return gwt_info

    def monitor_cookie_changes(self, duration: int = 10) -> List[Dict]:
        """Monitor cookie changes over time"""
        cookie_changes = []
        start_time = time.time()
        
        while time.time() - start_time < duration:
            current_cookies = self.analyze_cookies()
            cookie_changes.append({
                'timestamp': time.time(),
                'cookies': current_cookies
            })
            time.sleep(1)
            
        return cookie_changes

    def analyze_page(self) -> Dict:
        """Main method to analyze the page and gather all information"""
        try:
            logger.info("Starting page analysis...")
            
            # Navigate to the page
            self.driver.get(self.base_url)
            logger.info("Loaded main page")
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Initial cookies
            initial_cookies = self.analyze_cookies()
            logger.info(f"Found {len(initial_cookies)} initial cookies")
            
            # Monitor cookie changes for a short period
            cookie_changes = self.monitor_cookie_changes(duration=5)
            logger.info("Monitored cookie changes")
            
            # Get network requests
            network_data = self.analyze_network_requests()
            logger.info(f"Analyzed {len(network_data)} network requests")
            
            # Get storage data
            local_storage = self.get_local_storage()
            session_storage = self.get_session_storage()
            
            # Get script sources
            script_sources = self.find_script_sources()
            logger.info(f"Found {len(script_sources)} script sources")
            
            # Get GWT specific info
            gwt_info = self.analyze_gwt_module()
            logger.info("Analyzed GWT module information")
            
            # Try navigating to certificate page to trigger more cookies
            self.driver.get(f"{self.base_url}?a=Certificate_Detail")
            logger.info("Loaded certificate page")
            
            # Get final cookies
            final_cookies = self.analyze_cookies()
            
            # Compile all data
            analysis_data = {
                'initial_cookies': initial_cookies,
                'cookie_changes': cookie_changes,
                'final_cookies': final_cookies,
                'network_requests': network_data,
                'local_storage': local_storage,
                'session_storage': session_storage,
                'script_sources': script_sources,
                'gwt_info': gwt_info
            }
            
            return analysis_data
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            return {}
        finally:
            self.driver.quit()
            
    def save_analysis(self, data: Dict, filename: str = 'cookie_analysis.json'):
        """Save analysis data to file"""
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved analysis to {filename}")
        except Exception as e:
            logger.error(f"Error saving analysis: {str(e)}")

def main():
    analyzer = CBFCCookieAnalyzer()
    analysis_data = analyzer.analyze_page()
    
    if analysis_data:
        print("\nAnalysis Summary:")
        print("================")
        
        if 'initial_cookies' in analysis_data:
            print(f"\nInitial Cookies: {len(analysis_data['initial_cookies'])}")
            for cookie in analysis_data['initial_cookies']:
                print(f"- {cookie['name']}: {cookie['value'][:30]}...")
                
        if 'gwt_info' in analysis_data:
            print("\nGWT Information:")
            for key, value in analysis_data['gwt_info'].items():
                print(f"- {key}: {value}")
                
        if 'script_sources' in analysis_data:
            print(f"\nScript Sources: {len(analysis_data['script_sources'])}")
            for src in analysis_data['script_sources']:
                print(f"- {src}")
        
        # Save full analysis
        analyzer.save_analysis(analysis_data)
    else:
        print("Analysis failed")

if __name__ == "__main__":
    main()