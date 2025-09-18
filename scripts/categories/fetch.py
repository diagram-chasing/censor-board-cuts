#!/usr/bin/env python3

import os
import string
import requests
from bs4 import BeautifulSoup
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter
from io import BytesIO
import time
import random
import logging
import argparse
import re

# Set up command line arguments
parser = argparse.ArgumentParser(description='Fetch film data from CBFC India website')
parser.add_argument('--characters', '-c', type=str, default='A-Z',
                   help='Characters to search for (e.g. "A,B,C" or "A-Z" or "A")')
parser.add_argument('--output-dir', '-o', type=str, default='raw',
                   help='Directory to save results')
parser.add_argument('--from-date', type=str, default='',
                   help='Start date for search in DD/MM/YYYY format')
parser.add_argument('--delay-min', type=int, default=1,
                   help='Minimum delay between requests in seconds')
parser.add_argument('--delay-max', type=int, default=2,
                   help='Maximum delay between requests in seconds')
parser.add_argument('--debug', action='store_true',
                   help='Enable debug mode (save processed captcha images)')
parser.add_argument('--max-attempts', type=int, default=5,
                   help='Maximum number of attempts for each character')
args = parser.parse_args()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('cbfc_scraper.log'),
        logging.StreamHandler()
    ]
)

# Parse characters to search
def parse_characters(char_input):
    chars = []
    
    if '-' in char_input:
        # Handle range like A-Z
        start, end = char_input.split('-')
        start_idx = string.ascii_uppercase.index(start.upper())
        end_idx = string.ascii_uppercase.index(end.upper())
        chars = list(string.ascii_uppercase[start_idx:end_idx+1])
    elif ',' in char_input:
        # Handle list like A,B,C
        chars = [c.strip().upper() for c in char_input.split(',')]
    else:
        # Handle single character
        chars = [char_input.upper()]
    
    return chars

# Ensure the output directory exists
if not os.path.exists(args.output_dir):
    os.makedirs(args.output_dir)

# Function to preprocess and enhance captcha image for better recognition
def preprocess_captcha_image(image):
    # Create a copy of the image
    img = image.copy()
    
    # Convert to grayscale
    img = img.convert('L')
    
    # Increase contrast
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2.0)
    
    # Apply threshold to make it binary
    threshold = 140
    img = img.point(lambda x: 255 if x > threshold else 0)
    
    # Apply slight blur to reduce noise
    img = img.filter(ImageFilter.GaussianBlur(radius=0.5))
    
    # Apply another threshold after blur
    img = img.point(lambda x: 255 if x > threshold else 0)
    
    # Resize image to make it larger (can help with OCR)
    width, height = img.size
    img = img.resize((width * 2, height * 2), Image.LANCZOS)
    
    return img

# Try alternative preprocessing methods if standard one fails
def alternative_preprocess(image, method=1):
    img = image.copy()
    
    if method == 1:
        # Method 1: High contrast black and white
        img = img.convert('L')
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(3.0)  # Higher contrast
        threshold = 150
        img = img.point(lambda x: 255 if x > threshold else 0)
    
    elif method == 2:
        # Method 2: Edge enhancement
        img = img.convert('L')
        img = img.filter(ImageFilter.EDGE_ENHANCE)
        img = img.filter(ImageFilter.EDGE_ENHANCE_MORE)
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(2.0)
        threshold = 130
        img = img.point(lambda x: 255 if x > threshold else 0)
    
    elif method == 3:
        # Method 3: Sharpening
        img = img.convert('L')
        img = img.filter(ImageFilter.SHARPEN)
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.8)
        threshold = 160
        img = img.point(lambda x: 255 if x > threshold else 0)
    
    # Resize image to make it larger (can help with OCR)
    width, height = img.size
    img = img.resize((width * 2, height * 2), Image.LANCZOS)
    
    return img

# Function to get and solve captcha with multiple processing methods
def get_and_solve_captcha(session, max_attempts=5):
    captcha_url = 'https://cbfcindia.gov.in/cbfcAdmin/admin/captcha.php'
    
    # Try multiple times
    for attempt in range(max_attempts):
        try:
            # Add some randomization to avoid detection
            time.sleep(random.uniform(1, 2))
            
            response = session.get(captcha_url)
            
            if response.status_code != 200:
                logging.error(f"Failed to fetch captcha: {response.status_code}")
                continue
            
            # Save the captcha image
            captcha_image = Image.open(BytesIO(response.content))
            
            # Try different preprocessing methods
            preprocessing_methods = [
                ("standard", lambda img: preprocess_captcha_image(img)),
                ("alt1", lambda img: alternative_preprocess(img, 1)),
                ("alt2", lambda img: alternative_preprocess(img, 2)),
                ("alt3", lambda img: alternative_preprocess(img, 3))
            ]
            
            for method_name, preprocess_func in preprocessing_methods:
                try:
                    # Preprocess image to improve OCR
                    processed_image = preprocess_func(captcha_image)
                    
                    # Save processed captcha for debugging if enabled
                    if args.debug:
                        debug_dir = os.path.join(args.output_dir, 'debug')
                        if not os.path.exists(debug_dir):
                            os.makedirs(debug_dir)
                        filename = f'captcha_{method_name}_{attempt}_{int(time.time())}.png'
                        processed_image.save(os.path.join(debug_dir, filename))
                    
                    # Try different PSM modes for Tesseract
                    psm_modes = [7, 8, 6, 13]
                    
                    for psm in psm_modes:
                        # Use pytesseract to extract text
                        config = f'--psm {psm} -c tessedit_char_whitelist=0123456789abcdefghijklmnopqrstuvwxyz'
                        captcha_text = pytesseract.image_to_string(processed_image, config=config)
                        
                        # Clean the captcha text (remove spaces and newlines)
                        captcha_text = captcha_text.strip().replace(' ', '').replace('\n', '')
                        
                        # Validate captcha format (should be 6 characters and alphanumeric)
                        if len(captcha_text) >= 5 and all(c in '0123456789abcdefghijklmnopqrstuvwxyz' for c in captcha_text.lower()):
                            logging.debug(f"Detected captcha: {captcha_text} (method: {method_name}, psm: {psm})")
                            return captcha_text
                
                except Exception as e:
                    logging.error(f"Error with preprocessing method {method_name}: {e}")
            
            logging.warning(f"Failed to recognize captcha with any method, retrying...")
            
        except Exception as e:
            logging.error(f"Error solving captcha (attempt {attempt+1}/{max_attempts}): {e}")
    
    logging.error("Failed to solve captcha after maximum attempts")
    return None

# Check if the response indicates incorrect captcha
def is_incorrect_captcha(response_text):
    return "Incorrect Captcha" in response_text or "Invalid Captcha" in response_text

# Function to search films by character
def search_films_by_character(character, max_attempts=None):
    if max_attempts is None:
        max_attempts = args.max_attempts
        
    for attempt in range(max_attempts):
        try:
            # Create a session to maintain cookies
            session = requests.Session()
            
            # First visit the search page to get cookies
            search_page_url = 'https://cbfcindia.gov.in/cbfcAdmin/search-film.php'
            response = session.get(search_page_url)
            
            if response.status_code != 200:
                logging.error(f"Failed to access search page: {response.status_code}")
                continue
            
            # Get and solve the captcha
            captcha_text = get_and_solve_captcha(session)
            
            if not captcha_text:
                logging.error(f"Failed to solve captcha for character: {character}")
                continue
            
            # Make the search request
            search_url = 'https://cbfcindia.gov.in/cbfcAdmin/search.php'
            
            params = {
                'title': character,
                'from_date': args.from_date,
                'to_date': '',
                'languages': '',
                'captcha': captcha_text,
                'register': 'register'
            }
            
            # Add headers to mimic a browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': search_page_url,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Add some randomization to avoid detection
            time.sleep(random.uniform(2, 4))
            
            # Make the search request
            response = session.get(search_url, params=params, headers=headers)
            
            if response.status_code != 200:
                logging.error(f"Search failed for character {character}: {response.status_code}")
                continue
                
            # Check if the response contains an error message about invalid captcha
            if is_incorrect_captcha(response.text):
                logging.warning(f"Server rejected captcha '{captcha_text}' for character {character}, retrying...")
                continue
                
            # Save the response to a file
            output_file = os.path.join(args.output_dir, f"{character}.html")
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logging.debug(f"Successfully saved search results for '{character}' to {output_file}")
            return True
            
        except Exception as e:
            logging.error(f"Error searching for character {character} (attempt {attempt+1}/{max_attempts}): {e}")
    
    logging.error(f"Failed to search for character {character} after maximum attempts")
    return False

# Main function to search for specified characters
def main():
    characters = parse_characters(args.characters)
    logging.debug(f"Will process the following characters: {', '.join(characters)}")
    if args.from_date:
        logging.info(f"Searching from date: {args.from_date}")
    else:
        logging.info("No start date specified, searching all dates")
    
    for char in characters:
        logging.info(f"Processing character: {char}")
        
        if search_films_by_character(char):
            # Add a longer delay between successful characters
            delay = random.uniform(args.delay_min, args.delay_max)
            logging.debug(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
        else:
            # Add a longer delay after failures to avoid being rate-limited
            delay = random.uniform(args.delay_min * 2, args.delay_max * 2)
            logging.info(f"Request failed, waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)

if __name__ == "__main__":
    logging.info("Starting CBFC film search...")
    logging.debug(f"Output directory: {args.output_dir}")
    main()
    date_range_msg = f"from {args.from_date}" if args.from_date else "with no date restriction"
    logging.info(f"Search completed {date_range_msg}. Results saved in {args.output_dir} directory.")
