#!/usr/bin/env python3
import os
import csv
import json
import pandas as pd
import time
import argparse
from dotenv import load_dotenv
import google.generativeai as genai
from google.ai.generativelanguage_v1beta.types import content
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configure Gemini API
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

# Create the model with the same configuration as in tagging.py
generation_config = {
  "temperature": 0.2,
  "top_p": 0.95,
  "top_k": 40,
  "max_output_tokens": 8192,
  "response_schema": content.Schema(
    type = content.Type.OBJECT,
    description = "Schema for representing the analysis of media censorship actions described in text.",
    required = ["cleaned_description", "action_types", "content_types", "media_elements", "censored_items", "reason"],
    properties = {
      "cleaned_description": content.Schema(
        type = content.Type.STRING,
        description = "The original description text, cleaned of timestamps, translated to English if necessary, and formatted for clarity, preserving all relevant details.",
      ),
      "action_types": content.Schema(
        type = content.Type.ARRAY,
        description = "A unique list of all censorship actions identified across all censored items.",
        items = content.Schema(
          type = content.Type.STRING,
          enum = ["audio_mute", "audio_level", "audio_replace", "audio_effect", "visual_blur", "visual_censor", "visual_effect", "visual_adjust", "visual_framerate", "deletion", "insertion", "overlay", "reduction", "replacement", "translation", "spacing", "warning_disclaimer", "certification"],
        ),
      ),
      "content_types": content.Schema(
        type = content.Type.ARRAY,
        description = "A unique list of all types of content identified as being censored across all censored items.",
        items = content.Schema(
          type = content.Type.STRING,
          enum = ["violence_physical", "violence_destruction", "sexual_explicit", "sexual_suggestive", "substance_use", "substance_brand", "profanity", "religious", "social_commentary", "political", "group_reference"],
        ),
      ),
      "media_elements": content.Schema(
        type = content.Type.ARRAY,
        description = "A unique list of all types of media elements affected by censorship across all censored items.",
        items = content.Schema(
          type = content.Type.STRING,
          enum = ["song_music", "dialogue_speech", "scene_visual", "text_title", "brand_logo", "technical_meta", "certificate_disclaimer"],
        ),
      ),
      "censored_items": content.Schema(
        type = content.Type.ARRAY,
        description = "An array detailing each distinct instance of censorship identified in the description.",
        items = content.Schema(
          type = content.Type.OBJECT,
          required = ["content", "reference", "action", "content_types", "media_element", "replacement"],
          properties = {
            "content": content.Schema(
              type = content.Type.STRING,
              description = "A description of the specific content or context being censored.",
            ),
            "reference": content.Schema(
              type = content.Type.STRING,
              description = "The specific word, stemmed profanity root, name (person/group/brand), or concept identifier that was the DIRECT TARGET of censorship. Null if not applicable (e.g., general scene blur).",
              nullable = True,
            ),
            "action": content.Schema(
              type = content.Type.STRING,
              description = "The specific censorship action applied to this item.",
              enum = ["audio_mute", "audio_level", "audio_replace", "audio_effect", "visual_blur", "visual_censor", "visual_effect", "visual_adjust", "visual_framerate", "deletion", "insertion", "overlay", "reduction", "replacement", "translation", "spacing", "warning_disclaimer", "certification"],
            ),
            "content_types": content.Schema(
              type = content.Type.ARRAY,
              description = "A list of all relevant content types for this specific censored item.",
              items = content.Schema(
                type = content.Type.STRING,
                enum = ["violence_physical", "violence_destruction", "sexual_explicit", "sexual_suggestive", "substance_use", "substance_brand", "profanity", "religious", "social_commentary", "political", "group_reference"],
              ),
            ),
            "media_element": content.Schema(
              type = content.Type.STRING,
              description = "The specific media element affected by this censorship action.",
              enum = ["song_music", "dialogue_speech", "scene_visual", "text_title", "brand_logo", "technical_meta", "certificate_disclaimer"],
            ),
            "replacement": content.Schema(
              type = content.Type.STRING,
              description = "Description of what replaced the censored content, if applicable (e.g., 'bleep sound', 'blurred area', 'silence'). Null if no replacement occurred or none was specified.",
              nullable = True,
            ),
          },
        ),
      ),
      "reason": content.Schema(
        type = content.Type.STRING,
        description = "The explicitly stated reason for the censorship, if provided in the description text. Null if no reason is mentioned.",
        nullable = True,
      ),
    },
  ),
  "response_mime_type": "application/json",
}

model = genai.GenerativeModel(
  model_name="gemini-1.5-flash-8b",
  generation_config=generation_config,
  system_instruction="You are a specialized text analysis system designed to identify, classify, and extract specific details about media censorship actions described in text. Your analysis must be precise, adhering strictly to the provided schema and classification categories.\n\n## Task Definition\n\nYour goal is to:\n1. Analyze the provided censorship description text.\n2. Identify specific elements that were censored (including words, names, concepts, visuals, sounds).\n3. For each censored element, capture the **specific reference** (e.g., the actual word, stemmed profanity, the **specific person, group, or brand name** that was the target of censorship).\n4. Classify the censorship actions, the type of content being censored, and the media element affected for each censored item.\n5. Return a single, complete, and valid JSON object adhering to the specified schema. **Only capture named entities (persons, groups, brands) if they are the direct subject of a censorship action, within the details of that action.**\n\n## Classification Categories\n\nUse ONLY these predefined values:\n\n| Field | Valid Values |\n|-------|-------------|\n| **action_types** | audio_mute, audio_level, audio_replace, audio_effect, visual_blur, visual_censor, visual_effect, visual_adjust, visual_framerate, deletion, insertion, overlay, reduction, replacement, translation, spacing, warning_disclaimer, certification |\n| **content_types** | violence_physical, violence_destruction, sexual_explicit, sexual_suggestive, substance_use, substance_brand, profanity, religious, social_commentary, political, group_reference |\n| **media_elements** | song_music, dialogue_speech, scene_visual, text_title, brand_logo, technical_meta, certificate_disclaimer |\n\n## Output Format Requirements\n\nThe response MUST be a single valid JSON object with this precise structure:\n```json\n{\n  \"cleaned_description\": \"string\", // REQUIRED: Cleaned, translated (if needed) description without timestamps.\n  \"action_types\": [\"string\"], // REQUIRED: All unique action_types used across all censored_items.\n  \"content_types\": [\"string\"], // REQUIRED: All unique content_types across all censored_items.\n  \"media_elements\": [\"string\"], // REQUIRED: All unique media_elements across all censored_items.\n  \"censored_items\": [ // REQUIRED: Array of distinct censored content instances.\n    {\n      \"content\": \"string\", // REQUIRED: Description of the specific content/context being censored.\n      \"reference\": \"string\" or null, // REQUIRED: The specific word, stemmed profanity root, name (person/group/brand), or concept identifier that was THE DIRECT TARGET of censorship. Null if not applicable (e.g., general scene blur).\n      \"action\": \"string\", // REQUIRED: Must be one of the action_types enum values.\n      \"content_types\": [\"string\"], // REQUIRED: Must be from content_types enum. List all relevant types.\n      \"media_element\": \"string\", // REQUIRED: Must be one of the media_elements enum values.\n      \"replacement\": \"string\" or null // What replaced the content, if any (e.g., \"bleep\", \"blurred text\"). Null if no replacement or not specified.\n    }\n  ],\n  \"reason\": \"string\" or null // Stated reason for censorship, if provided in the description. Null otherwise.\n}\n",
)

def process_description(description, timeout=30):
    """Process a description using the Gemini model and return the JSON response."""
    if not description or description.strip() == "":
        return None
    
    try:
        # Create a simple mock response for testing
        # This allows you to test the script without making API calls
        if os.environ.get("USE_MOCK_RESPONSE", "false").lower() == "true":
            logger.info("Using mock response for testing")
            return create_mock_response(description)
        
        # Set a timeout for the API call
        start_time = time.time()
        chat_session = model.start_chat(history=[])
        
        # Use a timeout to prevent hanging
        response = None
        try:
            response = chat_session.send_message(description)
            elapsed_time = time.time() - start_time
            logger.debug(f"API call completed in {elapsed_time:.2f}s")
        except Exception as e:
            logger.error(f"API call failed: {e}")
            return None
        
        if response and response.text:
            return json.loads(response.text)
        else:
            logger.warning("Empty response from API")
            return None
    except Exception as e:
        logger.error(f"Error processing description: {e}")
        return None

def create_mock_response(description):
    """Create a mock response for testing purposes."""
    # Simple mock response based on the description
    if "blur" in description.lower():
        return {
            "cleaned_description": description,
            "action_types": ["visual_blur"],
            "content_types": ["substance_brand"],
            "media_elements": ["brand_logo"],
            "censored_items": [
                {
                    "content": "Liquor label and brand names",
                    "reference": None,
                    "action": "visual_blur",
                    "content_types": ["substance_brand"],
                    "media_element": "brand_logo",
                    "replacement": None
                }
            ],
            "reason": None
        }
    elif "mute" in description.lower() or "bleep" in description.lower():
        return {
            "cleaned_description": description,
            "action_types": ["audio_mute", "audio_replace"],
            "content_types": ["profanity"],
            "media_elements": ["dialogue_speech"],
            "censored_items": [
                {
                    "content": "Profanity in dialogue",
                    "reference": "profanity",
                    "action": "audio_mute",
                    "content_types": ["profanity"],
                    "media_element": "dialogue_speech",
                    "replacement": "bleep sound"
                }
            ],
            "reason": None
        }
    else:
        # Generic response
        return {
            "cleaned_description": description,
            "action_types": ["deletion"],
            "content_types": ["profanity"],
            "media_elements": ["dialogue_speech"],
            "censored_items": [
                {
                    "content": "Content in description",
                    "reference": None,
                    "action": "deletion",
                    "content_types": ["profanity"],
                    "media_element": "dialogue_speech",
                    "replacement": None
                }
            ],
            "reason": None
        }

def flatten_json_for_csv(json_data, original_row):
    """Convert the JSON response into a format suitable for CSV output."""
    if not json_data:
        return None
    
    # Start with the original row data
    flattened = original_row.copy()
    
    # Add the top-level fields
    flattened['ai_cleaned_description'] = json_data.get('cleaned_description', '')
    flattened['ai_action_types'] = ';'.join(json_data.get('action_types', []))
    flattened['ai_content_types'] = ';'.join(json_data.get('content_types', []))
    flattened['ai_media_elements'] = ';'.join(json_data.get('media_elements', []))
    flattened['ai_reason'] = json_data.get('reason', '')
    
    # Handle censored items - we'll create one row per censored item
    censored_items = json_data.get('censored_items', [])
    if not censored_items:
        return [flattened]
    
    result_rows = []
    for i, item in enumerate(censored_items):
        item_row = flattened.copy()
        item_row['censored_item_index'] = i + 1
        item_row['censored_content'] = item.get('content', '')
        item_row['censored_reference'] = item.get('reference', '')
        item_row['censored_action'] = item.get('action', '')
        item_row['censored_content_types'] = ';'.join(item.get('content_types', []))
        item_row['censored_media_element'] = item.get('media_element', '')
        item_row['censored_replacement'] = item.get('replacement', '')
        result_rows.append(item_row)
    
    return result_rows

def get_processed_ids(processed_log_file):
    """Read the processed IDs log file and return a set of processed (certificate_id, cut_no) tuples"""
    processed_ids = set()
    
    if os.path.exists(processed_log_file):
        try:
            with open(processed_log_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        parts = line.split(',')
                        if len(parts) >= 2:
                            certificate_id, cut_no = parts[0], parts[1]
                            processed_ids.add((certificate_id, str(cut_no)))
                
            logger.info(f"Loaded {len(processed_ids)} processed IDs from log")
        except Exception as e:
            logger.error(f"Error reading processed IDs log: {e}")
    else:
        logger.info("No processed IDs log found, starting fresh")
    
    return processed_ids

def update_processed_id_log(processed_log_file, cert_id, cut_no):
    """Append a single processed ID to the log file immediately"""
    try:
        with open(processed_log_file, 'a') as f:
            f.write(f"{cert_id},{cut_no}\n")
            f.flush()  # Ensure it's written to disk
    except Exception as e:
        logger.error(f"Error updating log for {cert_id},{cut_no}: {e}")

def extract_processed_ids_from_output(output_file):
    """Extract all processed IDs from the output CSV file and update the log file"""
    if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
        logger.info(f"No output file found or file is empty")
        return set()
    
    try:
        df = pd.read_csv(output_file)
        if 'certificate_id' in df.columns and 'cut_no' in df.columns:
            processed_ids = set(zip(df['certificate_id'].astype(str), df['cut_no'].astype(str)))
            logger.info(f"Extracted {len(processed_ids)} processed IDs from output file")
            return processed_ids
        else:
            logger.warning(f"Output file lacks 'certificate_id' or 'cut_no' columns")
            return set()
    except Exception as e:
        logger.error(f"Error extracting processed IDs from output file: {e}")
        return set()

def process_csv(input_file, output_file, log_file=None, limit=None, use_mock=False, rebuild_log=False):
    """Process the CSV file and output the results to a new CSV file,
    saving incrementally and resuming if interrupted."""
    # Set environment variable for mock responses if needed
    if use_mock:
        os.environ["USE_MOCK_RESPONSE"] = "true"
    else:
        # Ensure mock response is disabled if not explicitly requested
        if "USE_MOCK_RESPONSE" in os.environ:
            del os.environ["USE_MOCK_RESPONSE"]

    # Set default log file path if not provided
    if log_file is None:
        # Store log in the same directory as this script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        log_file = os.path.join(current_dir, "processed_ids.log")
    
    # If rebuild_log flag is set, extract IDs from output file and rebuild log
    if rebuild_log and os.path.exists(output_file):
        logger.info("Rebuilding processed IDs log from output file")
        processed_ids = extract_processed_ids_from_output(output_file)
        if processed_ids:
            if os.path.exists(log_file):
                # Backup the existing log file
                backup_path = log_file + '.bak'
                try:
                    os.rename(log_file, backup_path)
                    logger.info(f"Backed up existing log file")
                except Exception as e:
                    logger.error(f"Error backing up log file: {e}")
            
            # Create a new log file with extracted IDs
            with open(log_file, 'w') as f:
                for cert_id, cut_no in processed_ids:
                    f.write(f"{cert_id},{cut_no}\n")
            
            logger.info(f"Rebuilt log file with {len(processed_ids)} IDs")
    else:
        # Get already processed IDs from log file
        processed_ids = get_processed_ids(log_file)

    # Define columns
    original_columns = [
        'certificate_id', 'film_name', 'film_name_full', 'language', 'duration_mins',
        'description', 'cut_no',
        'deleted_mins', 'replaced_mins', 'inserted_mins', 'total_modified_time_mins',
        'cert_date', 'cert_no', 'applicant', 'certifier'
    ]
    ai_columns = [
        'ai_cleaned_description', 'ai_action_types', 'ai_content_types', 'ai_media_elements', 'ai_reason',
        'censored_item_index', 'censored_content', 'censored_reference', 'censored_action',
        'censored_content_types', 'censored_media_element', 'censored_replacement'
    ]
    columns_to_keep = original_columns + ai_columns

    # Read the input CSV file
    try:
        df = pd.read_csv(input_file)
        logger.info(f"Loaded input CSV with {len(df)} rows")
    except Exception as e:
        logger.error(f"Error loading input CSV file: {e}")
        return

    # Filter out already processed rows
    original_row_count = len(df)
    
    # Ensure id columns exist in input df before attempting to filter
    if 'certificate_id' in df.columns and 'cut_no' in df.columns:
        # Convert to string for consistent matching
        df['certificate_id'] = df['certificate_id'].astype(str)
        df['cut_no'] = df['cut_no'].astype(str)
        
        df_to_process = df[~df.apply(lambda row: (row['certificate_id'], row['cut_no']) in processed_ids, axis=1)]
        processed_count = original_row_count - len(df_to_process)
        if processed_count > 0:
            logger.info(f"Skipping {processed_count} already processed rows")
        else:
            logger.info("No rows to skip based on log file")
    else:
        logger.warning("Input file lacks ID columns. Processing all rows")
        df_to_process = df

    if df_to_process.empty:
        logger.info("All necessary rows already processed")
        return # Exit if nothing to do

    # Limit the number of rows to process if specified (after filtering)
    if limit is not None: # Allow limit=0
        df_to_process = df_to_process.head(limit)
        logger.info(f"Processing limit applied: {len(df_to_process)} rows for this run")

    if df_to_process.empty and limit is not None:
        logger.info("No rows left to process after applying limit")
        return

    # Determine if header needs to be written
    write_header = not os.path.exists(output_file) or (os.path.exists(output_file) and os.path.getsize(output_file) == 0)

    # Prepare to track newly processed IDs
    newly_processed_ids = set()

    # Process each remaining row
    logger.info(f"Starting processing for {len(df_to_process)} rows...")
    for index, row in tqdm(df_to_process.iterrows(), total=len(df_to_process), desc="Processing descriptions"):
        # Get the description
        description = row.get('description', '')

        # Process the description
        json_result = process_description(description)

        # Flatten the JSON result
        flattened_rows = flatten_json_for_csv(json_result, row.to_dict())

        if not flattened_rows:
            # Handle case where no censored items found or error occurred
            # Create a row with original data + empty AI fields
            row_dict = row.to_dict()
            row_dict.update({
                'ai_cleaned_description': json_result.get('cleaned_description', '') if json_result else '', # Try to keep cleaned desc if available
                'ai_action_types': '', 'ai_content_types': '',
                'ai_media_elements': '', 'ai_reason': json_result.get('reason', '') if json_result else '', # Try to keep reason if available
                'censored_item_index': pd.NA, 'censored_content': '', 'censored_reference': '',
                'censored_action': '', 'censored_content_types': '', 'censored_media_element': '',
                'censored_replacement': ''
            })
            flattened_rows = [row_dict] # Create a list containing this single row dict

        # Convert batch to DataFrame
        batch_df = pd.DataFrame(flattened_rows)

        # Ensure all necessary columns exist, fill missing with NA/empty string appropriately
        for col in columns_to_keep:
            if col not in batch_df.columns:
                 # Basic type guessing for default values
                 if col in ['censored_item_index', 'deleted_mins', 'replaced_mins', 'inserted_mins', 'total_modified_time_mins', 'duration_mins']:
                     batch_df[col] = pd.NA
                 else:
                     batch_df[col] = '' # Default to empty string for others

        # Reorder/select columns
        batch_df = batch_df[columns_to_keep]

        # Append the batch to the CSV file
        try:
            batch_df.to_csv(output_file, mode='a', header=write_header, index=False, lineterminator='\n')
            write_header = False # Header is written only once
            
            # Track this ID as processed and update log file immediately
            cert_id = str(row.get('certificate_id', ''))
            cut_no = str(row.get('cut_no', ''))
            if cert_id and cut_no:  # Only add valid IDs
                newly_processed_ids.add((cert_id, cut_no))
                # Update log file immediately after processing
                update_processed_id_log(log_file, cert_id, cut_no)
                
        except Exception as e:
            # Log error with identifying info if possible
            cert_id = row.get('certificate_id', 'UNKNOWN_ID')
            cut_num = row.get('cut_no', 'UNKNOWN_CUT')
            logger.error(f"Error writing batch for ID: {cert_id}, Cut: {cut_num}: {e}")
            # Continue with next row

    # We still keep a final log update in case we missed any for some reason
    if newly_processed_ids:
        logger.info(f"Processed {len(newly_processed_ids)} new rows")
    else:
        logger.info("No new rows processed")

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process descriptions using AI and update CSV file')
    parser.add_argument('--input', default=None, help='Path to input CSV file')
    parser.add_argument('--output', default=None, help='Path to output CSV file')
    parser.add_argument('--log', default=None, help='Path to processed IDs log file')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of descriptions to process')
    parser.add_argument('--mock', action='store_true', help='Use mock responses for testing')
    parser.add_argument('--rebuild-log', action='store_true', help='Rebuild the processed IDs log from output file')
    
    args = parser.parse_args()
    
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define default paths relative to the current directory if not provided
    input_file = args.input or os.path.join(current_dir, "..", "..", "data", "complete_data.csv")
    output_file = args.output or os.path.join(current_dir, "..", "..", "data", "processed_data.csv")
    log_file = args.log or os.path.join(current_dir, "processed_ids.log")  # Store log in analysis folder
    
    # Check if the input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file not found at {input_file}")
        logger.info(f"Current directory: {current_dir}")
        exit(1)
    
    # Process the CSV file
    process_csv(
        input_file=input_file, 
        output_file=output_file, 
        log_file=log_file, 
        limit=args.limit, 
        use_mock=args.mock, 
        rebuild_log=args.rebuild_log
    ) 