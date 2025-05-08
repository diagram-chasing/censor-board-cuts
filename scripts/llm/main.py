#!/usr/bin/env python3
import os
import csv
import json
import pandas as pd
import time
import argparse
from dotenv import load_dotenv
import google.generativeai as genai
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
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

# Create the model with the updated schema from the prompt
def setup_model():
    from google import genai
    from google.genai import types

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
    )

    # Updated schema based on the new prompt
    model = "gemini-2.0-flash-lite"
    response_schema = types.Schema(
        type=types.Type.OBJECT,
        required=["cleaned_description", "reference", "action", "content_types", "media_element"],
        properties={
            "cleaned_description": types.Schema(
                type=types.Type.STRING,
                description="Rewritten description in clear, human-readable language without timestamps.",
            ),
            "reference": types.Schema(
                type=types.Type.UNION,
                description="The specific word, entity, or concept censored. Can be a single string, array of strings, or null if no specific reference.",
                union_types=[
                    types.Schema(type=types.Type.STRING),
                    types.Schema(
                        type=types.Type.ARRAY,
                        items=types.Schema(type=types.Type.STRING),
                    ),
                    types.Schema(type=types.Type.NULL),
                ],
            ),
            "action": types.Schema(
                type=types.Type.STRING,
                description="The type of censorship action performed.",
                enum=[
                    "deletion", 
                    "insertion", 
                    "replacement", 
                    "audio_modification", 
                    "visual_modification", 
                    "text_modification", 
                    "content_overlay"
                ],
            ),
            "content_types": types.Schema(
                type=types.Type.ARRAY,
                description="Types of content being censored (1-2 most relevant categories).",
                items=types.Schema(
                    type=types.Type.STRING,
                    enum=[
                        "violence", 
                        "sexual_explicit", 
                        "sexual_suggestive", 
                        "substance", 
                        "profanity", 
                        "political", 
                        "religious", 
                        "identity_reference"
                    ],
                ),
            ),
            "media_element": types.Schema(
                type=types.Type.STRING,
                description="The media element affected by the censorship action.",
                enum=["music", "visual_scene", "text_dialogue", "metadata", "other"],
            ),
        },
    )
    return client, model, response_schema

def process_description(description, timeout=30):
    """Process a description using the Gemini model and return the JSON response."""
    if not description or description.strip() == "":
        return None
    
    try:
        # Set up the model and client
        client, model, response_schema = setup_model()
        
        # Set a timeout for the API call
        start_time = time.time()
        
        # Setup the generation config
        generate_content_config = genai.types.GenerateContentConfig(
            temperature=0.6,
            response_mime_type="application/json",
            response_schema=response_schema,
            system_instruction=genai.types.Part.from_text(text=get_system_instruction())
        )
        
        # Create the content
        contents = [
            genai.types.Content(
                role="user",
                parts=[
                    genai.types.Part.from_text(text=description),
                ],
            ),
        ]
        
        # Use a timeout to prevent hanging
        try:
            response = client.models.generate_content(
                model=model,
                contents=contents,
                config=generate_content_config,
            )
            elapsed_time = time.time() - start_time
            logger.debug(f"API call completed in {elapsed_time:.2f}s")
            
            if hasattr(response, 'text') and response.text:
                return json.loads(response.text)
            elif hasattr(response, 'parts') and response.parts:
                return json.loads(response.parts[0].text)
            else:
                logger.warning("Empty response from API")
                return None
        except Exception as e:
            logger.error(f"API call failed: {e}")
            return None
    except Exception as e:
        logger.error(f"Error processing description: {e}")
        return None

def flatten_json_for_csv(json_data, original_row):
    """Convert the JSON response into a format suitable for CSV output."""
    if not json_data:
        return None
    
    # Start with the original row data
    flattened = original_row.copy()
    
    # Extract data from the updated schema
    flattened['ai_cleaned_description'] = json_data.get('cleaned_description', '')
    flattened['ai_action'] = json_data.get('action', '')
    
    # Handle reference field which can be string, array or null
    reference = json_data.get('reference', None)
    if isinstance(reference, list):
        flattened['ai_reference'] = '|'.join([str(r) for r in reference if r is not None])
    elif reference is not None:
        flattened['ai_reference'] = str(reference)
    else:
        flattened['ai_reference'] = ''
    
    # Handle content_types array
    content_types = json_data.get('content_types', [])
    flattened['ai_content_types'] = '|'.join(content_types) if content_types else ''
    
    # Media element
    flattened['ai_media_element'] = json_data.get('media_element', '')
    
    return [flattened]

def get_system_instruction():
    """Load the system instruction from prompt.txt in the same folder as the script."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    prompt_file = os.path.join(current_dir, "prompt.txt")
    
    try:
        if os.path.exists(prompt_file):
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            logger.error(f"Prompt file not found at {prompt_file}")
            raise FileNotFoundError(f"Required prompt file not found: {prompt_file}")
    except Exception as e:
        logger.error(f"Error reading prompt file: {e}")
        raise

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

def process_csv(input_file, output_file, log_file=None, limit=None, rebuild_log=False):
    """Process the CSV file and output the results to a new CSV file,
    saving incrementally and resuming if interrupted."""

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

    # Define columns for the new updated schema
    ai_columns = [
        'ai_cleaned_description', 'ai_reference', 'ai_action', 'ai_content_types', 'ai_media_element'
    ]
    
    # Read the input CSV file
    try:
        df = pd.read_csv(input_file)
        logger.info(f"Loaded input CSV with {len(df)} rows")
        
        # Determine original columns from the input data
        # Exclude AI columns that might already exist in the input
        original_columns = [col for col in df.columns if not col.startswith('ai_') and col not in ai_columns]
        logger.info(f"Detected {len(original_columns)} original columns from input data")
        
        # Define columns to keep (original + AI)
        columns_to_keep = original_columns + ai_columns
        logger.info(f"Will preserve {len(original_columns)} original columns and add {len(ai_columns)} AI columns")
        
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
            
            # Add fields with empty values
            for col in ai_columns:
                if col == 'ai_cleaned_description' and json_result:
                    row_dict[col] = json_result.get('cleaned_description', '')
                else:
                    row_dict[col] = ''
            
            flattened_rows = [row_dict] # Create a list containing this single row dict

        # Convert batch to DataFrame
        batch_df = pd.DataFrame(flattened_rows)

        # Ensure all necessary columns exist, fill missing with NA/empty string appropriately
        for col in columns_to_keep:
            if col not in batch_df.columns:
                # Basic type guessing for default values
                if col in ['censored_item_index', 'deleted_mins', 'replaced_mins', 'inserted_mins', 'total_modified_time_mins', 'duration_mins']:
                    batch_df[col] = pd.NA
                elif col.startswith('ai_') or col.startswith('censored_'):
                    # AI-generated columns should be empty strings if missing
                    batch_df[col] = ''
                else:
                    # For original columns that are missing, try to get from the original row
                    if col in row.to_dict():
                        batch_df[col] = row[col]
                    else:
                        # If still not found, use empty string as default
                        batch_df[col] = ''
                        logger.debug(f"Column '{col}' not found in original data or processed result")

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
    parser.add_argument('--rebuild-log', action='store_true', help='Rebuild the processed IDs log from output file')
    
    args = parser.parse_args()
    
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define default paths relative to the current directory if not provided
    input_file = args.input or os.path.join(current_dir, "..", "..", "data", "metadata_modifications.csv")
    output_file = args.output or os.path.join(current_dir, "..", "..", "data", "data.csv")
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
        rebuild_log=args.rebuild_log
    )