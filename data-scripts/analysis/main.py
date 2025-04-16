#!/usr/bin/env python3
import os
import sys
import subprocess
import logging
import argparse
import time
import warnings
import join_and_process
import process_descriptions
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_script(script_path, script_name):
    """Run a Python script and return the success status"""
    start_time = time.time()
    logger.info(f"Starting {script_name}...")
    
    try:
        # Use the same Python interpreter to run the script
        result = subprocess.run([sys.executable, str(script_path)], 
                               check=True, 
                               capture_output=True, 
                               text=True)
        
        # Log the output
        if result.stdout:
            for line in result.stdout.splitlines():
                logger.info(f"{script_name}: {line}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Completed {script_name} in {elapsed_time:.2f}s")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error in {script_name}: {e}")
        if e.stdout:
            for line in e.stdout.splitlines():
                logger.info(f"{script_name}: {line}")
        if e.stderr:
            for line in e.stderr.splitlines():
                logger.error(f"{script_name}: {line}")
        return False
    
    except Exception as e:
        logger.error(f"Unexpected error in {script_name}: {e}")
        return False

def run_script_with_live_output(script_path, args, script_name):
    """Run a Python script with live console output for scripts with progress bars"""
    start_time = time.time()
    logger.info(f"Starting {script_name}...")
    
    try:
        # Don't capture output so it shows in the console in real-time
        process_args = [sys.executable, str(script_path)] + args
        result = subprocess.run(process_args, check=True)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Completed {script_name} in {elapsed_time:.2f}s")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error in {script_name}: {e}")
        return False
    
    except Exception as e:
        logger.error(f"Unexpected error in {script_name}: {e}")
        return False

def main():
    """Main function to orchestrate the data processing pipeline"""
    parser = argparse.ArgumentParser(description='Run the full data processing pipeline')
    parser.add_argument('--skip-join', action='store_true', help='Skip the data joining step')
    parser.add_argument('--skip-process', action='store_true', help='Skip the description processing step')
    parser.add_argument('--rebuild-log', action='store_true', help='Rebuild processed IDs log from existing processed data')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of descriptions to process')
    parser.add_argument('--force', action='store_true', help='Force processing even if files have not changed')
    parser.add_argument('--mock', action='store_true', help='Use mock responses for testing')
    
    args = parser.parse_args()
    
    # Get the current directory (where this script is located)
    current_dir = Path(__file__).parent.absolute()
    
    # Define paths to scripts
    join_script = current_dir / "join_and_process.py"
    process_script = current_dir / "process_descriptions.py"
    
    # Define paths to data files
    data_dir = current_dir.parent.parent / "data"
    complete_data_path = data_dir / "complete_data.csv"
    processed_data_path = data_dir / "processed_data.csv"
    log_file_path = current_dir / "processed_ids.log"
    
    # Ensure the data directory exists
    data_dir.mkdir(exist_ok=True)
    
    # Track overall pipeline success
    pipeline_success = True
    start_time = time.time()
    
    logger.info("Censor Board Cuts Data Processing Pipeline")
    logger.info("------------------------------------------")
    
    if args.rebuild_log:
        logger.info("REBUILDING PROCESSED IDS LOG from existing processed data")
        rebuild_log(processed_data_path)
        return True

    if not args.skip_join:
        logger.info("Running data joining and cleaning...")
        
        # Build command with force flag if provided
        cmd = [sys.executable, str(join_script)]
        if args.force:
            cmd.append('--force')
        
        # Run join_and_process.py
        join_success = subprocess.call(cmd) == 0
        pipeline_success = pipeline_success and join_success
        
        if not join_success:
            logger.error("Join and process step failed. Pipeline halted.")
            return False
        
        # Verify the complete_data.csv file was created
        if not complete_data_path.exists():
            logger.error(f"Expected output file not found: {complete_data_path}")
            logger.error("Join and process step did not produce expected output. Pipeline halted.")
            return False
    else:
        logger.info("Skipping data joining (--skip-join flag used)")
    
    # Step 2: Run process_descriptions.py (if not skipped)
    if not args.skip_process:
        if not process_script.exists():
            logger.error(f"Process script not found at {process_script}")
            return False
        
        # Check if complete_data.csv exists before processing
        if not complete_data_path.exists():
            logger.error(f"Input file for processing not found: {complete_data_path}")
            logger.error("Cannot run process_descriptions.py without input data. Pipeline halted.")
            return False
        
        # Prepare command-line arguments for process_descriptions.py
        process_args = [sys.executable, str(process_script)]
        
        # Add optional arguments based on user input
        if args.rebuild_log:
            process_args.extend(["--rebuild-log"])
        
        if args.limit is not None:
            process_args.extend(["--limit", str(args.limit)])
            
        if args.mock:
            process_args.extend(["--mock"])
        
        # Add input, output, and log file paths
        process_args.extend([
            "--input", str(complete_data_path),
            "--output", str(processed_data_path),
            "--log", str(log_file_path)
        ])
        
        logger.info(f"Running process_descriptions.py with args: {' '.join(str(arg) for arg in process_args[1:])}")
        
        # Run the process_descriptions.py script
        try:
            # Use the special function for live output
            process_success = run_script_with_live_output(
                process_script,
                process_args[2:],  # Skip python and script path
                "process_descriptions.py"
            )
            
            if not process_success:
                logger.error("Processing descriptions step failed")
                pipeline_success = False
            
        except Exception as e:
            logger.error(f"Unexpected error running process_descriptions.py: {e}")
            pipeline_success = False
    else:
        logger.info("Skipping process_descriptions.py step")
    
    # Report overall pipeline status
    elapsed_time = time.time() - start_time
    if pipeline_success:
        logger.info("------------------------------------------")
        logger.info(f"Pipeline completed successfully in {elapsed_time:.2f}s")
    else:
        logger.error(f"Pipeline completed with errors in {elapsed_time:.2f}s")
    
    return pipeline_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 