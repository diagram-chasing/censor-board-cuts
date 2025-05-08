#!/usr/bin/env python3
import os
import sys
import subprocess
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_script(script_path, args=None, cwd=None):
    """
    Run a script with the specified working directory
    
    Args:
        script_path: Path to the script to run
        args: List of arguments to pass to the script
        cwd: Working directory for script execution
        
    Returns:
        bool: True if script executed successfully, False otherwise
    """
    try:
        cmd = [sys.executable, str(script_path)]
        if args:
            cmd.extend(args)
            
        logger.info(f"Running {script_path} from {cwd or os.getcwd()}")
        logger.info(f"Command: {' '.join(cmd)}")
        
        # Run the script with specified working directory
        result = subprocess.run(
            cmd, 
            check=True, 
            cwd=cwd
        )
        
        logger.info(f"Successfully completed {script_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error running {script_path}: {e}")
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run the complete data pipeline')
    parser.add_argument('--skip-categories', action='store_true', help='Skip running the categories script')
    parser.add_argument('--skip-certificates', action='store_true', help='Skip running the certificates script')
    parser.add_argument('--skip-imdb', action='store_true', help='Skip running the IMDB script')
    parser.add_argument('--skip-processing', action='store_true', help='Skip running the data processing script')
    parser.add_argument('--force-processing', action='store_true', help='Force data processing even if input files have not changed')
    args = parser.parse_args()
    
    # Get the project root directory (parent of script directory)
    project_root = Path(__file__).parent.parent.absolute()
    
    logger.info("Starting complete data pipeline")
    logger.info(f"Project root: {project_root}")
    
    # Step 1: Run categories main.py
    if not args.skip_categories:
        categories_script = project_root / "scripts" / "categories" / "main.py"
        categories_dir = categories_script.parent
        
        if not categories_script.exists():
            logger.error(f"Categories script not found at {categories_script}")
            return False
            
        logger.info("Running categories script...")
        if not run_script(categories_script, cwd=categories_dir):
            logger.error("Categories script failed. Pipeline stopped.")
            return False
    else:
        logger.info("Skipping categories script as requested")
    
    # Step 2: Run certificates main.py
    if not args.skip_certificates:
        certificates_script = project_root / "scripts" / "certificates" / "main.py"
        certificates_dir = certificates_script.parent
        
        if not certificates_script.exists():
            logger.error(f"Certificates script not found at {certificates_script}")
            return False
            
        logger.info("Running certificates script...")
        if not run_script(certificates_script, cwd=certificates_dir):
            logger.error("Certificates script failed. Pipeline stopped.")
            return False
    else:
        logger.info("Skipping certificates script as requested")
    
    # Step 3: Run analysis main.py
    if not args.skip_processing:
        process_script = project_root / "scripts" / "analysis" / "main.py"
        process_dir = process_script.parent
        
        if not process_script.exists():
            logger.error(f"Processing script not found at {process_script}")
            return False
            
        logger.info("Running data processing script...")
        
        # Add --force flag if requested
        process_args = ["--force"] if args.force_processing else None
        
        if not run_script(process_script, args=process_args, cwd=process_dir):
            logger.error("Data processing script failed. Pipeline stopped.")
            return False
    else:
        logger.info("Skipping data processing as requested")

    # Step 4: Run IMDB main.py
    if not args.skip_imdb:
        imdb_script = project_root / "scripts" / "imdb" / "main.py"
        imdb_dir = imdb_script.parent
        
        if not imdb_script.exists():
            logger.error(f"IMDB script not found at {imdb_script}")
            return False
            
        logger.info("Running IMDB script...")
        if not run_script(imdb_script, cwd=imdb_dir):
            logger.error("IMDB script failed. Pipeline stopped.")
            return False
    else:
        logger.info("Skipping IMDB script as requested")
    
    logger.info("Complete data pipeline finished successfully!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 