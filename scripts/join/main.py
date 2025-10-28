#!/usr/bin/env python3
"""
Join script for the censor board cuts project.

This script merges metadata_modifications.csv, imdb.csv, and llm.csv datasets
and saves the result as data.csv according to the schema in data/README.md.
"""

import sys
import logging
import argparse
from pathlib import Path
import pandas as pd
from difflib import SequenceMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def string_similarity(str1, str2):
    if pd.isna(str1) or pd.isna(str2):
        return 0.0
    
    # Convert to lowercase and strip whitespace for better comparison
    str1_clean = str(str1).lower().strip()
    str2_clean = str(str2).lower().strip()
    
    return SequenceMatcher(None, str1_clean, str2_clean).ratio()

def join_datasets(project_root):
    """
    Join the metadata_modifications.csv, imdb.csv, and llm.csv files
    and save the result to data.csv with the schema from README.md
    
    Args:
        project_root: Root directory of the project
    
    Returns:
        bool: True if the operation was successful, False otherwise
    """
    try:
        logger.debug("Joining datasets to create the final data.csv file")
        
        # Define file paths
        metadata_modifications_path = project_root / "data" / "individual_files" / "metadata_modifications.csv"
        imdb_path = project_root / "data" / "raw" / "imdb.csv"
        llm_path = project_root / "data" / "raw" / "llm.csv"
        output_path = project_root / "data" / "data.csv"
        
        # Check if input files exist
        for file_path in [metadata_modifications_path, imdb_path, llm_path]:
            if not file_path.exists():
                logger.error(f"Input file not found: {file_path}")
                return False
        
        # Read the input files
        logger.debug(f"Reading {metadata_modifications_path}")
        metadata_df = pd.read_csv(metadata_modifications_path, dtype={'id': str, 'certificate_id': str})
        
        logger.debug(f"Reading {imdb_path}")
        imdb_df = pd.read_csv(imdb_path, sep=';', dtype={'original_id': str})
        imdb_df.columns = ["imdb_" + col for col in imdb_df.columns]
        imdb_df.rename(columns={'imdb_original_id': 'id'}, inplace=True)
        imdb_df.rename(columns={'imdb_imdb_id': 'imdb_id'}, inplace=True)
        
        logger.debug(f"Reading {llm_path}")
        llm_df = pd.read_csv(llm_path, dtype={'certificate_id': str})
        
        # Join the dataframes
        logger.debug("Merging datasets...")
        logger.debug("Original length of metadata_df: " + str(len(metadata_df)))
        logger.debug("Original length of imdb_df: " + str(len(imdb_df)))
        logger.debug("Original length of llm_df: " + str(len(llm_df)))
        
        # Merge with LLM data using both certificate_id and cut_no as the common keys
        merged_df = pd.merge(
            metadata_df,
            llm_df,
            on=["certificate_id", "cut_no"],
            how="left"
        )

        logger.debug("Length after merging with llm: " + str(len(merged_df)))

        # Merge with IMDb data using id as the common key
        merged_df = pd.merge(
            merged_df,
            imdb_df,
            on="id",
            how="left"
        )

        logger.debug("Length after merging with imdb: " + str(len(merged_df)))

        # Set 0 for blank values in imdb_id column and convert column to int
        merged_df.loc[:, 'imdb_id'] = merged_df['imdb_id'].fillna(0).astype(int)

        # Set 0 for blank values in imdb_year column and convert column to int
        merged_df.loc[:, 'imdb_year'] = merged_df['imdb_year'].fillna(0).astype(int)

        # Drop IMDB columns for rows where movie_name and imdb_title are not similar
        logger.debug("Checking similarity between movie_name and imdb_title...")
        similarity_threshold = 0.85  # Adjust this threshold as needed
        
        # Get all IMDB columns (columns starting with 'imdb_')
        imdb_columns = [col for col in merged_df.columns if col.startswith('imdb_')]
        logger.debug(f"Found {len(imdb_columns)} IMDB columns: {imdb_columns}")
        
        # Calculate similarity scores
        merged_df['similarity_score'] = merged_df.apply(
            lambda row: string_similarity(row['movie_name'], row['imdb_title']) 
            if 'imdb_title' in merged_df.columns else 0.0, 
            axis=1
        )
        
        # Count rows that will be affected
        dissimilar_rows = merged_df['similarity_score'] < similarity_threshold
        num_dissimilar = dissimilar_rows.sum()
        logger.debug(f"Found {num_dissimilar} rows with dissimilar movie names (similarity < {similarity_threshold})")
        
        # Set IMDB columns to NaN for dissimilar rows
        for col in imdb_columns:
            if col in merged_df.columns:
                merged_df.loc[dissimilar_rows, col] = None
        
        # Drop the temporary similarity_score column
        merged_df.drop('similarity_score', axis=1, inplace=True)
        
        logger.debug(f"Cleaned IMDB data for {num_dissimilar} rows with dissimilar movie names")

        
        # Save the final dataset
        logger.debug(f"Saving joined dataset to {output_path}")
        merged_df.to_csv(output_path, index=False)
        
        logger.debug(f"Successfully created {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error joining datasets: {e}")
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Join metadata_modifications, IMDB, and LLM datasets into a single data.csv file'
    )
    args = parser.parse_args()
    
    # Get the project root directory (parent of script directory)
    project_root = Path(__file__).parent.parent.parent.absolute()
    
    logger.info("Starting data join process")
    logger.debug(f"Project root: {project_root}")
    
    # Join the datasets
    if not join_datasets(project_root):
        logger.error("Data joining failed.")
        return False
    
    logger.info("Data join process completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 