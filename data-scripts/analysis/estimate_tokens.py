import pandas as pd
import tiktoken
import os

def estimate_input_tokens(csv_file_path, description_column='description'):
    """
    Estimates the total number of input tokens required to process descriptions
    in a CSV file using the tiktoken library.

    Args:
        csv_file_path (str): The path to the input CSV file.
        description_column (str): The name of the column containing the text descriptions.

    Returns:
        tuple: A tuple containing:
            - total_descriptions (int): The total number of descriptions processed.
            - total_tokens (int): The estimated total number of input tokens.
            Returns (0, 0) if the file cannot be read or the column is not found.
    """
    try:
        df = pd.read_csv(csv_file_path)
        print(f"Successfully loaded CSV: {csv_file_path} with {len(df)} rows.")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return 0, 0
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return 0, 0

    if description_column not in df.columns:
        print(f"Error: Description column '{description_column}' not found in the CSV.")
        return 0, 0

    # Use cl100k_base as it's commonly used for models like GPT-4 and often a good proxy for Gemini
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
    except Exception as e:
        print(f"Error initializing tiktoken encoder: {e}")
        print("Make sure tiktoken is installed (`pip install tiktoken`)")
        return 0, 0

    total_tokens = 0
    total_descriptions = 0

    # Iterate through descriptions, handling potential NaN values
    for description in df[description_column].fillna(''): # Replace NaN with empty string
        if isinstance(description, str):
            tokens = encoding.encode(description)
            total_tokens += len(tokens)
            total_descriptions += 1
        else:
            # Handle cases where the column might contain non-string data unexpectedly
            print(f"Warning: Skipping non-string value in description column: {description}")


    return total_descriptions, total_tokens

if __name__ == "__main__":
    # Assuming the script is in data-scripts/analysis
    # Construct the path to data/complete_data.csv relative to the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "..", "..", "data", "complete_data.csv")
    
    # Check if the constructed path exists
    if not os.path.exists(csv_path):
         print(f"Error: Input CSV file not found at the expected path: {csv_path}")
         print("Please ensure the script is in the 'data-scripts/analysis' directory and the data file is in 'data/complete_data.csv'")
    else:
        num_descriptions, estimated_tokens = estimate_input_tokens(csv_path)

        if num_descriptions > 0:
            print(f"--- Token Estimation ---")
            print(f"Total descriptions to process: {num_descriptions}")
            print(f"Estimated total INPUT tokens: {estimated_tokens}")
            print(f"Average INPUT tokens per description: {estimated_tokens / num_descriptions:.2f}")
            print("Note: This only estimates INPUT tokens. Output tokens will depend on the length and complexity of the generated JSON analysis.") 