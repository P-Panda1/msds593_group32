import pandas as pd
import sys
import os

def merge_datasets(x_file, progress_file):
    """
    Merge the datasets by equating the title from x_file and name from progress_file.
    
    Parameters:
    - x_file: str, path to the x CSV file
    - progress_file: str, path to the progress CSV file
    
    Returns:
    - merged_df: DataFrame, merged DataFrame
    """
    # Load the data
    x_df = pd.read_csv(x_file)
    progress_df = pd.read_csv(progress_file)
    
    # Check if the required columns exist
    if 'title' not in x_df.columns:
        print("Error: 'title' column is missing from the x file.")
        return
    
    if 'Title' not in progress_df.columns or 'Rating' not in progress_df.columns:
        print("Error: 'Title' or 'Rating' column is missing from the progress file.")
        return
    
    # Rename the 'Rating' column to 'IMDB_Rating' in progress_df
    progress_df.rename(columns={'Rating': 'IMDB_Rating'}, inplace=True)
    
    # Merge the datasets on the title and name columns
    merged_df = pd.merge(x_df, progress_df, left_on='title', right_on='Title', how='left')
    
    # Drop the 'Title' column as it's redundant after merging
    merged_df.drop(columns=['Title'], inplace=True)
    
    # Save the merged DataFrame to a new CSV file
    output_file = x_file.replace('.csv', '_modified.csv')
    merged_df.to_csv(output_file, index=False)
    print(f"Merged data saved to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <x_file> <progress_file>")
        sys.exit(1)
    
    x_file = sys.argv[1]
    progress_file = sys.argv[2]
    
    if not os.path.isfile(x_file):
        print(f"Error: The file {x_file} does not exist.")
        sys.exit(1)
    
    if not os.path.isfile(progress_file):
        print(f"Error: The file {progress_file} does not exist.")
        sys.exit(1)
    
    merge_datasets(x_file, progress_file)
