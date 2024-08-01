import imdb
import pandas as pd
import threading
import time
from requests.exceptions import Timeout, RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys

# Initialize IMDb instance
ia = imdb.IMDb()

def get_imdb_rating(title):
    """
    Fetch the IMDb rating for a given movie title with retry logic.
    
    Parameters:
    - title: str, the title of the movie/show
    
    Returns:
    - rating: float, IMDb rating of the movie or None if not found
    """
    retries = 3
    delay = 5
    for attempt in range(retries):
        try:
            search_results = ia.search_movie(title)
            if not search_results:
                return None
            movie = search_results[0]
            ia.update(movie)
            rating = movie.get('rating')
            return rating
        except Timeout:
            print(f"Timeout error while fetching rating for: {title}. Retrying...")
            time.sleep(delay)  # Wait before retrying
        except RequestException as e:
            print(f"Request exception occurred: {e}. Retrying...")
            time.sleep(delay)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error occurred: {e}. Retrying...")
            time.sleep(delay)  # Wait before retrying
    return None

def process_titles(titles, indices, progress_file, progress_lock, batch):
    """
    Process a list of titles and fetch IMDb ratings.
    
    Parameters:
    - titles: list of str, movie/show titles
    - indices: list of int, indices of the titles to process
    - progress_file: str, path to the progress CSV file
    - progress_lock: threading.Lock, lock for writing progress safely
    """
    ratings = []
    progress = []
    start_time = time.time()  # Track start time
    for index in indices:
        title = titles[index]
        rating = get_imdb_rating(title)
        ratings.append(rating)
        progress.append({'Index': index, 'Title': title, 'Rating': rating})
        
        # Print progress every title
        elapsed_time = time.time() - start_time
        print(f"{batch} Processing {index + 1}/{len(titles)} titles. Elapsed time: {elapsed_time:.2f} seconds.")

    # Save progress to CSV
    progress_df = pd.DataFrame(progress)
    with progress_lock:
        if os.path.exists(progress_file):
            existing_df = pd.read_csv(progress_file)
            combined_df = pd.concat([existing_df, progress_df])
        else:
            combined_df = progress_df
        combined_df.to_csv(progress_file, index=False)
    
    return ratings

def add_imdb_ratings(input_file):
    """
    Add IMDb ratings to the dataset and save to a new file.
    
    Parameters:
    - input_file: str, path to the input CSV file
    """
    # Load the data
    df = pd.read_csv(input_file)
    
    # Check if the 'title' column exists
    if 'title' not in df.columns:
        print("Error: 'title' column is missing from the input file.")
        return
    
    # Path for progress log
    progress_file = input_file.replace('.csv', '_progress.csv')
    
    # Load previous progress if exists
    if os.path.exists(progress_file):
        progress_df = pd.read_csv(progress_file)
        processed_indices = progress_df['Index'].tolist()
    else:
        processed_indices = []
    
    # Find unprocessed titles
    all_indices = set(range(len(df)))
    unprocessed_indices = list(all_indices - set(processed_indices))
    
    # Initialize the ratings list
    ratings = [None] * len(df)
    
    # Set up parallel processing
    batch_size = 5
    num_threads = 4
    progress_lock = threading.Lock()  # Lock for thread-safe progress updates
    start_time = time.time()  # Track start time for overall process
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        num_batches = (len(unprocessed_indices) + batch_size - 1) // batch_size
        for batch in range(num_batches):
            batch_indices = unprocessed_indices[batch * batch_size: (batch + 1) * batch_size]
            future = executor.submit(process_titles, df['title'].tolist(), batch_indices, progress_file, progress_lock, batch)
            futures.append(future)
        
        for future in as_completed(futures):
            batch_ratings = future.result()
            for index, rating in zip(batch_indices, batch_ratings):
                if index < len(ratings):
                    ratings[index] = rating
    
    # Fill in ratings from progress_df
    if os.path.exists(progress_file):
        progress_df = pd.read_csv(progress_file)
        for _, row in progress_df.iterrows():
            ratings[int(row['Index'])] = row['Rating']
    
    # Add the ratings to the DataFrame
    df['IMDB_Rating'] = ratings
    
    # Save the updated DataFrame to a new CSV file
    output_file = input_file.replace('.csv', '_modified.csv')
    df.to_csv(output_file, index=False)
    total_elapsed_time = time.time() - start_time
    print(f"Updated data saved to: {output_file}. Total elapsed time: {total_elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <input_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.isfile(input_file):
        print(f"Error: The file {input_file} does not exist.")
        sys.exit(1)
    
    print("Starting creating new file")
    add_imdb_ratings(input_file)
