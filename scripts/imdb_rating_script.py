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
        except HTTPError as e:
            print(f"HTTP error occurred: {e}. Retrying...")
            time.sleep(delay)  # Wait before retrying
        except imdb.IMDbDataAccessError as e:
            print(f"IMDb data access error occurred: {e}. Retrying...")
            time.sleep(delay)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error occurred: {e}. Retrying...")
            time.sleep(delay)  # Wait before retrying
    return None

def process_titles(titles, start_index, end_index, progress_file, progress_lock):
    """
    Process a list of titles and fetch IMDb ratings.
    
    Parameters:
    - titles: list of str, movie/show titles
    - start_index: int, starting index for this batch
    - end_index: int, ending index for this batch
    - progress_file: str, path to the progress CSV file
    - progress_lock: threading.Lock, lock for writing progress safely
    """
    ratings = []
    progress = []
    start_time = time.time()  # Track start time
    for index in range(start_index, end_index):
        title = titles[index]
        rating = get_imdb_rating(title)
        ratings.append(rating)
        progress.append({'Index': index, 'Title': title, 'Rating': rating})
        
         # Print progress every 10 files
        if (index - start_index + 1) % 1 == 0:
            elapsed_time = time.time() - start_time
            print(f"Processing {index - start_index + 1}/{end_index - start_index} titles. Elapsed time: {elapsed_time:.2f} seconds.")

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
        last_index = progress_df['Index'].max()
        titles_to_process = df['title'].iloc[last_index + 1:].tolist()
    else:
        titles_to_process = df['title'].tolist()
    
    # Initialize the ratings list
    ratings = [None] * len(df)
    
    # Set up parallel processing
    batch_size = 5
    num_threads = 4
    progress_lock = threading.Lock()  # Lock for thread-safe progress updates
    start_time = time.time()  # Track start time for overall process
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        num_batches = (len(titles_to_process) + batch_size - 1) // batch_size
        for batch in range(num_batches):
            start_index = batch * batch_size
            end_index = min(start_index + batch_size, len(titles_to_process))
            future = executor.submit(process_titles, df['title'].tolist(), start_index, end_index, progress_file, progress_lock)
            futures.append(future)
        
        for future in as_completed(futures):
            batch_ratings = future.result()
            for i, rating in enumerate(batch_ratings):
                index = start_index + i
                if index < len(ratings):
                    ratings[index] = rating
    
    # Add the ratings to the DataFrame
    df['IMDB_Rating'] = ratings
    
    # Save the updated DataFrame to a new CSV file
    output_file = input_file.replace('.csv', '_Modified.csv')
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



