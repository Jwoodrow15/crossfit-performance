import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import ssl
import random
import time
from tqdm import tqdm
import logging
import warnings
import sys
import os  # Added to check file existence

# Suppress FutureWarnings (optional, but recommended to fix them)
warnings.simplefilter(action='ignore', category=FutureWarning)

# Create a custom SSL context to skip certificate verification
sslcontext = ssl.create_default_context()
sslcontext.check_hostname = False
sslcontext.verify_mode = ssl.CERT_NONE

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all logs

# File handler for detailed logs (DEBUG and above) - specific to male scraping
file_handler = logging.FileHandler("scrape_male.log")
file_handler.setLevel(logging.DEBUG)  # Set to DEBUG
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Stream handler for console logs (WARNING and above)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Benchmark columns to scrape
BENCHMARK_COLS = ["Back Squat", "Clean and Jerk", "Deadlift", "Fran", "Run 5k", "Snatch"]

# Batch configuration
BATCH_SIZE = 250          # Number of athletes per batch
TOTAL_LIMIT = 200000      # Total number of athletes to process in this run
DELAY_MIN = 15            # Minimum delay in seconds between batches
DELAY_MAX = 30            # Maximum delay in seconds between batches

# Asynchronous scraping function to get specific benchmarks
async def fetch_benchmarks(session, semaphore, athlete_id):
    url = f"https://games.crossfit.com/athlete/{athlete_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/94.0.4606.81 Safari/537.36"
        )
    }

    async with semaphore:
        retry_count = 0
        max_retries = 5
        delay = 60  # Initial delay for exponential backoff

        while retry_count <= max_retries:
            try:
                async with session.get(url, headers=headers, ssl=sslcontext) as response:
                    if response.status in [429, 403]:
                        logger.warning(
                            f"Received status {response.status} for athlete {athlete_id}. "
                            f"Waiting {delay} seconds before retrying."
                        )
                        await asyncio.sleep(delay)
                        retry_count += 1
                        delay *= 2  # Exponential backoff
                        continue
                    elif response.status != 200:
                        logger.warning(f"Unexpected status {response.status} for athlete {athlete_id}")
                        return {"competitorId": athlete_id}

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Scrape the benchmark data
                    benchmarks = {}
                    benchmark_sections = soup.select('div.stats-section')  # Select all sections

                    # Iterate over each stats-section to extract benchmarks
                    for section in benchmark_sections:
                        benchmark_table = section.select_one('table.stats tbody')
                        if benchmark_table:
                            for row in benchmark_table.select('tr'):
                                th = row.select_one('th')
                                td = row.select_one('td')
                                if not th or not td:
                                    continue  # Skip if th or td is missing
                                key = th.text.strip()
                                value = td.text.strip()

                                # Flexible matching: case-insensitive
                                if key.lower() in [col.lower() for col in BENCHMARK_COLS]:
                                    # Find the exact benchmark column name
                                    matched_col = next(
                                        (col for col in BENCHMARK_COLS if col.lower() == key.lower()), key
                                    )
                                    benchmarks[matched_col] = value

                    # Ensure all benchmark columns are present
                    for col in BENCHMARK_COLS:
                        if col not in benchmarks:
                            benchmarks[col] = "--"  # Mark missing benchmarks as "--"

                    logger.info(f"Successfully scraped athlete {athlete_id}: {benchmarks}")

                    # Random delay between requests to mimic human behavior
                    await asyncio.sleep(random.uniform(.5, 1.5))

                    return {"competitorId": athlete_id, **benchmarks}

            except Exception as e:
                logger.error(f"Error fetching athlete {athlete_id}: {e}. Retrying after {delay} seconds.")
                await asyncio.sleep(delay)
                retry_count += 1
                delay *= 2

        # After max retries, mark benchmarks as "error"
        logger.error(f"Failed to fetch athlete {athlete_id} after {max_retries} retries.")
        return {"competitorId": athlete_id, **{col: "error" for col in BENCHMARK_COLS}}

# Asynchronous function to scrape athletes with missing benchmarks
async def scrape_athletes_with_missing_benchmarks(athlete_ids):
    semaphore = asyncio.Semaphore(15)  # Increased concurrency for larger batches
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
        tasks = [fetch_benchmarks(session, semaphore, athlete_id) for athlete_id in athlete_ids]
        results = []
        # Use tqdm to display progress for the current batch
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Processing Athletes', unit='athlete'):
            res = await f
            results.append(res)
        return results

# Function to scrape benchmarks and merge with the CSV
def scrape_and_merge_with_csv(input_file, output_file, batch_size=250, total_limit=10000, delay_min=15, delay_max=30):
    # Record total start time
    total_start_time = time.time()

    # Determine which file to read from
    if os.path.exists(output_file):
        athlete_data = pd.read_csv(output_file, low_memory=False, dtype=str, quotechar='"')  # Handling quote issues in CSV
        logger.info(f"Loaded existing data from '{output_file}'")
    else:
        athlete_data = pd.read_csv(input_file, low_memory=False, dtype=str, quotechar='"')  # Handling quote issues in CSV
        logger.info(f"Loaded data from '{input_file}'")

    # Ensure 'competitorId' column is of type str
    athlete_data['competitorId'] = athlete_data['competitorId'].astype(str)

    # Ensure benchmark columns exist and are of type 'object'
    for col in BENCHMARK_COLS:
        if col not in athlete_data.columns:
            athlete_data[col] = pd.NA
        athlete_data[col] = athlete_data[col].astype('object')  # Ensure dtype is 'object'

    # Replace placeholder values with NaN and infer object types
    athlete_data[BENCHMARK_COLS] = athlete_data[BENCHMARK_COLS].replace({'': pd.NA, 'NONE': pd.NA}).infer_objects(copy=False)

    # Check for duplicate competitorIds and remove them
    if athlete_data['competitorId'].duplicated().any():
        duplicates = athlete_data[athlete_data['competitorId'].duplicated()]['competitorId'].unique()
        logger.warning(f"Duplicate competitorIds found and removed: {duplicates}")
        athlete_data = athlete_data.drop_duplicates(subset='competitorId', keep='first')

    # Initialize processed count
    processed_count = 0

    # Calculate how many athletes are available to process
    total_available = athlete_data[athlete_data[BENCHMARK_COLS].isna().all(axis=1)].shape[0]
    total_to_process = min(total_limit, total_available)
    num_batches = (total_to_process // batch_size) + (1 if total_to_process % batch_size != 0 else 0)

    logger.info(f"Starting scraping: {total_to_process} athletes in {num_batches} batches of {batch_size} each.")

    for batch_num in range(num_batches):
        # Identify athletes with missing benchmark data (all benchmarks are NaN)
        unprocessed_data = athlete_data[athlete_data[BENCHMARK_COLS].isna().all(axis=1)]
        athlete_ids = unprocessed_data['competitorId'].tolist()

        # Limit to batch_size
        current_batch_size = min(batch_size, total_to_process - processed_count)
        batch_ids = athlete_ids[:current_batch_size]
        actual_batch_size = len(batch_ids)

        if actual_batch_size == 0:
            logger.info("No more athletes to process.")
            break

        # Display batch information in the terminal using tqdm.write
        tqdm.write(f"Batch {batch_num + 1}/{num_batches}")

        logger.info(f"Processing batch {batch_num + 1}/{num_batches} with {actual_batch_size} athletes.")

        # Get count before scraping
        back_squat_before = athlete_data['Back Squat'].notna().sum()

        # Scrape benchmarks asynchronously
        scraped_data = asyncio.run(scrape_athletes_with_missing_benchmarks(batch_ids))

        # Convert the scraped data to a DataFrame
        benchmarks_df = pd.DataFrame(scraped_data)

        # Ensure 'competitorId' is of type str in benchmarks_df
        benchmarks_df['competitorId'] = benchmarks_df['competitorId'].astype(str)

        # Ensure all expected columns are present in benchmarks_df
        for col in ["competitorId"] + BENCHMARK_COLS:
            if col not in benchmarks_df.columns:
                benchmarks_df[col] = "error"  # Set missing benchmarks as "error"

        # Set the data types of benchmark columns to 'object' in benchmarks_df
        for col in BENCHMARK_COLS:
            benchmarks_df[col] = benchmarks_df[col].astype('object')

        # Merge the scraped benchmarks with the existing athlete data using 'update'
        athlete_data.set_index('competitorId', inplace=True)
        benchmarks_df.set_index('competitorId', inplace=True)

        # Update benchmark columns with new data for the processed athletes
        athlete_data.update(benchmarks_df)

        # Reset index to turn 'competitorId' back into a column
        athlete_data.reset_index(inplace=True)

        # Get Back Squat count after scraping
        back_squat_after = athlete_data['Back Squat'].notna().sum()

        # Use tqdm.write to print the output clearly after progress bar finishes
        tqdm.write(f"Back Squat count before scraping: {back_squat_before} | Back Squat count after scraping: {back_squat_after}")

        # Save the merged data to the CSV file after processing
        athlete_data.to_csv(output_file, index=False)
        logger.info(f"All athlete data with benchmarks saved as '{output_file}'")

        # Update processed count
        processed_count += actual_batch_size

        # If there are more batches to process, wait before next batch
        if batch_num < num_batches - 1:
            delay = random.randint(delay_min, delay_max)
            logger.info(f"Waiting for {delay} seconds before next batch.")
            time.sleep(delay)

    # Calculate and print total processing time
    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time
    logger.info(f"\nTotal time to process all athletes: {total_elapsed_time:.2f} seconds.")

    print("Scraping completed.")

# Entry point of the script
if __name__ == "__main__":
    # Define input and output file paths
    input_csv = "crossfit_open_2024_male_data.csv"  # Male data input
    output_csv = "crossfit_open_2024_male_with_benchmarks.csv"  # Male data output

    # Call the function with your CSV file paths
    scrape_and_merge_with_csv(
        input_file=input_csv,
        output_file=output_csv,
        batch_size=BATCH_SIZE,
        total_limit=TOTAL_LIMIT,
        delay_min=DELAY_MIN,
        delay_max=DELAY_MAX
    )
