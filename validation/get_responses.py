import requests
import random
import polars as pl
import glob
import os

# Set api config
endpoint = 'https://server/endpoint/'  # API endpoint
api_key = 'apikey'  # NCBI API key
# for NCBI API key details, see the following url:
# https://www.ncbi.nlm.nih.gov/books/NBK25497/#chapter2.Usage_Guidelines_and_Requiremen

# Method for cleaning file paths during iterations
def cleanup(path):
    file_paths = glob.glob("data/api_output_*.csv")
    # Write a combined file
    (
        # Read all CSV files and concatenate them into a single DataFrame
        pl.concat([pl.read_csv(file) for file in file_paths], how="vertical")
        # Write the combined dataframe as a CSV
        .write_csv(path)
    )
    # Remove the original files
    for file in file_paths: os.remove(file)

# Read in GenBank strain names
strains = pl.read_csv('data/genbank_strains.csv')['SEQUENCE_GENBANK_STRAIN']

# Define and print seed before sampling
seed = random.randint(1, 100000)
print(f"Seed: {seed}")

# Set the headers
headers = {'accept': 'application/json'}

# Iterate through batches of requests until final count is reached or exceeded
num_workers = 200
batch_count = 0
final_count = 100000
while (batch_count * num_workers) < final_count:
    # Define the terms you want to search for
    terms = strains.sample(num_workers)
    # Add terms to search for
    url = f'{endpoint}fetch-accession/?terms={','.join(terms)}&api_key={api_key}'
    # Send GET request
    response = requests.get(url, headers=headers, timeout=300)
    # Check if the request was successful and print the returned dictionary or terms (keys) and accessions (values)
    if response.status_code == 200:
        response_dict = response.json()
        pl.DataFrame({
            'SEQUENCE_GENBANK_STRAIN': response_dict.keys(),
            'SEQUENCE_GENBANK_ACCESSION': response_dict.values()
        }).write_csv(f'data/api_output_{batch_count}.csv')
        batch_count += 1
        strains = strains.filter(~strains.is_in(terms))
        print(f'Completed {batch_count * num_workers} strain look ups... {final_count - (batch_count * num_workers)} to go!')
        if (final_count - (batch_count * num_workers)) % final_count == 0:
            # Specify the folder path and pattern for your CSV files
            cleanup(f'data/api_output_combined_{num_workers}_{terms_completed}.csv')
            # Print message after cleanup
            print("--- Cleaned up file outputs ---")
    else:
        print(f"Error {response.status_code}: {response.text}")

# Combine/rename final file(s)
cleanup(f'data/api_output.csv')
