# import os
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import pyarrow.parquet as pq

# # Create a directory to save CSV files
# output_dir = 'csv_files_2019'
# os.makedirs(output_dir, exist_ok=True)

# # URL of the TLC trip record data page
# url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# # Send a GET request to the page
# response = requests.get(url)
# soup = BeautifulSoup(response.content, 'html.parser')

# # Find all links to parquet files
# links = soup.find_all('a', href=True)
# parquet_links = [link['href'] for link in links if '2019' in link['href'] and link['href'].endswith('.parquet')]

# for link in parquet_links:
#     full_url = link if link.startswith('http') else 'https://www.nyc.gov' + link
    
#     # Download the parquet file
#     response = requests.get(full_url)
#     filename = os.path.join(output_dir, full_url.split('/')[-1])
    
#     # Save the parquet file
#     with open(filename, 'wb') as f:
#         f.write(response.content)
    
#     # Convert parquet to CSV
#     table = pq.read_table(filename)
#     df = table.to_pandas()
#     csv_filename = filename.replace('.parquet', '.csv')
#     df.to_csv(csv_filename, index=False)
    
#     # Remove the parquet file
#     os.remove(filename)

# print("All files have been downloaded and converted to CSV.")


import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyarrow.parquet as pq

# Create a directory to save CSV files
output_dir = 'csv_files_2019'
os.makedirs(output_dir, exist_ok=True)

# URL of the TLC trip record data page
url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Send a GET request to the page
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Find all links to parquet files
links = soup.find_all('a', href=True)
parquet_links = [link['href'] for link in links if '2019' in link['href'] and link['href'].endswith('.parquet')]

for link in parquet_links:
    full_url = link if link.startswith('http') else 'https://www.nyc.gov' + link
    
    # Define filenames
    parquet_filename = os.path.join(output_dir, full_url.split('/')[-1])
    csv_filename = parquet_filename.replace('.parquet', '.csv')
    
    # Skip if CSV file already exists
    if os.path.exists(csv_filename):
        print(f"{csv_filename} already exists. Skipping download.")
        continue
    
    # Download the parquet file
    response = requests.get(full_url)
    
    # Save the parquet file
    with open(parquet_filename, 'wb') as f:
        f.write(response.content)
    
    # Convert parquet to CSV
    try:
        table = pq.read_table(parquet_filename)
        df = table.to_pandas()
        df.to_csv(csv_filename, index=False)
    except Exception as e:
        print(f"Failed to process {parquet_filename}: {e}")
    
    # Remove the parquet file to save space
    os.remove(parquet_filename)

print("All files have been processed.")
