import pandas as pd
import sqlite3
import glob
import os

# Directory containing the cleaned CSV files
input_dir = 'cleaned_csv_files_2019'

# List all cleaned CSV files in the directory
csv_files = glob.glob(os.path.join(input_dir, '*.csv'))

# SQLite database file
db_file = 'ny_taxi_data.db'

# Connect to SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create table schema
create_table_query = """
CREATE TABLE IF NOT EXISTS taxi_trips (
    VendorID INTEGER,
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count INTEGER,
    trip_distance REAL,
    RatecodeID INTEGER,
    store_and_fwd_flag TEXT,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    congestion_surcharge REAL,
    trip_duration REAL,
    average_speed REAL
);
"""
cursor.execute(create_table_query)

# Function to load data into the database
def load_data_to_db(df, conn):
    df.to_sql('taxi_trips', conn, if_exists='append', index=False)

# Process each cleaned CSV file
for file in csv_files:
    chunks = pd.read_csv(file, chunksize=100000)
    for chunk in chunks:
        load_data_to_db(chunk, conn)

# Commit changes and close the connection
conn.commit()
conn.close()

