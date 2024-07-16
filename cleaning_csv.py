import pandas as pd
import glob
import os

# Directory containing the CSV files
input_dir = 'csv_files_2019'

# List all CSV files in the directory
csv_files = glob.glob(os.path.join(input_dir, '*.csv'))

# Function to clean and transform data
def clean_transform_data(df):
    # Remove trips with missing or corrupt data
    df = df.dropna()
    
    # Convert pickup and dropoff times to datetime
    datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    # Derive trip duration in minutes
    if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
        df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    
    # Calculate distance in miles (assuming distance column is in miles)
    if 'trip_distance' in df.columns:
        df['trip_distance'] = df['trip_distance']
    
    # Derive average speed in miles per hour
    if 'trip_distance' in df.columns and 'trip_duration' in df.columns:
        df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)
    
    return df

# Initialize an empty list to store dataframes
dataframes = []

# Process each CSV file
for file in csv_files:
    chunk_size = 100000  # Adjust the chunk size based on your system's memory
    for chunk in pd.read_csv(file, chunksize=chunk_size):
        cleaned_chunk = clean_transform_data(chunk)
        dataframes.append(cleaned_chunk)

# Concatenate all dataframes into one
all_data = pd.concat(dataframes, ignore_index=True)

# Aggregate data to calculate total trips and average fare per day
if 'tpep_pickup_datetime' in all_data.columns:
    daily_agg = all_data.groupby(all_data['tpep_pickup_datetime'].dt.date).agg({
        'trip_distance': 'count',     # Total trips
        'total_amount': 'mean'        # Average fare
    }).rename(columns={'trip_distance': 'total_trips', 'total_amount': 'average_fare'})

    # Save the aggregated data to a CSV file
    daily_agg.to_csv('aggregated_data_2019.csv')

    print("Data cleaning, transformation, and aggregation complete. Aggregated data saved to 'aggregated_data_2019.csv'.")
else:
    print("No data available to aggregate.")
