import sqlite3

# SQLite database file
db_file = 'ny_taxi_data.db'

# Connect to SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Check the number of rows in the table
query = "SELECT COUNT(*) FROM taxi_trips"
cursor.execute(query)
row_count = cursor.fetchone()[0]
print(f"Total number of rows: {row_count}")

# Fetch the first few rows to inspect the data
query = "SELECT * FROM taxi_trips LIMIT 5"
cursor.execute(query)
rows = cursor.fetchall()
for row in rows:
    print(row)

# Check for a specific column data to ensure correct data types and values
query = "SELECT trip_distance, fare_amount, trip_duration FROM taxi_trips LIMIT 5"
cursor.execute(query)
rows = cursor.fetchall()
for row in rows:
    print(f"Trip Distance: {row[0]}, Fare Amount: {row[1]}, Trip Duration: {row[2]}")

# Close the connection
conn.close()
