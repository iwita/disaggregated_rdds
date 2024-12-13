import pandas as pd
import numpy as np
import csv

# Parameters
num_cities = 10000  # Total cities
num_records_per_city = 100000  # Records per city
chunk_size = 100  # Number of cities per chunk
start_date = "2024-01-01"
end_date = "2024-12-31"

# Generate date range for the entire year
date_range = pd.date_range(start=start_date, end=end_date, freq="H")  # Hourly data

# File paths
energy_csv_path = "massive_energy_data.csv"
weather_csv_path = "massive_weather_data.csv"

# Initialize CSV files with headers
with open(energy_csv_path, mode="w", newline="") as energy_file, \
     open(weather_csv_path, mode="w", newline="") as weather_file:
    energy_writer = csv.writer(energy_file)
    weather_writer = csv.writer(weather_file)
    energy_writer.writerow(["City", "Timestamp", "EnergyConsumption"])
    weather_writer.writerow(["City", "Timestamp", "Temperature"])

# Function to generate data for a single city
def generate_city_data(city_id, num_records):
    city_name = f"City{city_id}"
    timestamps = np.random.choice(date_range, num_records, replace=True)
    energy_data = []
    weather_data = []
    for timestamp in timestamps:
        # Convert numpy datetime64 to pandas Timestamp
        timestamp = pd.Timestamp(timestamp)
        # Energy data
        energy_consumption = np.random.uniform(500, 2000)  # Energy in kWh
        energy_data.append([city_name, timestamp.strftime("%Y-%m-%d %H:%M"), round(energy_consumption, 2)])
        # Weather data
        temperature = np.random.uniform(-10, 40)  # Temperature in Celsius
        weather_data.append([city_name, timestamp.strftime("%Y-%m-%d %H:%M"), round(temperature, 2)])
    return energy_data, weather_data

# Generate data in chunks
for chunk_start in range(1, num_cities + 1, chunk_size):
    chunk_end = min(chunk_start + chunk_size - 1, num_cities)
    energy_chunk = []
    weather_chunk = []
    for city_id in range(chunk_start, chunk_end + 1):
        city_energy_data, city_weather_data = generate_city_data(city_id, num_records_per_city)
        energy_chunk.extend(city_energy_data)
        weather_chunk.extend(city_weather_data)

    # Append to CSV files
    with open(energy_csv_path, mode="a", newline="") as energy_file, \
         open(weather_csv_path, mode="a", newline="") as weather_file:
        energy_writer = csv.writer(energy_file)
        weather_writer = csv.writer(weather_file)
        energy_writer.writerows(energy_chunk)
        weather_writer.writerows(weather_chunk)

    print(f"Processed cities {chunk_start} to {chunk_end}")

print("Data generation complete!")
