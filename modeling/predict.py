import pandas as pd
import os
from datetime import datetime

# Function to read relevant CSVs from a directory into a single DataFrame
def read_relevant_csvs(directory, start_date, end_date):
    df_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                # Extract date from the file name
                date_str = file.split('=')[1].split('.')[0]
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if start_date <= file_date <= end_date:
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)
                    df_list.append(df)
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def predict_flight_delays(departure_city, destination, start_date, end_date):
    # Define the directories
    flights_dir = "data_to_predict/data/flights/spark_output/"
    weather_dir = "data_to_predict/data/weather/spark_output/"

    # Convert string dates to date objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Read and filter the flights and weather data
    flights_df = read_relevant_csvs(flights_dir, start_date, end_date)
    weather_df = read_relevant_csvs(weather_dir, start_date, end_date)


    # Ensure the timestamp columns are in datetime format
    flights_df['rounded_hour'] = pd.to_datetime(flights_df['rounded_hour'])
    weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])


    # Filter based on departure city and destination
    filtered_flights_df = flights_df[
        (flights_df['from_city'] == departure_city.lower()) & 
        (flights_df['to_city'] == destination.lower())
    ]

    # Join flights and weather data
    merged_df = pd.merge(
        filtered_flights_df, 
        weather_df, 
        left_on=['rounded_hour', 'to'], 
        right_on=['timestamp', 'location'], 
        how='inner'
    )
    '''
    # Select and rename columns for the final output
    final_df = merged_df[[
        'date', 'from_city', 'to_city', 'aircraft', 'flight_time', 
        'scheduled_time_departure', 'actual_time_departure', 'scheduled_time_arrival', 
        'status', 'temperature', 'wind_direction', 'wind_speed', 
        'humidity', 'cloud_cover', 'precip'
    ]]
    final_df.columns = [
        'Date', 'From', 'To', 'Aircraft', 'Flight Time', 'STD', 'ATD', 'STA', 
        'Status', 'Delay Time', 'Temperature', 'Wind Direction', 'Wind Speed', 
        'Humidity', 'Cloud Cover', 'Precipitation'
    ]
    '''
    # Replace NaN values with None
    merged_df = merged_df.where(pd.notnull(merged_df), '')

    # Convert the final DataFrame to a list of dictionaries
    flights = merged_df.to_dict(orient='records')

    return flights
