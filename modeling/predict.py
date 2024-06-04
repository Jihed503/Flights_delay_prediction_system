import pandas as pd
import os
from datetime import datetime
import pickle
import dill
import joblib


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
                    print(f"££££££££££££££££££££££££££{len(df)}££££££££££££££££££££$$$")
                    df_list.append(df)
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def encode_new_data(df, columns_to_encode, value_mappings):
    for column in columns_to_encode:
        if column == 'flight':
            df[column] = df[column].str.strip('()')  # Remove leading and trailing parentheses
        df[column+'_encoded'] = df[column].map(value_mappings[column]).fillna(-1).astype(int)
    return df

def predict_flight_delays(departure_city, destination, start_date, end_date):
    ###-------------------------------Loading Data-------------------------------###
    # Define the directories
    flights_dir = "data_to_predict/data/flights/spark_output/"
    weather_dir = "data_to_predict/data/weather/spark_output/"
    model_path = "classifiers/random_forest_classifier.joblib"
    regressor_path = "regressors/random_forest_regressor.joblib"

    # Convert string dates to date objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Read and filter the flights and weather data
    flights_df = read_relevant_csvs(flights_dir, start_date, end_date)
    weather_df = read_relevant_csvs(weather_dir, start_date, end_date)
    airports_info = pd.read_csv('../data/processed/csv/airports_info/airports_info.csv')

    print(f"####################################{flights_df.columns}#########################")

    ###-------------------------------Preprocessing-------------------------------###
    # Ensure the timestamp columns are in datetime format
    flights_df['rounded_hour'] = pd.to_datetime(flights_df['rounded_hour'], utc=True)
    flights_df['scheduled_time_departure'] = pd.to_datetime(flights_df['scheduled_time_departure'], utc=True)

    flights_df['actual_time_departure'] = flights_df['actual_time_departure'].fillna('')
    flights_df.loc[flights_df['actual_time_departure'] == '', 'actual_time_departure'] = flights_df['scheduled_time_departure']
    
    flights_df['actual_time_departure'] = pd.to_datetime(flights_df['actual_time_departure'], utc=True)
    flights_df['scheduled_time_arrival'] = pd.to_datetime(flights_df['scheduled_time_arrival'], utc=True)
    weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])

    flights_df['flight_time'] = ((flights_df['scheduled_time_arrival'] - flights_df['scheduled_time_departure']).dt.total_seconds() / 3600).astype(float)
    flights_df['departure_delay'] = ((flights_df['scheduled_time_departure'] - flights_df['actual_time_departure']).dt.total_seconds() / 60).astype(float)

    # Extract additional datetime components
    flights_df['year'] = flights_df['scheduled_time_departure'].dt.year
    flights_df['month'] = flights_df['scheduled_time_departure'].dt.month
    flights_df['day'] = flights_df['scheduled_time_departure'].dt.day
    flights_df['hour'] = flights_df['scheduled_time_departure'].dt.hour
    flights_df['dayofweek'] = flights_df['scheduled_time_departure'].dt.dayofweek
    flights_df['quarter'] = flights_df['scheduled_time_departure'].dt.quarter
    
    # Filter based on departure city and destination
    # filtered_flights_df = flights_df[
    #     (flights_df['from_city'] == departure_city.lower()) & 
    #     (flights_df['to_city'] == destination.lower())
    # ]

    
    merged_df = pd.merge(flights_df,
                         airports_info,
                         left_on=['to'],
                         right_on=['airport'],
                         how='inner'
                         )
    print(f"***************************************{len(merged_df)}********************************")

    # Join flights and weather data
    merged_df = pd.merge(
        merged_df, 
        weather_df, 
        left_on=['rounded_hour', 'to'], 
        right_on=['timestamp', 'location'], 
        how='inner'
    )
    print(f"-----------------------------------{len(merged_df)}------------------------------------")

    columns_to_encode = ["flight", "from", "to", "from_city", "to_city"]
    # Load the LabelEncoders from disk using dill
    with open('value_mappings.pkl', 'rb') as f:
        value_mappings = dill.load(f)

    # Encode the new data
    merged_df = encode_new_data(merged_df, columns_to_encode, value_mappings)

    features = [
        "flight_encoded",
        "from_encoded",
        "to_encoded",
        "from_city_encoded",
        "to_city_encoded",
        "flight_time",
        "departure_delay",
        "temperature",
        "humidity",
        "wind_speed",
        "my_flightradar24_rating",
        "arrival_delay_index",
        "departure_delay_index",
        "year",
        "month",
        "day",
        "hour",
        "dayofweek",
        "quarter"
    ]

    # Load the pre-trained model
    classifier = joblib.load(model_path)

    df_model = merged_df[features]
    
    # Dictionary mapping old column names to new column names
    column_mappings = {
        'flight_encoded': 'flight',
        'from_encoded': 'from',
        'to_encoded': 'to',
        'from_city_encoded': 'from_city',
        'to_city_encoded': 'to_city'
    }
    df_model = df_model.rename(columns=column_mappings)
    # Predict delays
    merged_df['status'] = classifier.predict(df_model)

    # Load the pre-trained regressor model
    regressor = joblib.load(regressor_path)

    # Filter rows where status is 1 (delayed)
    delayed_flights_df = merged_df[merged_df['status'] == 1]

    # Predict delay time for delayed flights
    if not delayed_flights_df.empty:
        merged_df.loc[merged_df['status'] == 1, 'predicted_delay_time'] = regressor.predict(delayed_flights_df[features])


    # Replace NaN values with None
    merged_df = merged_df.where(pd.notnull(merged_df), '')

    # Convert the final DataFrame to a list of dictionaries
    flights = merged_df.to_dict(orient='records')

    return flights
