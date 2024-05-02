from datetime import datetime
from typing import Any

from pyspark.sql.functions import col, concat, lit, split, expr, to_date, to_timestamp, date_format, lower, concat_ws, regexp_replace, when, regexp_replace, trim, regexp_extract, hour, mean, minute, lpad
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import DataFrame

from ..common.s3 import write_to_parquet
from ..config.config import app_config
from ..data.joined_flights.joined_flights_schema import JOINED_FLIGHTS_SCHEMA
from ..data.flights.flights_reader import FlightsReader
from ..data.airports_info.airports_info_reader import AirportsReader
from ..data.aircrafts_info.aircrafts_info_reader import AircraftsReader
from ..data.reviews.reviews_reader import ReviewsReader
from ..data.weather.weather_reader import WeatherReader

'''
from ..data.flights.airports_info_schema import N_FIN_PLAN
from reporting_tool.data.flights.flights_schema import C_IDENT_VAL
from reporting_tool.data.reviews.reviews_schema import C_IDENT_NATURE
'''

class FlightsJob:
    def __init__(
        self,
        flights_input_path: str,
        airports_info_input_path: str,
        aicrafts_info_input_path: str,
        reviews_input_path: str,
        weather_input_path: str,
        flights_output_path: str,
    ) -> None:
        self.flights_input_path: str = flights_input_path
        self.airports_info_input_path: str = airports_info_input_path
        self.aicrafts_info_input_path: str = aicrafts_info_input_path
        self.reviews_input_path: str = reviews_input_path
        self.weather_input_path: str = weather_input_path
        self.flights_output_path: str = flights_output_path

    def run(self) -> None:
        flights_df: DataFrame = self._get_data_from_flights(
            self.flights_input_path
        )
        airports_info: DataFrame = self._get_data_from_airports_info(
            self.airports_info_input_path
        )

        aicrafts_info: DataFrame = self._get_data_from_aicrafts_info(
            self.aicrafts_info_input_path
        )
        reviews_df: DataFrame = self._get_data_from_reviews(
            self.reviews_input_path
        )
        weather_df: DataFrame = self._get_data_from_weather(
            self.weather_input_path
        )
        joined_flights: DataFrame = self._create_joined_flights(
            self._process_flights(flights_df),
            self._process_airports_info(airports_info),
            self._process_aircrafts_info(aicrafts_info),
            self._process_reviews(reviews_df),
            self._process_weather(weather_df)
        )

        self._write_joined_flights_to_s3(joined_flights, self.flights_output_path)

    def _get_data_from_flights(self, path: str) -> DataFrame:
        flights_reader: FlightsReader = FlightsReader(path)
        flights = flights_reader.read()
        return flights

    def _get_data_from_airports_info(self, path: str) -> DataFrame:
        airports_info_reader: AirportsReader = AirportsReader(path)
        airports_info = airports_info_reader.read()
        return airports_info

    def _get_data_from_aicrafts_info(self, path: str) -> DataFrame:
        aircrafts_reader: AircraftsReader = AircraftsReader(path)
        aircrafts = aircrafts_reader.read()
        return aircrafts

    def _get_data_from_reviews(self, path: str) -> DataFrame:
        reviews_readar: ReviewsReader = ReviewsReader(path)
        reviews = reviews_readar.read()
        return reviews

    def _get_data_from_weather(self, path: str) -> DataFrame:
        weather_reader: WeatherReader = WeatherReader(path)
        weather = weather_reader.read()
        return weather

    def _create_joined_flights(
        self,
        flights_df: DataFrame,
        airports_info: DataFrame,
        aircrafts_df: DataFrame,
        reviews_df: DataFrame,
        weather_df: DataFrame
    ) -> DataFrame:
        
        flights_df: DataFrame = self._process_flights(flights_df)
        airports_info: DataFrame = self._process_airports_info(airports_info)
        aircrafts_df: DataFrame = self._process_aircrafts_info(aircrafts_df)
        reviews_df: DataFrame = self._process_reviews(reviews_df)
        weather_df: DataFrame = self._process_weather(weather_df)

        joined_flights: DataFrame = (
            (
                flights_df.join(weather_df, ["rounded_hour", "airport"], "left")
                .join(airports_info, ["airport"], "left").drop("time_diff")
                .join(aircrafts_df, ["aircraft"], "left")
            )
            .distinct()
            .select(*JOINED_FLIGHTS_SCHEMA.fieldNames())
        )
        return joined_flights
    
    def _process_flights(self, flights_df: DataFrame) -> DataFrame:
        """
        Transforms flight data by cleaning and structuring. Removes unnecessary columns, normalizes dates and times, 
        extracts key information from strings, and filters based on flight status. Assumes data is loaded from a CSV 
        with a predefined schema.

        Returns:
            flights_df (DataFrame): A Spark DataFrame with processed flights information.
        """
        # Data Preprocessing Steps

        # 1. Remove unnecessary columns
        flights_df = flights_df.drop("temp1", "temp2", "temp3", "temp4")
        
        # 2. Convert date to DateType
        flights_df = flights_df.withColumn("date", to_date("date", "dd MMM yyyy"))

        # 7. Split 'status' into new 'status' and 'actual_time_arrival'
        split_col = split(col("status"), " ")
        flights_df = flights_df.withColumn("actual_time_arrival", expr("substring(status, length(status) - 4, 5)"))
        flights_df = flights_df.withColumn("status", split_col.getItem(0))

        
        # 8. Filter rows to only include statuses 'Departed' or 'Arrived'
        flights_df = flights_df.filter(col("status").rlike("Landed"))

        
        # 3. Convert 'time' to TimestampType assuming it contains AM/PM
        # Concatenate 'date' with 'time' before converting to timestamp for 'expected_time'
        # This ensures the timestamp includes the correct date instead of defaulting to '1970-01-01'
        flights_df = flights_df.withColumn(
            "flight_time", 
            to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("flight_time")), "yyyy-MM-dd HH:mm")
        ).withColumn(
            "scheduled_time_departure", 
            to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("scheduled_time_departure")), "yyyy-MM-dd HH:mm")
        ).withColumn(
            "actual_time_departure", 
            to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("actual_time_departure")), "yyyy-MM-dd HH:mm")
        ).withColumn(
            "scheduled_time_arrival", 
            to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("scheduled_time_arrival")), "yyyy-MM-dd HH:mm")
        ).withColumn(
            "actual_time_arrival", 
            to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("actual_time_arrival")), "yyyy-MM-dd HH:mm")
        )
        
        
        # 4. Extract city from 'from' and 'to' and convert it to lowercase
        flights_df = flights_df.withColumn("from_city", lower(split(col("from"), " \\(")[0])) \
                            .withColumn("to_city", lower(split(col("to"), " \\(")[0]))

        # 5. Extract airport code from 'from' and 'to'
        flights_df = flights_df.withColumn("from", lower(split(col("from"), " \\(")[1].substr(0, 3))) \
                            .withColumn("to", lower(split(col("to"), " \\(")[1].substr(0, 3))) \

        
        # Add a new column 'rounded_hour' that represents the closest hour to the scheduled time arrival
        flights_df = flights_df.withColumn("hour", hour("scheduled_time_arrival")) \
            .withColumn("minute", minute("scheduled_time_arrival")) \
            .withColumn("rounded_hour",
                            when(col("minute") >= 30, expr("hour + 1"))
                            .otherwise(col("hour"))
                        ) \
            .drop("hour", "minute")
        
        # Adjust for the case where adding 1 to the hour results in 24
        flights_df = flights_df.withColumn("rounded_hour",
                        when(col("rounded_hour") == 24, 0)
                        .otherwise(col("rounded_hour"))
                        )
        
        # Convert 'rounded_hour' to a string with two digits
        hour_str = lpad(col("rounded_hour"), 2, '0')
        
        # Concatenate 'date' and 'hour_str' to form a datetime string
        datetime_str = concat_ws(" ", col("date"), hour_str)

        # Append ":00:00" to represent minutes and seconds, forming a full datetime string
        datetime_str = concat_ws(":", datetime_str, lit("00"), lit("00"))

        # Convert the datetime string to a timestamp
        flights_df = flights_df.withColumn("rounded_hour", to_timestamp(datetime_str, "yyyy-MM-dd HH:mm:ss"))

        # 10. Remove duplicates
        flights_df = flights_df.dropDuplicates()

        flights_df = flights_df.withColumn('airport', col('to'))

        # 11. Add status and delay_time
        # Calculate delay in minutes
        flights_df = flights_df.withColumn("delay_time", 
                                    (col("actual_time_arrival").cast("long") - col("scheduled_time_arrival").cast("long")) / 60)
        
        # Define status based on delay_time
        flights_df = flights_df.withColumn("status", when(col("delay_time") > 15, "Delayed").otherwise("On Time"))
        
        # Return the processed DataFrame
        return flights_df
    
    def _process_airports_info(self, airports_info: DataFrame) -> DataFrame:
        """
        Processes airport information data, cleaning and converting specific columns to proper data types.
        N/A values are treated as null, and numeric fields are cast to their respective types.
        
        Returns:
            airports_info (DataFrame): A Spark DataFrame with processed airport information.
        """
        # Drop the 'temp' column as it contains null values due to scraping errors
        airports_info = airports_info.drop("temp")

        # Replace "N/A" string values with null across the DataFrame
        airports_info = airports_info.na.replace("N/A", None)

        # Clean numeric fields and cast to correct types
        airports_info = airports_info.withColumn("my_flightradar24_rating", 
                                    regexp_replace(col("my_flightradar24_rating"), "[^0-9]", "").cast(IntegerType())) \
                        .withColumn("arrival_delay_index", col("arrival_delay_index").cast(FloatType())) \
                        .withColumn("departure_delay_index", col("departure_delay_index").cast(FloatType()))
        
        # Extract the utc time part and convert it to a Spark timestamp format
        airports_info = airports_info.withColumn("utc", to_timestamp(regexp_extract(col("utc"), "(\\d{2}:\\d{2})", 0), "HH:mm"))

        # Convert local time to a Spark timestamp format
        airports_info = airports_info.withColumn("local", to_timestamp(concat(lit("1970-01-01 "), col("local")), "yyyy-MM-dd hh:mm a"))

        # Calculate time difference utc-local
        airports_info = airports_info.withColumn("time_diff", col('utc')-col('local')).drop('utc', 'local')

        # Remove duplicates
        airports_info = airports_info.dropDuplicates()

        # Return the processed DataFrame
        return airports_info
    
    def _process_aircrafts_info(self, aircraft_info_df: DataFrame) -> DataFrame:
        """
        Processes airaircraftport information data, cleaning and converting specific columns to proper data types.
        N/A values are treated as null, and numeric fields are cast to their respective types.
        
        Returns:
            aircraft_info_df (DataFrame): A Spark DataFrame with processed aircraft information.
        """
        aircraft_info_df = aircraft_info_df.drop("photo")

        age_pattern = r"\((\d+) years\)"

        # Add a new column "age" that extracts the age part and converts it to an integer
        aircraft_info_df = aircraft_info_df.withColumn("age", regexp_extract(col("first_flight"), age_pattern, 1).cast("integer")).drop('first_flight')

        
        # Convert the 'aircraft' column to lowercase
        aircraft_info_df = aircraft_info_df.withColumn("aircraft", lower(aircraft_info_df["aircraft"]))

        # Check the schema of columns
        aircraft_info_df.printSchema()

        aircraft_info_df.show(truncate=False)

        return aircraft_info_df
    
    def _process_reviews(self, reviews_df: DataFrame) -> DataFrame:
        """
        Cleans review data from a CSV file. This function lowercases comments, removes special characters,
        filters out empty comments, and removes duplicate rows. It initializes a Spark session, reads the data using
        a predefined schema, and applies text preprocessing to the 'comment' field. The cleaned DataFrame is then returned.

        Returns:
            DataFrame: The processed reviews DataFrame.
        """
        # Data cleaning and preprocessing
        reviews_df = reviews_df.withColumn("comment", lower(col("comment")))
        reviews_df = reviews_df.withColumn("comment", regexp_replace(col("comment"), "[^a-zA-Z0-9 ]", ""))
        reviews_df = reviews_df.filter(trim(col("comment")) != "")
        reviews_df = reviews_df.dropDuplicates()
        
        return reviews_df
    
    def _process_weather(self, weather_df: DataFrame) -> DataFrame:
        """
        Processes weather data by cleaning and transforming specific columns.
        This includes removing non-numeric characters, handling special cases in visibility,
        and converting date_time strings to timestamp format.

        Returns:
            weather_df (DataFrame): A Spark DataFrame with processed weather information.
        """
        # Drop null values
        weather_df = weather_df.dropna(how="any")

        # Clean numeric fields and cast to correct types
        weather_df = weather_df.withColumn("temperature", 
                                    regexp_replace(col("temperature"), "[^0-9-]", "").cast(IntegerType())) \
                                .withColumn("dew_point", 
                                    regexp_replace(col("dew_point"), "[^0-9-]", "").cast(IntegerType())) \
                                .withColumn("humidity", 
                                    regexp_replace(col("humidity"), "[^0-9]", "").cast(IntegerType())) \
                                .withColumn("wind_speed", 
                                    regexp_replace(col("wind_speed"), "[^0-9]", "").cast(IntegerType())) \
                                .withColumn("wind_gust", 
                                    regexp_replace(col("wind_gust"), "[^0-9]", "").cast(IntegerType())) \
                                .withColumn("pressure", 
                                    regexp_replace(col("pressure"), "[^0-9.]", "").cast(FloatType())) \
                                .withColumn("precip", 
                                    regexp_replace(col("precip"), "[^0-9.]", "").cast(FloatType()))

        

        weather_df = weather_df.withColumn(
            "date_time", 
            to_timestamp(concat_ws(" ", split(col("date"), " ")[0], col("time")), "yyyy-MM-dd hh:mm a")
        ).drop("date", "time")

        # Remove duplicates
        weather_df = weather_df.dropDuplicates()

        # Add a new column 'rounded_hour' that represents the closest hour to date_time
        weather_df = weather_df.withColumn("date", to_date("date_time")) \
            .withColumn("hour", hour("date_time")) \
            .withColumn("minute", minute("date_time")) \
            .withColumn("rounded_hour",
                            when(col("minute") >= 30, expr("hour + 1"))
                            .otherwise(col("hour"))
                        ) \
            .drop("hour", "minute")
        
        # Adjust for the case where adding 1 to the hour results in 24
        weather_df = weather_df.withColumn("rounded_hour",
                        when(col("rounded_hour") == 24, 0)
                        .otherwise(col("rounded_hour"))
                        )

        # Convert 'hour_column' to a string with two digits
        rounded_hour = lpad(col("rounded_hour"), 2, '0')
        
        # Concatenate 'date_column' and 'hour_str' to form a datetime string
        datetime_str = concat_ws(" ", col("date"), rounded_hour)

        # Append ":00:00" to represent minutes and seconds, forming a full datetime string
        datetime_str = concat_ws(":", datetime_str, lit("00"), lit("00"))

        # Convert the datetime string to a timestamp
        weather_df = weather_df.withColumn("rounded_hour", to_timestamp(datetime_str, "yyyy-MM-dd HH:mm:ss")).drop('date')
        
        # Drop duplicate rounded_hour
        weather_df = weather_df.dropDuplicates(['airport', 'rounded_hour'])
        '''
        # Join the airports_info data with the aggregated weather data
        weather_df = weather_df.join(info_df, "airport", "left")

        # Converting weather date_time to local time using difference from joining info_df
        weather_df = weather_df.withColumn("date_time", expr("date_time - time_diff")).drop("time_diff")
        '''
        '''
        # Aggregating wind direction, wind speed, temperature, dew point, pressure and visibility
        weather_df = weather_df.groupBy("airport", "rounded_hour").agg(
            mean("wind_direction").alias("wind_direction"),
            mean("wind_speed").alias("wind_speed"),
            mean("temperature").alias("temperature"),
            mean("dew_point").alias("dew_point"),
            mean("pressure").alias("pressure"),
            mean("visibility").alias("visibility"),
        )
        '''
        # Return the processed DataFrame
        return weather_df

    def _write_joined_flights_to_s3(self, df: DataFrame, output_path: str) -> None:
        write_to_parquet(df, output_path)


def run_job(**kwargs: Any) -> None:
    print(f"Running Job with arguments[{kwargs}]")

    bucket_name_datalake = app_config.bucket_name_datalake
    bucket_name_results = app_config.bucket_name_results

    file_path_flights = app_config.file_path_flights
    file_path_airports_info = app_config.file_path_airports_info
    file_path_reviews = app_config.file_path_reviews
    file_path_aircrafts = app_config.file_path_aircrafts
    file_path_weather = app_config.file_path_weater

    output_file_path_flights = app_config.output_file_path_flights

    date: str = datetime.now().strftime("%Y%m%d")
    flights_path: str = (
        f"s3a://{bucket_name_datalake}/{file_path_flights}"
    )
    airports_info_path: str = (
        f"s3a://{bucket_name_datalake}/{file_path_airports_info}"
    )
    reviews_path: str = f"s3a://{bucket_name_datalake}/{file_path_reviews}"
    aircrafts_path: str = (
        f"s3a://{bucket_name_datalake}/{file_path_aircrafts}"
    )
    weather_path: str = f"s3a://{bucket_name_datalake}/{file_path_weather}"
    flights_output_path: str = (
        f"s3://{bucket_name_results}/{output_file_path_flights}/eventdate={date}"
    )

    job: FlightsJob = FlightsJob(
        flights_path,
        airports_info_path,
        reviews_path,
        aircrafts_path,
        weather_path,
        flights_output_path,
    )
    job.run()
