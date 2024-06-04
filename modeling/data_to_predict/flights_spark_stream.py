from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split, expr, to_date, to_timestamp, date_format, lower, concat_ws, regexp_replace, when, regexp_replace, trim, regexp_extract, hour, mean, minute, lpad, substring, substring
import os

# Define the output directory
output_dir = "data/flights/spark_output/"
checkpoint_dir = "data/flights/"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FlightDataStreaming") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "scheduled_flights_data_topic") \
    .load()

# Convert the binary values to string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Define the schema as a list of column names
columns = [
    "flight", "temp1", "temp2", "date", "from", "to", "aircraft", "flight_time", "scheduled_time_departure",
    "actual_time_departure", "scheduled_time_arrival", "temp3", "status", "temp4"
]

# Split the CSV string into columns
flights_df = kafka_df.withColumn("value", split(col("value"), ",")).select(
    [col("value").getItem(i).alias(columns[i]) for i in range(len(columns))]
)

# Select and clean up necessary columns
flights_df = flights_df.select(
    "flight", "date", "from", "to", "aircraft", "flight_time", "scheduled_time_departure",
    "actual_time_departure", "scheduled_time_arrival", "status"
    )

# 2. Convert date to DateType
flights_df = flights_df.withColumn("date", to_date("date", "dd MMM yyyy"))



# 3. Convert 'time' to TimestampType assuming it contains AM/PM
# Concatenate 'date' with 'time' before converting to timestamp for 'expected_time'
# This ensures the timestamp includes the correct date instead of defaulting to '1970-01-01'

flights_df = flights_df.withColumn(
    "scheduled_time_departure", 
    to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("scheduled_time_departure")), "yyyy-MM-dd HH:mm")
).withColumn(
    "actual_time_departure", 
    to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("actual_time_departure")), "yyyy-MM-dd HH:mm")
).withColumn(
    "scheduled_time_arrival", 
    to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("scheduled_time_arrival")), "yyyy-MM-dd HH:mm")
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

# Remove the first three characters and the last character from the 'flight' column
flights_df = flights_df.withColumn("flight", regexp_replace(col("flight"), r'\\?"', ''))

# Remove rows with null values
#flights_df = flights_df.dropna(subset=['from', 'to', 'scheduled_time_departure', 'actual_time_departure', 'scheduled_time_arrival'])


# Ensure directories exist
os.makedirs(output_dir, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)

# Write the data to CSV files in the specified output directory
query = flights_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .option("header", "true") \
    .option("sep", ",") \
    .partitionBy("date") \
    .start()

# Await termination of the streaming query
query.awaitTermination()
