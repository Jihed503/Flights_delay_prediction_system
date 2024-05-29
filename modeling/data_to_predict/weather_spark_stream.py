from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, to_timestamp, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType
import os

# Define the output directory
output_dir = "data/weather/spark_output/"
checkpoint_dir = "data/weather/"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WeatherDataCleaning") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rt_weather_data_topic") \
    .load()

# Convert binary values to strings
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Define schema for weather data
weather_schema = StructType([
    StructField("date", StringType(), True),
    StructField("hour", StringType(), True),
    StructField("details", StringType(), True)
])

# Apply schema and separate the CSV string into columns
weather_df = kafka_df.select(from_json(col("value"), weather_schema).alias("parsed_value")).select("parsed_value.*")

# Define schema for details
details_schema = StructType([
    StructField("feels_like", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("cloud_cover", StringType(), True),
    StructField("rain_amount", StringType(), True)
])

# Extract information from the "details" field (JSON)
weather_df = weather_df.withColumn("details", from_json(col("details"), details_schema))

# Select and rename necessary columns
weather_df = weather_df.select(
    to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
    regexp_replace(col("details.feels_like"), "[^0-9]", "").cast("int").alias("temperature"),
    col("details.wind_direction").alias("wind_direction"),
    regexp_replace(col("details.wind_speed"), "[^0-9]", "").cast("int").alias("wind_speed"),
    regexp_replace(col("details.humidity"), "[^0-9]", "").cast("int").alias("humidity"),
    regexp_replace(col("details.cloud_cover"), "[^0-9]", "").cast("int").alias("cloud_cover"),
    regexp_replace(col("details.rain_amount"), "[^0-9.]", "").cast("float").alias("precip")
)

# Add a fixed column with the value "doh"
weather_df = weather_df.withColumn("location", lit("doh"))

# Filter rows where necessary columns are null or empty
weather_df = weather_df.filter(
    (col("timestamp").isNotNull()) &
    (col("temperature").isNotNull()) &
    (col("wind_direction").isNotNull()) &
    (col("wind_speed").isNotNull()) &
    (col("humidity").isNotNull()) &
    (col("cloud_cover").isNotNull()) &
    (col("precip").isNotNull())
)

# Extract date part from timestamp for partitioning
weather_df = weather_df.withColumn("date", to_date(col("timestamp")))

# Ensure output directories exist
os.makedirs(output_dir, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)

# Write the cleaned data to a CSV file partitioned by date
query = weather_df \
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
