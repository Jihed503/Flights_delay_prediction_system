from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, to_timestamp, lit, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
 
# Define the output directory
output_dir = "data/weather/spark_output/"
checkpoint_dir = "data/weather/"
 
# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("WeatherDataCleaning") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
 
# Lire les données de Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rt_weather_data_topic") \
    .load()
 
# Convertir les valeurs binaires en chaînes de caractères
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
 
# Définir le schéma pour les données météorologiques
weather_schema = StructType([
    StructField("date", StringType(), True),
    StructField("hour", StringType(), True),
    StructField("details", StringType(), True)
])
 
# Appliquer le schéma et séparer la chaîne CSV en colonnes
weather_df = kafka_df.select(from_json(col("value"), weather_schema).alias("parsed_value")).select("parsed_value.*")
 
# Définir le schéma des détails
details_schema = StructType([
    StructField("feels_like", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("cloud_cover", StringType(), True),
    StructField("rain_amount", StringType(), True)
])
 
# Extraire les informations du champ "details" (JSON)
weather_df = weather_df.withColumn("details", from_json(col("details"), details_schema))
 
# Sélectionner et renommer les colonnes nécessaires
weather_df = weather_df.select(
    to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss").alias("date"),
    regexp_replace(col("details.feels_like"), "[^0-9]", "").cast("int").alias("temperature"),
    col("details.wind_direction").alias("wind_direction"),
    regexp_replace(col("details.wind_speed"), "[^0-9]", "").cast("int").alias("wind_speed"),
    regexp_replace(col("details.humidity"), "[^0-9]", "").cast("int").alias("humidity"),
    regexp_replace(col("details.cloud_cover"), "[^0-9]", "").cast("int").alias("cloud_cover"),
    regexp_replace(col("details.rain_amount"), "[^0-9.]", "").cast("float").alias("precip")
)
 
# Ajouter la colonne fixe avec la valeur "doh"
weather_df = weather_df.withColumn("location", lit("doh"))
 
# Filtrer les lignes où des colonnes nécessaires sont nulles ou vides
weather_df = weather_df.filter(
    (col("date").isNotNull()) &
    (col("temperature").isNotNull()) &
    (col("wind_direction").isNotNull()) &
    (col("wind_speed").isNotNull()) &
    (col("humidity").isNotNull()) &
    (col("cloud_cover").isNotNull()) &
    (col("precip").isNotNull())
)
 
# Formater la colonne de date dans le format souhaité
weather_df = weather_df.withColumn("date", date_format(col("date"), "yyyy-MM-dd HH:mm:ss"))
 
# Configurer le répertoire de sortie
os.makedirs(output_dir, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)
 
# Écrire les données nettoyées dans un fichier CSV
query = weather_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .option("header", "true") \
    .option("sep", ",") \
    .start()
 
# Attendre la fin de la diffusion en continu
query.awaitTermination()