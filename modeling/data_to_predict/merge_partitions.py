from pyspark.sql import SparkSession
import os
import shutil

# Define the base directory for partitioned data
base_dir = "data/weather/spark_output/" # flights or weather

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MergePartitionedCSVFiles") \
    .getOrCreate()

# Get list of partition directories
partition_dirs = [d for d in os.listdir(base_dir) if d.startswith("date=")]

for partition_dir in partition_dirs:
    # Construct the full path for each partition directory
    partition_path = os.path.join(base_dir, partition_dir)
    
    # Read all CSV files from the partition directory
    partition_df = spark.read.option("header", "true").csv(partition_path)
    
    # Write the merged DataFrame to a single CSV file in a temporary directory
    temp_output_path = os.path.join(base_dir, f"temp_{partition_dir}")
    partition_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_output_path)
    
    # Find the part file written by Spark
    part_file = [f for f in os.listdir(temp_output_path) if f.startswith("part-") and f.endswith(".csv")][0]
    
    # Move the part file to the main directory with the appropriate name
    final_output_path = os.path.join(base_dir, f"{partition_dir}.csv")
    shutil.move(os.path.join(temp_output_path, part_file), final_output_path)
    
    # Remove the temporary output directory created by Spark
    shutil.rmtree(temp_output_path)

    # Optionally, remove the old partition directory
    shutil.rmtree(partition_path)

# Stop the Spark session
spark.stop()
