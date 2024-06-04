from pyspark.sql import SparkSession
import os
import shutil
import argparse

# Function to delete existing CSV files in the directory
def delete_existing_csv_files(directory):
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            os.remove(os.path.join(directory, file))

def main(data_type):
    # Define the base directory for partitioned data
    if data_type == "flights":
        base_dir = "data/flights/spark_output/"
    elif data_type == "weather":
        base_dir = "data/weather/spark_output/"
    else:
        raise ValueError("Invalid data type. Use 'flights' or 'weather'.")

    # Delete existing CSV files
    delete_existing_csv_files(base_dir)

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge partitioned CSV files and delete old ones.")
    parser.add_argument("data_type", help="Specify the type of data: 'flights' or 'weather'.")
    args = parser.parse_args()
    main(args.data_type)
