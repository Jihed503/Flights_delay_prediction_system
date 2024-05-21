from enum import Enum
import boto3
from pyspark.sql import DataFrame
import os
from glue.config.config import ACCESS_KEY_ID, SECRET_ACCESS_KEY, app_config

class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"

def write_to_parquet(df: DataFrame, output_file_path: str) -> str:
    """
    Converts a Spark DataFrame to a Pandas DataFrame, writes it to a local Parquet file, 
    and then uploads the file to an AWS S3 bucket. Finally, the local file is deleted.
    
    Parameters:
    - df (DataFrame): The Spark DataFrame to be written.
    - output_file_path (str): The S3 path where the Parquet file will be uploaded.

    Returns:
    - str: The S3 path to which the file was uploaded.
    
    Notes:
    - This function assumes that the AWS credentials and S3 bucket configuration are correct and accessible.
    - The DataFrame is written in 'overwrite' mode as per the Enum setting.
    """
    # Convert DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Write Pandas DataFrame to Parquet file locally
    local_parquet_file = "temp.parquet"
    pandas_df.to_parquet(local_parquet_file)

    # Upload Parquet file to S3
    s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
    s3.upload_file(local_parquet_file, app_config.get('bucket_name_results'), output_file_path)

    # Remove temporary local Parquet file
    os.remove(local_parquet_file)

    return output_file_path
