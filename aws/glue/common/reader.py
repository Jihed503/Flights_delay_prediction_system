import re
from datetime import datetime as dt

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from glue.context.context import spark

def create_df_with_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Creates a new DataFrame by enforcing a specified schema on an existing DataFrame.

    Parameters:
    - df (DataFrame): The original DataFrame to which the schema will be applied.
    - schema (StructType): The schema to be enforced on the DataFrame.

    Returns:
    - DataFrame: A new DataFrame with the specified schema applied.
    """
    return spark.createDataFrame(
        data=spark.sparkContext.emptyRDD(), schema=schema
    ).union(df)

def read_from_csv_with_header(csv_file_path: str) -> DataFrame:
    """
    Reads a CSV file into a DataFrame, including the header as the column names.

    Parameters:
    - csv_file_path (str): The path to the CSV file.

    Returns:
    - DataFrame: The DataFrame created from the CSV file.
    """
    return spark.read.option("header", True).option("delimiter", ",").csv(csv_file_path)
