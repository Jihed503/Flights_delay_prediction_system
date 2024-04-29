import datetime
import re
from datetime import datetime as dt

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from .s3 import list_objects_names
from ..context.context import spark


def read_from_parquet(parquet_file_path: str) -> DataFrame:
    return spark.read.parquet(parquet_file_path)


def get_date_from_string(text: str) -> datetime.date:
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    assert match is not None
    return dt.strptime(match.group(), "%Y-%m-%d").date()

'''
def get_folder_with_max_evendate(path: str, prefix: str) -> str:
    event_date_folder_list: list[str] = list_objects_names(path, prefix)

    if len(event_date_folder_list) == 1:
        folder_with_max_eventdate: str = event_date_folder_list[0]
    else:
        folder_with_max_eventdate = filter_max_eventdate(event_date_folder_list)

    return folder_with_max_eventdate


def filter_max_eventdate(eventdate_folder_list: list) -> str:
    date_dict: dict = {}

    for foldername in eventdate_folder_list:

        eventdate: datetime.date = get_date_from_string(foldername)
        date_dict[foldername] = eventdate

    eventdate_list = [value for key, value in date_dict.items()]
    newer_eventdate = max(eventdate_list)
    return list(date_dict.keys())[list(date_dict.values()).index(newer_eventdate)]
'''

def create_df_with_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Create a dataframe and enforce the given schema."""
    return spark.createDataFrame(
        schema=schema, data=spark.sparkContext.emptyRDD()
    ).union(df)

def read_from_csv_with_header(csv_file_path: str) -> DataFrame:
    """Read a csv file and ignore his header."""
    return spark.read.option("header", True).option("delimiter",";").csv(csv_file_path)
