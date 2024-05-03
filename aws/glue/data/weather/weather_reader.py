from pyspark.sql import DataFrame
from glue.common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from glue.context.context import logger
from .weather_schema import (
    WEATHER_SCHEMA,

    PREFIX_PATH_FLIGHTS,
)

N_APPLIC_INFQ_VALUE = 38


class WeatherReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        weather_df: DataFrame = read_from_parquet(
            self.path
        ).select(*WEATHER_SCHEMA.fieldNames())

        return create_df_with_schema(
            weather_df,
            WEATHER_SCHEMA,
        )
