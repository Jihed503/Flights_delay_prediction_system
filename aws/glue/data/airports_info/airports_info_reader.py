from pyspark.sql import DataFrame
from ...common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from ...context.context import logger
from .airports_info_schema import (
    AIRPORTS_INFO_SCHEMA,

    PREFIX_PATH_FLIGHTS,
)

N_APPLIC_INFQ_VALUE = 38


class AirportsReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        airports_info_df: DataFrame = read_from_parquet(
            self.path
        ).select(*AIRPORTS_INFO_SCHEMA.fieldNames())

        return create_df_with_schema(
            airports_info_df,
            AIRPORTS_INFO_SCHEMA,
        )
