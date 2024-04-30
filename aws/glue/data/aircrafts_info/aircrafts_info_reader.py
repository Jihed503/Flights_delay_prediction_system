from pyspark.sql import DataFrame
from ...common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from ...context.context import logger
from .aircrafts_info_schema import (
    AIRCRAFTS_INFO_SCHEMA,

    PREFIX_PATH_AIRCRAFTS_INFO,
)

N_APPLIC_INFQ_VALUE = 38


class AircraftsReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        aircrafts_info_df: DataFrame = read_from_parquet(
            self.path
        ).select(*AIRCRAFTS_INFO_SCHEMA.fieldNames())

        return create_df_with_schema(
            aircrafts_info_df,
            AIRCRAFTS_INFO_SCHEMA,
        )
