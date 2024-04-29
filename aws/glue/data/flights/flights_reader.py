from pyspark.sql import DataFrame
from ...common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from ...context.context import logger
from .flights_schema import (
    FLIGHTS_SCHEMA,

    PREFIX_PATH_FLIGHTS,
)

N_APPLIC_INFQ_VALUE = 38


class FlightsReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        flights_df: DataFrame = read_from_parquet(
            self.path
        ).select(*FLIGHTS_SCHEMA.fieldNames())

        return create_df_with_schema(
            flights_df,
            FLIGHTS_SCHEMA,
        )
