from pyspark.sql import DataFrame
from ...common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from ...context.context import logger
from .reviews_schema import (
    REVIEWS_SCHEMA,

    PREFIX_PATH_REVIEWSS,
)

N_APPLIC_INFQ_VALUE = 38


class REVIEWSsReader:
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        logger.info("start reading table")
        reviews_df: DataFrame = read_from_parquet(
            self.path
        ).select(*REVIEWS_SCHEMA.fieldNames())

        return create_df_with_schema(
            reviews_df,
            REVIEWS_SCHEMA,
        )
