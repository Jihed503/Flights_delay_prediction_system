from typing import Tuple

from pyspark.sql.types import StringType, StructField, StructType



# reviews fields
COMMENT: str = "comment"
AIRPORT: str = "airport"

PREFIX_PATH_REVIEWS: str = (
    ""
)

'''
PREFIX_PATH_FLIGHTS: str = (
    "CORPORATE/COMPTOIR_CBSFINANCEMENT/MOM/V2/FIAG_FIPL_ATTACHED/eventdate="
)
'''

REVIEWS_SCHEMA: StructType = StructType(
    [
        StructField(COMMENT, StringType()),
        StructField(AIRPORT, StringType())
    ]
)


def build_reviews(
        comment: str,
        airport: str
) -> Tuple:
    return (
        comment,
        airport
    )
