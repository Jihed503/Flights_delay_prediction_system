from typing import Tuple

from pyspark.sql.types import StringType, StructField, StructType



# airports_info fields
MY_FLIGHTRADAR24_RATING: str = "my_flightradar24_rating"
TEMP: str = "temp"
ARRIVAL_DELAY_INDEX: str = "arrival_delay_index"
DEPARTURE_DELAY_INDEX: str = "departure_delay_index"
UTC: str = "utc"
LOCAL: str = "local"
AIRPORT: str = "airport"

PREFIX_PATH_AIRPORTS_INFO: str = (
    ""
)

'''
PREFIX_PATH_AIRPORTS_INFO: str = (
    "CORPORATE/COMPTOIR_CBSFINANCEMENT/MOM/V2/FIAG_FIPL_ATTACHED/eventdate="
)
'''

AIRPORTS_INFO_SCHEMA: StructType = StructType(
    [
        StructField(MY_FLIGHTRADAR24_RATING, StringType()),
        StructField(TEMP, StringType()),
        StructField(ARRIVAL_DELAY_INDEX, StringType()),
        StructField(DEPARTURE_DELAY_INDEX, StringType()),
        StructField(UTC, StringType()),
        StructField(LOCAL, StringType()),
        StructField(AIRPORT, StringType())
    ]
)


def build_airports_info(
        my_flightradar24_rating: str,
        temp: str,
        arrival_delay_index: str,
        departure_delay_index: str,
        utc: str,
        local: str,
        airport: str
) -> Tuple:
    return (
        my_flightradar24_rating,
        temp,
        arrival_delay_index,
        departure_delay_index,
        utc,
        local,
        airport
    )
