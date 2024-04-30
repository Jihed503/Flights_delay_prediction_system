from typing import Tuple

from pyspark.sql.types import StringType, StructField, StructType



# aircrafts_info fields
MSN: str = "msn"
TYPE: str = "type"
REGISTRATION: str = "registration"
AIRLINE: str = "airline"
FIRST_FLIGHT: str = "first_flight"
PHOTO: str = "photo"

PREFIX_PATH_AIRCRAFTS_INFO: str = (
    ""
)

'''
PREFIX_PATH_FLIGHTS: str = (
    "CORPORATE/COMPTOIR_CBSFINANCEMENT/MOM/V2/FIAG_FIPL_ATTACHED/eventdate="
)
'''

AIRCRAFTS_INFO_SCHEMA: StructType = StructType(
    [
        StructField(MSN, StringType()),
        StructField(TYPE, StringType()),
        StructField(REGISTRATION, StringType()),
        StructField(AIRLINE, StringType()),
        StructField(FIRST_FLIGHT, StringType()),
        StructField(PHOTO, StringType())
    ]
)


def build_aircrafts_info(
        msn: str,
        type: str,
        registration: str,
        airline: str,
        first_flight: str,
        photo: str
) -> Tuple:
    return (
        msn,
        type,
        registration,
        airline,
        first_flight,
        first_flight
    )
