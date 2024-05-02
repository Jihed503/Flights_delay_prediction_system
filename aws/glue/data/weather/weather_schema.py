from typing import Tuple

from pyspark.sql.types import StringType, StructField, StructType



# flights fields
AIRCRAFT: str = "aircraft"
TEMP1: str = "temp1"
TEMP2: str = "temp2"
DATETIME: str = "date"
FROM: str = "from"
TO: str = "to"
FLIGHT: str = "flight"
FLIGHT_TIME: str = "flight_time"
SCHEDULED_TIME_DEPARTURE: str = "scheduled_time_departure"
ACTUAL_TIME_DEPARTURE: str = "actual_time_departure"
SCHEDULED_TIME_ARRIVAL: str = "scheduled_time_arrival"
TEMP3: str = "temp3"
STATUS: str = "status"
TEMP4: str = "temp4"

PREFIX_PATH_FLIGHTS: str = (
    ""
)

'''
PREFIX_PATH_FLIGHTS: str = (
    "CORPORATE/COMPTOIR_CBSFINANCEMENT/MOM/V2/FIAG_FIPL_ATTACHED/eventdate="
)
'''

WEATHER_SCHEMA: StructType = StructType(
    [
        StructField(AIRCRAFT, StringType()),
        StructField(TEMP1, StringType()),
        StructField(TEMP2, StringType()),
        StructField(DATETIME, StringType()),
        StructField(FROM, StringType()),
        StructField(TO, StringType()),
        StructField(FLIGHT, StringType()),
        StructField(FLIGHT_TIME, StringType()),
        StructField(SCHEDULED_TIME_DEPARTURE, StringType()),
        StructField(ACTUAL_TIME_DEPARTURE, StringType()),
        StructField(SCHEDULED_TIME_ARRIVAL, StringType()),
        StructField(TEMP3, StringType()),
        StructField(STATUS, StringType()),
        StructField(TEMP4, StringType())
    ]
)


def build_flights(
        aircraft: str,
        temp1: str,
        temp2: str,
        date: str,
        from_: str,
        to: str,
        flight: str,
        flight_time: str,
        scheduled_time_departure: str,
        actual_time_departure: str,
        scheduled_time_arrival: str,
        temp3: str,
        status: str,
        temp4
) -> Tuple:
    return (
        aircraft,
        temp1,
        temp2,
        date,
        from_,
        to,
        flight,
        flight_time,
        scheduled_time_departure,
        actual_time_departure,
        scheduled_time_arrival,
        temp3,
        status,
        temp4
    )
