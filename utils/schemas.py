"""Schemas explícitos da Bronze, espelhando os CSVs de origem.

`children` e `reservation_status_date` ficam como string na Bronze para preservar
os valores da fonte (`children` tem o literal "NA"); a conversão é feita na Silver.
"""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def _fields(columns: list[tuple[str, type]]) -> StructType:
    return StructType([StructField(name, dtype(), nullable=True) for name, dtype in columns])


BOOKINGS_SCHEMA = _fields(
    [
        ("hotel", StringType),
        ("is_canceled", IntegerType),
        ("lead_time", IntegerType),
        ("arrival_date_year", IntegerType),
        ("arrival_date_month", StringType),
        ("arrival_date_week_number", IntegerType),
        ("arrival_date_day_of_month", IntegerType),
        ("stays_in_weekend_nights", IntegerType),
        ("stays_in_week_nights", IntegerType),
        ("adults", IntegerType),
        ("children", StringType),
        ("babies", IntegerType),
        ("meal", StringType),
        ("country", StringType),
        ("market_segment", StringType),
        ("distribution_channel", StringType),
        ("is_repeated_guest", IntegerType),
        ("previous_cancellations", IntegerType),
        ("previous_bookings_not_canceled", IntegerType),
        ("reserved_room_type", StringType),
        ("assigned_room_type", StringType),
        ("booking_changes", IntegerType),
        ("deposit_type", StringType),
        ("agent", IntegerType),
        ("company", IntegerType),
        ("days_in_waiting_list", IntegerType),
        ("customer_type", StringType),
        ("adr", DoubleType),
        ("required_car_parking_spaces", IntegerType),
        ("total_of_special_requests", IntegerType),
        ("reservation_status", StringType),
        ("reservation_status_date", StringType),
    ]
)

COUNTRIES_SCHEMA = _fields(
    [
        ("country_code", StringType),
        ("country_name", StringType),
        ("continent", StringType),
    ]
)

HOTELS_SCHEMA = _fields(
    [
        ("hotel_name", StringType),
        ("city", StringType),
        ("country_code", StringType),
        ("star_rating", IntegerType),
        ("opened_year", IntegerType),
    ]
)
