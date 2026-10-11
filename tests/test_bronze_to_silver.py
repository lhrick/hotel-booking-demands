import datetime as dt

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scripts.bronze_to_silver import (
    RowCountMismatchError,
    add_arrival_date,
    add_derived_columns,
    apply_drop_rules,
    cast_types,
    check_row_count,
    count_warn_rules,
    enrich_with_countries,
    enrich_with_hotels,
    fill_nulls,
    normalize_country_codes,
)
from utils.quality_rules import SILVER_DROP_RULES, SILVER_WARN_RULES

GUESTS_SCHEMA = "adults int, children int, babies int, adr double"


def test_arrival_date_maps_month_name_and_pads(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(2015, "July", 1), (2016, "December", 31), (2017, "February", 30), (2017, "Smarch", 1)],
        "arrival_date_year int, arrival_date_month string, arrival_date_day_of_month int",
    )
    rows = add_arrival_date(df).orderBy("arrival_date_year", "arrival_date_month").collect()

    assert [(r.arrival_date_month_num, r.arrival_date) for r in rows] == [
        (7, dt.date(2015, 7, 1)),
        (12, dt.date(2016, 12, 31)),
        (2, None),  # impossible date -> null, counted by the warn rule
        (None, None),  # unknown month name
    ]


def test_cast_types_children_na_becomes_null_and_parses_status_date(spark: SparkSession) -> None:
    columns = [
        "is_canceled", "lead_time", "arrival_date_year", "arrival_date_week_number",
        "arrival_date_day_of_month", "stays_in_weekend_nights", "stays_in_week_nights",
        "adults", "babies", "is_repeated_guest", "previous_cancellations",
        "previous_bookings_not_canceled", "booking_changes", "days_in_waiting_list",
        "required_car_parking_spaces", "total_of_special_requests",
    ]
    schema = ", ".join(f"{c} int" for c in columns) + ", children string, adr double, reservation_status_date string"
    df = spark.createDataFrame([(0,) * len(columns) + ("NA", 10.0, "2015-07-01")], schema)

    row = cast_types(df).first()

    assert row.children is None
    assert row.reservation_status_date == dt.date(2015, 7, 1)
    assert dict(cast_types(df).dtypes)["children"] == "int"


def test_fill_nulls_and_country_normalization(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, None), (1, "CN"), (2, "PRT")], "children int, country string")

    rows = fill_nulls(normalize_country_codes(df)).orderBy("children").collect()

    assert [(r.children, r.country) for r in rows] == [(0, "UNK"), (1, "CHN"), (2, "PRT")]


@pytest.mark.parametrize(
    ("row", "removed_by"),
    [
        ((0, 0, 0, 50.0), "has_guests"),
        ((2, 0, 0, -6.38), "non_negative_adr"),
        ((1, 0, 0, 0.0), None),  # adr = 0 is valid
        ((0, 0, 1, 10.0), None),  # a baby alone still counts as a guest
    ],
)
def test_drop_rules(spark: SparkSession, row: tuple, removed_by: str | None) -> None:
    df = spark.createDataFrame([row], GUESTS_SCHEMA)

    result, removed = apply_drop_rules(df, SILVER_DROP_RULES)

    expected = {name: int(name == removed_by) for name in SILVER_DROP_RULES}
    assert removed == expected
    assert result.count() == (0 if removed_by else 1)


def _bookings(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return spark.createDataFrame(
        rows,
        "is_canceled int, reservation_status string, adr double, stays_in_weekend_nights int, "
        "stays_in_week_nights int, adults int, children int, babies int",
    )


def test_derived_columns(spark: SparkSession) -> None:
    df = _bookings(
        spark,
        [
            (0, "Check-Out", 100.0, 2, 6, 2, 1, 0),
            (1, "Canceled", 100.0, 1, 2, 2, 0, 0),
            (1, "No-Show", 80.0, 0, 3, 1, 0, 1),
        ],
    )

    rows = add_derived_columns(df).orderBy("reservation_status").collect()
    by_status = {r.reservation_status: r for r in rows}

    checked_out = by_status["Check-Out"]
    assert (checked_out.total_nights, checked_out.total_guests) == (8, 3)
    assert (checked_out.is_family, checked_out.is_long_stay) == (1, 1)
    assert checked_out.revenue == 800.0
    assert checked_out.booking_status == "CheckedOut"

    assert by_status["Canceled"].revenue == 0.0
    assert by_status["Canceled"].booking_status == "Canceled"
    assert by_status["No-Show"].revenue == 0.0
    assert by_status["No-Show"].booking_status == "NoShow"
    assert by_status["No-Show"].is_family == 1


def test_joins_keep_row_count_and_bring_attributes(spark: SparkSession) -> None:
    bookings = spark.createDataFrame(
        [("City Hotel", "PRT"), ("Resort Hotel", "UNK"), ("City Hotel", "XYZ")], "hotel string, country string"
    )
    countries = spark.createDataFrame([("PRT", "Portugal", "Europe")], "country_code string, country_name string, continent string")
    hotels = spark.createDataFrame(
        [("City Hotel", "Lisbon", "PRT", 4, 2008), ("Resort Hotel", "Algarve", "PRT", 4, 2010)],
        "hotel_name string, city string, country_code string, star_rating int, opened_year int",
    )

    result = enrich_with_hotels(enrich_with_countries(bookings, countries), hotels)

    check_row_count(3, result, "test joins")
    prt = result.filter("country = 'PRT'").first()
    assert (prt.country_name, prt.continent, prt.hotel_city, prt.hotel_star_rating, prt.hotel_opened_year) == (
        "Portugal", "Europe", "Lisbon", 4, 2008,
    )
    assert result.filter("country_name IS NULL").count() == 2
    assert count_warn_rules(result.withColumn("arrival_date", F.current_date()), SILVER_WARN_RULES) == {
        "valid_arrival_date": 0,
        "known_country": 1,
        "has_country_metadata": 2,
    }


def test_join_with_duplicate_key_fails(spark: SparkSession) -> None:
    bookings = spark.createDataFrame([("City Hotel", "PRT")], "hotel string, country string")
    countries = spark.createDataFrame(
        [("PRT", "Portugal", "Europe"), ("PRT", "Portugal (dup)", "Europe")],
        "country_code string, country_name string, continent string",
    )

    with pytest.raises(RowCountMismatchError, match="from 1 to 2"):
        check_row_count(1, enrich_with_countries(bookings, countries), "join country_metadata")
