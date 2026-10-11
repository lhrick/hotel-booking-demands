"""Bronze -> Silver: type casting, cleaning, derived columns and enrichment."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# spark-submit does not put the project root on sys.path; add it so `utils` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.config import PipelineConfig, parse_args
from utils.quality_rules import SILVER_DROP_RULES, SILVER_WARN_RULES
from utils.spark import get_spark, setup_logging

logger = logging.getLogger("bronze_to_silver")

INT_COLUMNS = [
    "is_canceled", "lead_time", "arrival_date_year", "arrival_date_week_number",
    "arrival_date_day_of_month", "stays_in_weekend_nights", "stays_in_week_nights",
    "adults", "babies", "is_repeated_guest", "previous_cancellations",
    "previous_bookings_not_canceled", "booking_changes", "days_in_waiting_list",
    "required_car_parking_spaces", "total_of_special_requests",
]

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# Source uses both the ISO-2 "CN" and the ISO-3 "CHN" for China.
COUNTRY_CODE_FIXES = {"CN": "CHN"}

UNKNOWN_COUNTRY = "UNK"

BOOKING_STATUS = {"Canceled": "Canceled", "No-Show": "NoShow", "Check-Out": "CheckedOut"}

SILVER_PARTITIONS = ["arrival_date_year", "arrival_date_month_num"]


class RowCountMismatchError(RuntimeError):
    """A join changed the number of rows (duplicate keys in a reference table)."""


def _map_literal(mapping: dict) -> Column:
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


def cast_types(df: DataFrame) -> DataFrame:
    """Cast numeric and date columns to the Silver types.

    `children` uses try_cast because the source has literal "NA" and ANSI mode
    (default in Spark 4) makes a plain cast fail. `reservation_status_date` uses a
    strict to_date: its format was validated, so a failure means the source changed.
    """
    return (
        df.withColumns({c: F.col(c).cast("int") for c in INT_COLUMNS})
        .withColumn("children", F.col("children").try_cast("int"))
        .withColumn("adr", F.col("adr").cast("double"))
        .withColumn("reservation_status_date", F.to_date("reservation_status_date", "yyyy-MM-dd"))
    )


def add_arrival_date(df: DataFrame) -> DataFrame:
    """Add `arrival_date_month_num` (1-12) and `arrival_date`.

    try_to_date returns null for impossible dates instead of failing the job;
    those rows are counted by the `valid_arrival_date` warn rule.
    """
    month_num = _map_literal({name: i for i, name in enumerate(MONTHS, start=1)})[F.col("arrival_date_month")]
    df = df.withColumn("arrival_date_month_num", month_num)
    date_str = F.concat_ws(
        "-",
        F.col("arrival_date_year"),
        F.lpad(F.col("arrival_date_month_num").cast("string"), 2, "0"),
        F.lpad(F.col("arrival_date_day_of_month").cast("string"), 2, "0"),
    )
    return df.withColumn("arrival_date", F.try_to_date(date_str, "yyyy-MM-dd"))


def normalize_country_codes(df: DataFrame) -> DataFrame:
    """Map non-standard country codes (e.g. "CN") to their ISO-3 form."""
    fixed = _map_literal(COUNTRY_CODE_FIXES)[F.col("country")]
    return df.withColumn("country", F.coalesce(fixed, F.col("country")))


def fill_nulls(df: DataFrame) -> DataFrame:
    """`children` null -> 0; `country` null -> "UNK"."""
    return df.fillna({"children": 0, "country": UNKNOWN_COUNTRY})


def apply_drop_rules(df: DataFrame, rules: dict[str, str]) -> tuple[DataFrame, dict[str, int]]:
    """Apply each drop rule in order; return the filtered DataFrame and rows removed per rule."""
    removed: dict[str, int] = {}
    for name, rule in rules.items():
        before = df.count()
        df = df.filter(F.expr(rule))
        removed[name] = before - df.count()
    return df, removed


def count_warn_rules(df: DataFrame, rules: dict[str, str]) -> dict[str, int]:
    """Count rows failing each warn rule (null results count as failures)."""
    row = df.select(
        *[F.sum(F.when(F.coalesce(F.expr(rule), F.lit(False)), 0).otherwise(1)).alias(name) for name, rule in rules.items()]
    ).first()
    return {name: int(row[name] or 0) for name in rules}


def add_derived_columns(df: DataFrame) -> DataFrame:
    total_nights = F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights")
    df = df.withColumns(
        {
            "total_nights": total_nights,
            "total_guests": F.col("adults") + F.col("children") + F.col("babies"),
            "is_family": ((F.col("children") > 0) | (F.col("babies") > 0)).cast("int"),
            "booking_status": F.coalesce(_map_literal(BOOKING_STATUS)[F.col("reservation_status")], F.col("reservation_status")),
            "is_long_stay": (total_nights > 7).cast("int"),
        }
    )
    # No-Show rows have is_canceled = 1, so they generate no revenue.
    return df.withColumn(
        "revenue",
        F.when(F.col("is_canceled") == 0, F.col("adr") * F.col("total_nights")).otherwise(F.lit(0.0)),
    )


def enrich_with_countries(bookings: DataFrame, countries: DataFrame) -> DataFrame:
    dim = countries.select(F.col("country_code").alias("country"), "country_name", "continent")
    return bookings.join(F.broadcast(dim), on="country", how="left")


def enrich_with_hotels(bookings: DataFrame, hotels: DataFrame) -> DataFrame:
    dim = hotels.select(
        F.col("hotel_name").alias("hotel"),
        F.col("city").alias("hotel_city"),
        F.col("star_rating").alias("hotel_star_rating"),
        F.col("opened_year").alias("hotel_opened_year"),
    )
    return bookings.join(F.broadcast(dim), on="hotel", how="left")


def check_row_count(expected: int, df: DataFrame, step: str) -> None:
    actual = df.count()
    if actual != expected:
        raise RowCountMismatchError(
            f"{step}: row count changed from {expected} to {actual}; the reference table has duplicate join keys"
        )
    logger.info("%s: %d rows (unchanged)", step, actual)


def run(spark: SparkSession, config: PipelineConfig) -> None:
    bookings = spark.read.parquet(str(config.bronze_dir / "bookings"))
    countries = spark.read.parquet(str(config.bronze_dir / "countries"))
    hotels = spark.read.parquet(str(config.bronze_dir / "hotels"))
    logger.info("bookings: read %d rows from bronze", bookings.count())

    df = add_arrival_date(cast_types(bookings))
    logger.info("children: %d values not castable to int (source 'NA') -> 0", df.filter(F.col("children").isNull()).count())
    fixed = df.filter(F.col("country").isin(*COUNTRY_CODE_FIXES)).count()
    logger.info("country: %d rows normalized %s", fixed, COUNTRY_CODE_FIXES)
    logger.info("country: %d null values -> '%s'", df.filter(F.col("country").isNull()).count(), UNKNOWN_COUNTRY)
    df = fill_nulls(normalize_country_codes(df))

    df, removed = apply_drop_rules(df, SILVER_DROP_RULES)
    for name, count in removed.items():
        logger.info("drop rule %s: removed %d rows", name, count)

    df = add_derived_columns(df)
    expected = df.count()
    logger.info("bookings after drop rules: %d rows", expected)

    df = enrich_with_countries(df, countries)
    check_row_count(expected, df, "join country_metadata")
    df = enrich_with_hotels(df, hotels)
    check_row_count(expected, df, "join hotel_metadata")

    for name, failed in count_warn_rules(df, SILVER_WARN_RULES).items():
        logger.log(logging.WARNING if failed else logging.INFO, "warn rule %s: %d rows failed", name, failed)

    target = str(config.silver_dir / "bookings_enriched")
    df.write.mode("overwrite").partitionBy(*SILVER_PARTITIONS).parquet(target)
    logger.info("bookings_enriched: wrote %d rows to %s", spark.read.parquet(target).count(), target)


def main() -> None:
    setup_logging()
    config = parse_args(__doc__)
    spark = get_spark("bronze_to_silver")
    try:
        run(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
