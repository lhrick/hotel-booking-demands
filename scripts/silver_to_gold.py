"""Silver -> Gold: aggregated analytical views."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# spark-submit does not put the project root on sys.path; add it so `utils` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from utils.config import PipelineConfig, parse_args
from utils.spark import get_spark, setup_logging

logger = logging.getLogger("silver_to_gold")

TOP_COUNTRIES_LIMIT = 20

def _effective() -> Column:
    return F.col("is_canceled") == 0



def _effective_only(col: str) -> Column:
    """Value of `col` for effective (non-canceled) bookings, null otherwise; aggregates skip nulls."""
    return F.when(_effective(), F.col(col))


def _rate(numerator: str, denominator: str) -> Column:
    return F.round(F.col(numerator) / F.col(denominator), 4)


def revenue_by_hotel_month(silver: DataFrame) -> DataFrame:
    """Revenue and occupancy per hotel and arrival month."""
    return (
        silver.groupBy("hotel", "arrival_date_year", "arrival_date_month_num", "arrival_date_month")
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum(1 - F.col("is_canceled")).alias("effective_bookings"),
            F.sum("is_canceled").alias("cancelled_bookings"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg(_effective_only("adr")), 2).alias("avg_adr"),
            F.coalesce(F.sum(_effective_only("total_nights")), F.lit(0)).alias("total_nights_sold"),
        )
        .withColumn("cancellation_rate", _rate("cancelled_bookings", "total_bookings"))
        .orderBy("hotel", "arrival_date_year", "arrival_date_month_num")
    )


def cancellation_by_segment(silver: DataFrame) -> DataFrame:
    """Cancellation behaviour per market segment, distribution channel and customer type."""
    return (
        silver.groupBy("market_segment", "distribution_channel", "customer_type")
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum("is_canceled").alias("cancelled_bookings"),
            F.round(F.avg("lead_time"), 2).alias("avg_lead_time"),
            F.round(F.avg("total_of_special_requests"), 2).alias("avg_total_special_requests"),
        )
        .withColumn("cancellation_rate", _rate("cancelled_bookings", "total_bookings"))
        .orderBy(F.desc("total_bookings"))
    )


def top_countries_by_revenue(silver: DataFrame, limit: int = TOP_COUNTRIES_LIMIT) -> DataFrame:
    """Top countries by revenue from effective bookings; unknown country is excluded from the ranking."""
    ranked = (
        silver.filter(_effective() & (F.col("country") != "UNK"))
        .groupBy("country", "country_name", "continent")
        .agg(
            F.count("*").alias("effective_bookings"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.sum("revenue") / F.count("*"), 2).alias("avg_ticket"),
            F.round(F.avg("lead_time"), 2).alias("avg_lead_time"),
        )
        # Single-partition window is fine: at most ~180 countries.
        .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"), "country")))
    )
    return ranked.filter(F.col("revenue_rank") <= limit).orderBy("revenue_rank")


def stay_profile(silver: DataFrame) -> DataFrame:
    """Stay length and guest profile per hotel and customer type (effective bookings only)."""
    return (
        silver.filter(_effective())
        .groupBy("hotel", "customer_type")
        .agg(
            F.count("*").alias("effective_bookings"),
            F.round(F.avg("total_nights"), 2).alias("avg_total_nights"),
            F.round(F.avg("total_guests"), 2).alias("avg_total_guests"),
            F.round(F.avg("is_long_stay"), 4).alias("pct_long_stay"),
            F.round(F.avg("is_family"), 4).alias("pct_family"),
        )
        .orderBy("hotel", "customer_type")
    )


GOLD_VIEWS = {
    "revenue_by_hotel_month": revenue_by_hotel_month,
    "cancellation_by_segment": cancellation_by_segment,
    "top_countries_by_revenue": top_countries_by_revenue,
    "stay_profile": stay_profile,
}


def run(spark: SparkSession, config: PipelineConfig) -> None:
    silver = spark.read.parquet(str(config.silver_dir / "bookings_enriched"))
    logger.info("bookings_enriched: read %d rows from silver", silver.count())

    for name, build in GOLD_VIEWS.items():
        target = str(config.gold_dir / name)
        # Gold views are tiny; a single ordered file is easier to consume.
        build(silver).coalesce(1).write.mode("overwrite").parquet(target)
        logger.info("%s: wrote %d rows to %s", name, spark.read.parquet(target).count(), target)


def main() -> None:
    setup_logging()
    config = parse_args(__doc__)
    spark = get_spark("silver_to_gold")
    try:
        run(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
