"""Raw -> Bronze: ingest the source CSVs as-is into Snappy Parquet."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# spark-submit does not put the project root on sys.path; add it so `utils` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from utils.config import PipelineConfig, parse_args
from utils.schemas import BOOKINGS_SCHEMA, COUNTRIES_SCHEMA, HOTELS_SCHEMA
from utils.spark import get_spark, setup_logging

logger = logging.getLogger("raw_to_bronze")

# (raw file key, schema, bronze table, partition columns)
SOURCES: list[tuple[str, StructType, str, list[str]]] = [
    ("bookings", BOOKINGS_SCHEMA, "bookings", ["arrival_date_year"]),
    ("countries", COUNTRIES_SCHEMA, "countries", []),
    ("hotels", HOTELS_SCHEMA, "hotels", []),
]


def read_raw_csv(spark: SparkSession, path: str, schema: StructType, options: dict) -> DataFrame:
    """Read a source CSV with an explicit schema and the configured options."""
    return spark.read.options(**options).schema(schema).csv(path)


def write_bronze(df: DataFrame, path: str, partition_by: list[str]) -> None:
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def run(spark: SparkSession, config: PipelineConfig) -> None:
    for source, schema, table, partition_by in SOURCES:
        df = read_raw_csv(spark, config.raw_file(source), schema, config.csv_options)
        logger.info("%s: read %d rows from %s", table, df.count(), config.raw_file(source))

        target = str(config.bronze_dir / table)
        write_bronze(df, target, partition_by)
        written = spark.read.parquet(target).count()
        logger.info("%s: wrote %d rows to %s (partitioned by %s)", table, written, target, partition_by or "-")


def main() -> None:
    setup_logging()
    config = parse_args(__doc__)
    spark = get_spark("raw_to_bronze")
    try:
        run(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
