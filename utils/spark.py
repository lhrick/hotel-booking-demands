"""SparkSession e configuração de logging compartilhadas pelos scripts."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def get_spark(app_name: str) -> SparkSession:
    """SparkSession local dimensionada para um dataset de ~120 mil linhas."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.log.level", "WARN")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
