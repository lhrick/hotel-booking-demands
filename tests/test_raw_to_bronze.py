from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scripts.raw_to_bronze import add_ingestion_metadata, read_raw_csv
from utils.schemas import HOTELS_SCHEMA

CSV_OPTIONS = {"header": True, "sep": ",", "nullValue": "NULL", "mode": "FAILFAST", "enforceSchema": False}


def test_ingestion_metadata_columns(spark: SparkSession, tmp_path: Path) -> None:
    csv = tmp_path / "hotel_metadata.csv"
    csv.write_text("hotel_name,city,country_code,star_rating,opened_year\nCity Hotel,Lisbon,PRT,4,NULL\n")
    ingested_at = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)

    df = add_ingestion_metadata(read_raw_csv(spark, str(csv), HOTELS_SCHEMA, CSV_OPTIONS), ingested_at)
    # Formata dentro do Spark (fuso da sessão em UTC): o collect() converteria para o fuso local do Python.
    row = df.withColumn("ingested_at_utc", F.date_format("ingested_at", "yyyy-MM-dd HH:mm:ss")).first()

    assert df.columns[-2:] == ["ingested_at", "source_file_name"]
    assert row.source_file_name == "hotel_metadata.csv"
    assert row.ingested_at_utc == "2026-10-10 12:00:00"
    assert row.opened_year is None
