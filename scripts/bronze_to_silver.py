"""Bronze -> Silver: conversão de tipos, limpeza, colunas derivadas e enriquecimento."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# O spark-submit não coloca a raiz do projeto no sys.path; adiciona aqui para importar `utils`.
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

# A fonte usa tanto o ISO-2 "CN" quanto o ISO-3 "CHN" para a China.
COUNTRY_CODE_FIXES = {"CN": "CHN"}

UNKNOWN_COUNTRY = "UNK"

BOOKING_STATUS = {"Canceled": "Canceled", "No-Show": "NoShow", "Check-Out": "CheckedOut"}

SILVER_PARTITIONS = ["arrival_date_year", "arrival_date_month_num"]


class RowCountMismatchError(RuntimeError):
    """Um join alterou o número de linhas (chave duplicada em uma tabela de referência)."""


def _map_literal(mapping: dict) -> Column:
    """Converte um dicionário Python em uma coluna de mapa literal do Spark."""
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


def cast_types(df: DataFrame) -> DataFrame:
    """Converte colunas numéricas e de data para os tipos da Silver.

    `children` usa try_cast porque a fonte tem o literal "NA" e o modo ANSI (padrão
    no Spark 4) faz o cast simples falhar. `reservation_status_date` usa to_date
    estrito: o formato foi validado, então uma falha indica mudança na fonte.
    """
    return (
        df.withColumns({c: F.col(c).cast("int") for c in INT_COLUMNS})
        .withColumn("children", F.col("children").try_cast("int"))
        .withColumn("adr", F.col("adr").cast("double"))
        .withColumn("reservation_status_date", F.to_date("reservation_status_date", "yyyy-MM-dd"))
    )


def add_arrival_date(df: DataFrame) -> DataFrame:
    """Adiciona `arrival_date_month_num` (1-12) e `arrival_date`.

    try_to_date devolve nulo para datas impossíveis em vez de falhar o job; essas
    linhas são contadas pela regra de alerta `valid_arrival_date`.
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
    """Converte códigos de país fora do padrão (ex.: "CN") para o ISO-3."""
    fixed = _map_literal(COUNTRY_CODE_FIXES)[F.col("country")]
    return df.withColumn("country", F.coalesce(fixed, F.col("country")))


def fill_nulls(df: DataFrame) -> DataFrame:
    """`children` nulo -> 0; `country` nulo -> "UNK"."""
    return df.fillna({"children": 0, "country": UNKNOWN_COUNTRY})


def apply_drop_rules(df: DataFrame, rules: dict[str, str]) -> tuple[DataFrame, dict[str, int]]:
    """Aplica cada regra de remoção em ordem; devolve o DataFrame filtrado e as linhas removidas por regra."""
    removed: dict[str, int] = {}
    for name, rule in rules.items():
        before = df.count()
        df = df.filter(F.expr(rule))
        removed[name] = before - df.count()
    return df, removed


def count_warn_rules(df: DataFrame, rules: dict[str, str]) -> dict[str, int]:
    """Conta as linhas que falham em cada regra de alerta (resultado nulo conta como falha)."""
    row = df.select(
        *[F.sum(F.when(F.coalesce(F.expr(rule), F.lit(False)), 0).otherwise(1)).alias(name) for name, rule in rules.items()]
    ).first()
    return {name: int(row[name] or 0) for name in rules}


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Adiciona as colunas derivadas: noites, hóspedes, família, status, estadia longa e receita."""
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
    # No-Show vem com is_canceled = 1, portanto não gera receita.
    return df.withColumn(
        "revenue",
        F.when(F.col("is_canceled") == 0, F.col("adr") * F.col("total_nights")).otherwise(F.lit(0.0)),
    )


def enrich_with_countries(bookings: DataFrame, countries: DataFrame) -> DataFrame:
    """LEFT JOIN com `country_metadata` pelo código do país; traz `country_name` e `continent`."""
    dim = countries.select(F.col("country_code").alias("country"), "country_name", "continent")
    return bookings.join(F.broadcast(dim), on="country", how="left")


def enrich_with_hotels(bookings: DataFrame, hotels: DataFrame) -> DataFrame:
    """LEFT JOIN com `hotel_metadata` pelo nome do hotel; traz cidade, estrelas e ano de abertura."""
    dim = hotels.select(
        F.col("hotel_name").alias("hotel"),
        F.col("city").alias("hotel_city"),
        F.col("star_rating").alias("hotel_star_rating"),
        F.col("opened_year").alias("hotel_opened_year"),
    )
    return bookings.join(F.broadcast(dim), on="hotel", how="left")


def check_row_count(expected: int, df: DataFrame, step: str) -> None:
    """Falha com RowCountMismatchError se a contagem de linhas mudou após um join."""
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
