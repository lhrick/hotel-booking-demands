import pytest
from pyspark.sql import DataFrame, SparkSession

from scripts.silver_to_gold import (
    RISK_MIN_BOOKINGS,
    cancellation_by_segment,
    cancellation_risk_profile,
    revenue_by_hotel_month,
    stay_profile,
    top_countries_by_revenue,
)

SILVER_SCHEMA = (
    "hotel string, arrival_date_year int, arrival_date_month_num int, arrival_date_month string, "
    "market_segment string, distribution_channel string, customer_type string, "
    "country string, country_name string, continent string, "
    "is_canceled int, adr double, total_nights int, total_guests int, revenue double, "
    "lead_time int, total_of_special_requests int, is_long_stay int, is_family int, "
    "deposit_type string, lead_time_bucket string, previous_cancellations_bucket string, has_previous_stay int"
)


def _silver(spark: SparkSession, rows: list[dict]) -> DataFrame:
    defaults = {
        "hotel": "City Hotel", "arrival_date_year": 2016, "arrival_date_month_num": 7, "arrival_date_month": "July",
        "market_segment": "Online TA", "distribution_channel": "TA/TO", "customer_type": "Transient",
        "country": "PRT", "country_name": "Portugal", "continent": "Europe",
        "is_canceled": 0, "adr": 100.0, "total_nights": 2, "total_guests": 2, "revenue": 200.0,
        "lead_time": 10, "total_of_special_requests": 0, "is_long_stay": 0, "is_family": 0,
        "deposit_type": "No Deposit", "lead_time_bucket": "008-030", "previous_cancellations_bucket": "0",
        "has_previous_stay": 0,
    }
    columns = [c.split()[0] for c in SILVER_SCHEMA.split(", ")]
    return spark.createDataFrame([tuple({**defaults, **r}[c] for c in columns) for r in rows], SILVER_SCHEMA)


def test_revenue_by_hotel_month_metrics(spark: SparkSession) -> None:
    silver = _silver(
        spark,
        [
            {"adr": 100.0, "total_nights": 2, "revenue": 200.0},
            {"adr": 50.0, "total_nights": 4, "revenue": 200.0},
            {"is_canceled": 1, "adr": 500.0, "total_nights": 3, "revenue": 0.0},
        ],
    )

    row = revenue_by_hotel_month(silver).first()

    assert (row.total_bookings, row.effective_bookings, row.cancelled_bookings) == (3, 2, 1)
    assert row.total_revenue == 400.0
    assert row.avg_adr == 75.0  # o adr da reserva cancelada é ignorado
    assert row.total_nights_sold == 6
    assert row.cancellation_rate == pytest.approx(0.3333)


def test_cancellation_rate_between_0_and_1(spark: SparkSession) -> None:
    silver = _silver(
        spark,
        [
            {"market_segment": "Groups", "is_canceled": 1},
            {"market_segment": "Groups", "is_canceled": 1},
            {"market_segment": "Direct", "is_canceled": 0},
            {"market_segment": "Online TA", "is_canceled": 1},
            {"market_segment": "Online TA", "is_canceled": 0},
        ],
    )

    for view in (cancellation_by_segment(silver), revenue_by_hotel_month(silver), cancellation_risk_profile(silver)):
        rates = [r.cancellation_rate for r in view.collect()]
        assert rates and all(0 <= r <= 1 for r in rates)

    by_segment = {r.market_segment: r.cancellation_rate for r in cancellation_by_segment(silver).collect()}
    assert by_segment == {"Groups": 1.0, "Direct": 0.0, "Online TA": 0.5}


def test_top_countries_limit_order_and_exclusions(spark: SparkSession) -> None:
    rows = [{"country": f"C{i:02d}", "country_name": f"Country {i}", "revenue": float(i * 10)} for i in range(25)]
    rows += [
        {"country": "UNK", "country_name": None, "continent": None, "revenue": 1e9},  # país desconhecido não entra no ranking
        {"country": "C24", "country_name": "Country 24", "is_canceled": 1, "revenue": 0.0},  # reserva cancelada é excluída
    ]

    result = top_countries_by_revenue(_silver(spark, rows)).collect()

    assert len(result) == 20
    revenues = [r.total_revenue for r in result]
    assert revenues == sorted(revenues, reverse=True)
    assert [r.revenue_rank for r in result] == list(range(1, 21))
    assert result[0].country == "C24"
    assert result[0].effective_bookings == 1
    assert "UNK" not in {r.country for r in result}


def test_stay_profile_uses_effective_bookings(spark: SparkSession) -> None:
    silver = _silver(
        spark,
        [
            {"total_nights": 10, "is_long_stay": 1, "is_family": 1, "total_guests": 3},
            {"total_nights": 2, "total_guests": 1},
            {"is_canceled": 1, "total_nights": 30, "is_long_stay": 1},
        ],
    )

    row = stay_profile(silver).first()

    assert row.effective_bookings == 2
    assert (row.avg_total_nights, row.avg_total_guests) == (6.0, 2.0)
    assert (row.pct_long_stay, row.pct_family) == (0.5, 0.5)


def test_top_countries_keeps_same_name_countries_apart(spark: SparkSession) -> None:
    silver = _silver(
        spark,
        [
            {"country": "KOR", "country_name": "Korea", "continent": "Asia", "revenue": 300.0},
            {"country": "PRK", "country_name": "Korea", "continent": "Asia", "revenue": 100.0},
        ],
    )

    result = {r.country: r.total_revenue for r in top_countries_by_revenue(silver).collect()}

    assert result == {"KOR": 300.0, "PRK": 100.0}


def test_cancellation_risk_profile_potential_revenue_counts_only_cancelled(spark: SparkSession) -> None:
    silver = _silver(
        spark,
        [
            {"adr": 100.0, "total_nights": 2, "revenue": 200.0},
            {"is_canceled": 1, "adr": 50.0, "total_nights": 3, "revenue": 0.0},
        ],
    )

    row = cancellation_risk_profile(silver).first()

    assert (row.total_bookings, row.cancelled_bookings) == (2, 1)
    assert row.cancelled_potential_revenue == 150.0


@pytest.mark.parametrize(
    ("cancelled", "total", "risk_level"),
    [(6, 10, "high"), (5, 10, "medium"), (3, 10, "medium"), (2, 10, "low")],
)
def test_cancellation_risk_profile_risk_level_boundaries(
    spark: SparkSession, cancelled: int, total: int, risk_level: str
) -> None:
    silver = _silver(spark, [{"is_canceled": int(i < cancelled), "revenue": 0.0} for i in range(total)])

    row = cancellation_risk_profile(silver).first()

    assert row.risk_level == risk_level


@pytest.mark.parametrize(("total", "has_min_sample"), [(RISK_MIN_BOOKINGS - 1, 0), (RISK_MIN_BOOKINGS, 1)])
def test_cancellation_risk_profile_min_sample(spark: SparkSession, total: int, has_min_sample: int) -> None:
    row = cancellation_risk_profile(_silver(spark, [{}] * total)).first()

    assert row.has_min_sample == has_min_sample
