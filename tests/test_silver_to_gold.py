import pytest
from pyspark.sql import DataFrame, SparkSession

from scripts.silver_to_gold import cancellation_by_segment, revenue_by_hotel_month, stay_profile, top_countries_by_revenue

SILVER_SCHEMA = (
    "hotel string, arrival_date_year int, arrival_date_month_num int, arrival_date_month string, "
    "market_segment string, distribution_channel string, customer_type string, "
    "country string, country_name string, continent string, "
    "is_canceled int, adr double, total_nights int, total_guests int, revenue double, "
    "lead_time int, total_of_special_requests int, is_long_stay int, is_family int"
)


def _silver(spark: SparkSession, rows: list[dict]) -> DataFrame:
    defaults = {
        "hotel": "City Hotel", "arrival_date_year": 2016, "arrival_date_month_num": 7, "arrival_date_month": "July",
        "market_segment": "Online TA", "distribution_channel": "TA/TO", "customer_type": "Transient",
        "country": "PRT", "country_name": "Portugal", "continent": "Europe",
        "is_canceled": 0, "adr": 100.0, "total_nights": 2, "total_guests": 2, "revenue": 200.0,
        "lead_time": 10, "total_of_special_requests": 0, "is_long_stay": 0, "is_family": 0,
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
    assert row.avg_adr == 75.0  # canceled booking's adr is ignored
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

    for view in (cancellation_by_segment(silver), revenue_by_hotel_month(silver)):
        rates = [r.cancellation_rate for r in view.collect()]
        assert rates and all(0 <= r <= 1 for r in rates)

    by_segment = {r.market_segment: r.cancellation_rate for r in cancellation_by_segment(silver).collect()}
    assert by_segment == {"Groups": 1.0, "Direct": 0.0, "Online TA": 0.5}


def test_top_countries_limit_order_and_exclusions(spark: SparkSession) -> None:
    rows = [{"country": f"C{i:02d}", "country_name": f"Country {i}", "revenue": float(i * 10)} for i in range(25)]
    rows += [
        {"country": "UNK", "country_name": None, "continent": None, "revenue": 1e9},  # unknown country is not ranked
        {"country": "C24", "country_name": "Country 24", "is_canceled": 1, "revenue": 0.0},  # canceled is excluded
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
