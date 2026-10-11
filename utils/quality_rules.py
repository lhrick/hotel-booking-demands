"""Data quality rules as SQL boolean expressions ({name: expression}).

Kept as SQL strings so the same rules can be reused as Databricks expectations.
A row passes a rule when the expression evaluates to true.
"""

# Rows failing these rules are removed from Silver.
SILVER_DROP_RULES: dict[str, str] = {
    "has_guests": "adults + children + babies > 0",
    "non_negative_adr": "adr >= 0",
}

# Rows failing these rules are kept; failures are only counted and logged.
SILVER_WARN_RULES: dict[str, str] = {
    "valid_arrival_date": "arrival_date IS NOT NULL",
    "known_country": "country <> 'UNK'",
    "has_country_metadata": "country_name IS NOT NULL",
}
