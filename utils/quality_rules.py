"""Regras de qualidade como expressões SQL booleanas ({nome: expressão}).

Mantidas como SQL string para serem reaproveitadas como expectations no Databricks.
Uma linha passa na regra quando a expressão é verdadeira.
"""

# Linhas que falham nestas regras são removidas da Silver.
SILVER_DROP_RULES: dict[str, str] = {
    "has_guests": "adults + children + babies > 0",
    "non_negative_adr": "adr >= 0",
}

# Linhas que falham nestas regras são mantidas; as falhas só são contadas e logadas.
SILVER_WARN_RULES: dict[str, str] = {
    "valid_arrival_date": "arrival_date IS NOT NULL",
    "known_country": "country <> 'UNK'",
    "has_country_metadata": "country_name IS NOT NULL",
    # Limite heurístico: o maior adr plausível na fonte é 510; acima de 1000 indica provável erro de digitação.
    "adr_within_expected_range": "adr <= 1000",
}
