from typing import List

import duckdb


def get_raw_income(con: duckdb.DuckDBPyConnection, income_statement_metrics: List):
    """Get the TTM and previous 3 annual report values for a metric on the income statement (vs. balance sheet)."""
    select_columns = []
    where_columns = []
    for col in income_statement_metrics:
        trimmed_col = col[:-4] if col.endswith("_ttm") else col
        select_columns.append(
            f"m.{col}, a1.{trimmed_col} AS {trimmed_col}_y1, a2.{trimmed_col} AS {trimmed_col}_y2, a3.{trimmed_col} AS {trimmed_col}_y3")
        where_columns.append(
            f"m.{col} IS NOT NULL AND a1.{trimmed_col} IS NOT NULL AND a2.{trimmed_col} IS NOT NULL AND a3.{trimmed_col} IS NOT NULL")
    ttm_query = f"""
    SELECT
        m.vendor_symbol_id,
        m.year,
        m.quarter,
        m.date,
        {', '.join(select_columns)}
    FROM
        tiingo_fundamental_metrics m
    JOIN
        tiingo_fundamentals_amended_distinct a1 ON m.vendor_symbol_id = a1.vendor_symbol_id AND a1.year = m.year - 1 AND a1.quarter = 0
    JOIN
        tiingo_fundamentals_amended_distinct a2 ON m.vendor_symbol_id = a2.vendor_symbol_id AND a2.year = m.year - 2 AND a2.quarter = 0
    JOIN
        tiingo_fundamentals_amended_distinct a3 ON m.vendor_symbol_id = a3.vendor_symbol_id AND a3.year = m.year - 3 AND a3.quarter = 0
    WHERE
        {' OR '.join(where_columns)}
    ORDER BY
        m.vendor_symbol_id, m.year, m.quarter;
    """
    return con.execute(ttm_query).fetchdf()