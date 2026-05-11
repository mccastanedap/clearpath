"""Read queries against the dbt marts in Supabase Postgres."""

import pandas as pd

from src.database import get_connection


def run_query(query, params=None):
    """Run a SQL query and return the result as a DataFrame."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def top_products(limit=5):
    return run_query(
        "SELECT * FROM clearpath.mart_top_products LIMIT %s",
        params=(limit,),
    )


def daily_revenue(days=30):
    return run_query(
        "SELECT * FROM clearpath.mart_daily_revenue ORDER BY date DESC LIMIT %s",
        params=(days,),
    )


def product_velocity():
    return run_query(
        "SELECT * FROM clearpath.mart_product_velocity ORDER BY velocity ASC"
    )
