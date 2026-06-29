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


def top_products(client_id, limit=5):
    return run_query(
        "SELECT * FROM clearpath.mart_top_products "
        "WHERE client_id = %s LIMIT %s",
        params=(client_id, limit),
    )


def daily_revenue(client_id, days=30):
    return run_query(
        "SELECT * FROM clearpath.mart_daily_revenue "
        "WHERE client_id = %s ORDER BY date DESC LIMIT %s",
        params=(client_id, days),
    )


def product_velocity(client_id):
    return run_query(
        "SELECT * FROM clearpath.mart_product_velocity "
        "WHERE client_id = %s ORDER BY velocity ASC",
        params=(client_id,),
    )
