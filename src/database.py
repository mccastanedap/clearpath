"""Postgres (Supabase) loader for the Clearpath pipeline."""

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from src.config import (
    SUPABASE_DATABASE,
    SUPABASE_HOST,
    SUPABASE_PASSWORD,
    SUPABASE_PORT,
    SUPABASE_USER,
)


def get_connection():
    """Open a connection to the Supabase Postgres warehouse."""
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=int(SUPABASE_PORT),
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        dbname=SUPABASE_DATABASE,
        sslmode="require",
    )


def get_profile_by_user_id(user_id):
    """
    Look up a client profile in public.profiles by their Supabase Auth UID.

    Returns a dict with keys business_name, business_type, email, or None if
    no profile matches the given user_id.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT business_name, business_type, email "
                "FROM public.profiles WHERE user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return {
        "business_name": row[0],
        "business_type": row[1],
        "email": row[2],
    }


def load_to_database(df, table_name, schema="clearpath", client_id=None):
    """
    Replace this client's rows in {schema}.{table_name} with df rows.

    Multi-tenant mode (client_id given): DELETE only the rows belonging to
    client_id, then bulk INSERT df rows stamped with that client_id. The
    client_id is added as an extra column on every inserted row, so callers
    don't need it in the DataFrame. DELETE + INSERT run in one transaction,
    so a client's re-upload is idempotent and never touches other clients.

    Legacy mode (client_id is None): falls back to the old TRUNCATE + INSERT
    of the whole table, preserving behavior for any non-tenant callers.
    """
    if df.empty:
        print(f"No rows to load into {schema}.{table_name}")
        return

    columns = list(df.columns)

    table_ref = sql.Identifier(schema, table_name)

    if client_id is not None:
        # Per-client: stamp client_id onto every row and only clear this
        # client's existing rows (never the whole table).
        insert_cols = columns + ["client_id"]
        rows = [
            tuple(None if pd.isna(v) else v for v in row) + (client_id,)
            for row in df.itertuples(index=False, name=None)
        ]
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols)
        clear_stmt = sql.SQL(
            "DELETE FROM {tbl} WHERE client_id = %s"
        ).format(tbl=table_ref)
        clear_params = (client_id,)
    else:
        # Legacy single-tenant behavior: wipe the whole table.
        rows = [
            tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False, name=None)
        ]
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        clear_stmt = sql.SQL("TRUNCATE TABLE {tbl}").format(tbl=table_ref)
        clear_params = None

    insert_stmt = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES %s").format(
        tbl=table_ref, cols=col_list
    )

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(clear_stmt, clear_params)
            execute_values(cur, insert_stmt.as_string(cur), rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"Loaded {len(rows)} rows into {schema}.{table_name}")


def compute_weekly_summary(df):
    """
    From a cleaned sales DataFrame (columns: date, product_name, category,
    size, quantity, price), compute a one-week summary dict:
    week_start, week_end (real date range in the file), total_revenue,
    total_units, and the top product by units. Returns None if there are no
    usable rows.
    """
    if df is None or df.empty:
        return None

    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["quantity"] = pd.to_numeric(d["quantity"], errors="coerce")
    d["price"] = pd.to_numeric(d["price"], errors="coerce")
    d = d.dropna(subset=["date", "quantity", "price"])
    d = d[d["quantity"] > 0]
    if d.empty:
        return None

    d["revenue"] = d["quantity"] * d["price"]
    by_product = d.groupby("product_name").agg(
        units=("quantity", "sum"),
        revenue=("revenue", "sum"),
    ).reset_index().sort_values("units", ascending=False)
    top_row = by_product.iloc[0]

    return {
        "week_start": d["date"].min().date(),
        "week_end": d["date"].max().date(),
        "total_revenue": round(float(d["revenue"].sum()), 2),
        "total_units": int(d["quantity"].sum()),
        "top_product_name": str(top_row["product_name"]),
        "top_product_units": int(top_row["units"]),
        "top_product_revenue": round(float(top_row["revenue"]), 2),
    }


def save_weekly_summary(df, client_id, schema="clearpath"):
    """
    Compute this batch's weekly summary and upsert it into
    {schema}.weekly_history, keyed by (client_id, week_start, week_end).
    If a summary for the same client and date range already exists, it's
    updated (so a client's re-upload of the same period is idempotent).
    Does nothing if there are no usable rows or no client_id.
    """
    if client_id is None:
        print("No client_id given; skipping weekly history.")
        return

    summary = compute_weekly_summary(df)
    if summary is None:
        print("No usable rows for weekly history; skipping.")
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {tbl} "
                    "(client_id, week_start, week_end, total_revenue, total_units, "
                    "top_product_name, top_product_units, top_product_revenue) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (client_id, week_start, week_end) DO UPDATE SET "
                    "total_revenue = EXCLUDED.total_revenue, "
                    "total_units = EXCLUDED.total_units, "
                    "top_product_name = EXCLUDED.top_product_name, "
                    "top_product_units = EXCLUDED.top_product_units, "
                    "top_product_revenue = EXCLUDED.top_product_revenue, "
                    "created_at = now()"
                ).format(tbl=sql.Identifier(schema, "weekly_history")),
                (
                    client_id,
                    summary["week_start"],
                    summary["week_end"],
                    summary["total_revenue"],
                    summary["total_units"],
                    summary["top_product_name"],
                    summary["top_product_units"],
                    summary["top_product_revenue"],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        f"Saved weekly history for {client_id}: "
        f"{summary['week_start']} to {summary['week_end']}, "
        f"revenue {summary['total_revenue']}, units {summary['total_units']}."
    )
