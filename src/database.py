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


def load_to_database(df, table_name, schema="clearpath"):
    """
    Replace the contents of {schema}.{table_name} with df rows.
    TRUNCATE + bulk INSERT in a single transaction, so re-runs are idempotent.
    """
    if df.empty:
        print(f"No rows to load into {schema}.{table_name}")
        return

    columns = list(df.columns)
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    table_ref = sql.Identifier(schema, table_name)
    col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    truncate_stmt = sql.SQL("TRUNCATE TABLE {tbl}").format(tbl=table_ref)
    insert_stmt = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES %s").format(
        tbl=table_ref, cols=col_list
    )

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(truncate_stmt)
            execute_values(cur, insert_stmt.as_string(cur), rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"Loaded {len(rows)} rows into {schema}.{table_name}")
