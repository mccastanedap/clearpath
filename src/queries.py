import sqlite3
import pandas as pd
from src.database import get_connection

def run_query(query, db_path='data/clearpath.db'):
    conn = get_connection(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def top_products(limit=5, db_path='data/clearpath.db'):
    query = f"""
        SELECT * FROM mart_top_products
        LIMIT {limit}
    """
    return run_query(query, db_path)

def daily_revenue(days=30, db_path='data/clearpath.db'):
    query = f"""
        SELECT * FROM mart_daily_revenue
        ORDER BY date DESC
        LIMIT {days}
    """
    return run_query(query, db_path)

def product_velocity(db_path='data/clearpath.db'):
    query = """
        SELECT * FROM mart_product_velocity
        ORDER BY velocity ASC
    """
    return run_query(query, db_path)