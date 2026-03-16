import sqlite3
import pandas as pd
from src.database import get_connection

def run_query(query, db_path='data/clearpath.db'):
    """
    Runs any SQL query and returns results as a dataframe.
    """
    conn = get_connection(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def top_products(limit=5, db_path='data/clearpath.db'):
    """
    Returns top selling products by total quantity sold.
    """
    query = f"""
        SELECT 
            product_name,
            SUM(quantity) as total_sold,
            SUM(quantity * price) as total_revenue
        FROM sales
        GROUP BY product_name
        ORDER BY total_sold DESC
        LIMIT {limit}
    """
    return run_query(query, db_path)

def daily_revenue(days=30, db_path='data/clearpath.db'):
    """
    Returns total revenue per day for the last N days.
    """
    query = f"""
    WITH daily_sales AS (
        SELECT
            date,
            quantity * price as revenue
        FROM sales
    )
    SELECT
        date,
        sum(revenue) as total_revenue
    FROM daily_sales
    WHERE date >= date('now', '-{days} days')    -- fix 3 is done for you
    GROUP BY date
    ORDER BY date ASC
"""
    return run_query(query, db_path)