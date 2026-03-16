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
    WHERE date >= date('now', '-{days} days')    
    GROUP BY date
    ORDER BY date ASC
"""
    return run_query(query, db_path)


def product_velocity(db_path='data/clearpath.db'):
    """
    Returns sales velocity per product.
    Velocity = total quantity sold / days in market.
    Low velocity products are spoilage/dead stock risks.
    """
    query = """
        WITH velocity_calc AS (
            SELECT
                product_name,
                SUM(quantity) as total_sold,
                MIN(date) as first_sale,
                MAX(date) as last_sale,
                (julianday(MAX(date)) - julianday(MIN(date))) as days_in_market
            FROM sales
            GROUP BY product_name
        )
        SELECT
            product_name,
            total_sold,
            days_in_market,
            CASE 
                WHEN days_in_market = 0 THEN total_sold
                ELSE ROUND(total_sold * 1.0 / days_in_market, 2)
            END as velocity
        FROM velocity_calc
        ORDER BY velocity ASC
    """
    return run_query(query, db_path)