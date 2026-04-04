import sys
import pandas as pd
from src.clean import clean_sales_data
from src.database import load_to_database
from src.queries import top_products, daily_revenue, product_velocity
from src.insights import generate_insights
from src.s3 import read_csv_from_s3
from src.email_sender import send_weekly_insights

# --- Client configuration ---
CLIENT_NAME = "Juice Bar NYC"
CLIENT_EMAIL = "client@example.com"

def run_pipeline(raw_file_path, business_name, business_type):
    """
    Runs the full Clearpath pipeline:
    1. Clean raw sales data
    2. Load into database
    3. Run queries
    4. Generate AI insights
    5. Email insights to client
    """

    # Step 1 - Clean
    print("Cleaning data...")
    raw_df = read_csv_from_s3(
        bucket_name='clearpath-retail-data',
        file_key='raw/sales.csv'
    )
    clean_df = clean_sales_data(raw_df)

    # Step 2 - Load
    print("Loading to database...")
    load_to_database(clean_df, table_name='sales')

    # Step 3 - Query
    print("Running queries...")
    top_df = top_products()
    revenue_df = daily_revenue()
    velocity_df = product_velocity()

    # Step 4 - Insights
    print("Generating insights...")
    insights = generate_insights(
        top_df, revenue_df, velocity_df,
        business_name, business_type
    )

    sys.stdout.buffer.write(b"\n--- CLEARPATH INSIGHTS ---\n\n")
    sys.stdout.buffer.write(insights.encode("utf-8", errors="replace"))
    sys.stdout.buffer.write(b"\n")
    sys.stdout.flush()

    # Step 5 - Email
    print("\nSending weekly insights email...")
    send_weekly_insights(CLIENT_NAME, CLIENT_EMAIL, insights)

    return insights

if __name__ == "__main__":
    run_pipeline(
        raw_file_path='data/raw/sales.csv',
        business_name=CLIENT_NAME,
        business_type='Juice Bar'
    )