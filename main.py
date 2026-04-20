import sys
import boto3
import os
from dotenv import load_dotenv
import pandas as pd
from src.clean import clean_sales_data
from src.database import load_to_database
from src.queries import top_products, daily_revenue, product_velocity
from src.insights import generate_insights
from src.s3 import read_csv_from_s3
from src.email_sender import send_weekly_insights


load_dotenv()
# --- Client configuration ---
CLIENT_NAME = "Juice Bar NYC"
CLIENT_EMAIL = "melissa.c.castaneda.p@gmail.com"

def run_pipeline( business_name, business_type):
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

    # Get the most recent file uploaded by this client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )

    response = s3_client.list_objects_v2(
        Bucket='clearpath-retail-data',
        Prefix=f'uploads/{CLIENT_NAME}/'
    )

    objects = response.get('Contents', [])
    if not objects:
        raise ValueError(f"No files found in S3 for client: {CLIENT_NAME}")

    latest_file = sorted(objects, key=lambda x: x['LastModified'], reverse=True)[0]
    latest_key = latest_file['Key']
    print(f"Reading latest file: {latest_key}")

    raw_df = read_csv_from_s3(
        bucket_name='clearpath-retail-data',
        file_key=latest_key
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
    
        business_name=CLIENT_NAME,
        business_type='Juice Bar'
    )