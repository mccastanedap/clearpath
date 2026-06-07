import subprocess
import sys
from pathlib import Path

from src.aws import get_s3_client
from src.clean import clean_sales_data
from src.config import (
    BUSINESS_TYPE,
    CLIENT_NAME,
    REPORT_RECIPIENT_EMAIL,
    S3_BUCKET_NAME,
)
from src.database import load_to_database
from src.email_sender import send_weekly_insights
from src.insights import generate_insights
from src.queries import daily_revenue, product_velocity, top_products
from src.s3 import read_csv_from_s3


def run_pipeline(business_name, business_type):
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
    s3_client = get_s3_client()

    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=f'uploads/{CLIENT_NAME}/'
    )

    objects = response.get('Contents', [])
    if not objects:
        raise ValueError(f"No files found in S3 for client: {CLIENT_NAME}")

    latest_file = sorted(objects, key=lambda x: x['LastModified'], reverse=True)[0]
    latest_key = latest_file['Key']
    print(f"Reading latest file: {latest_key}")

    raw_df = read_csv_from_s3(
        bucket_name=S3_BUCKET_NAME,
        file_key=latest_key
    )
    clean_df = clean_sales_data(raw_df)

    # Step 2 - Load
    print("Loading to database...")
    load_to_database(clean_df, table_name='sales')

    # Step 2.5 - Run dbt models so marts reflect this batch's data
    print("Running dbt models...")
    dbt_dir = Path(__file__).resolve().parent / "clearpath_dbt"
    result = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run", "--profiles-dir", "."],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )
    print("DBT STDOUT:", result.stdout)
    print("DBT STDERR:", result.stderr)
    result.check_returncode()

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
    send_weekly_insights(CLIENT_NAME, REPORT_RECIPIENT_EMAIL, insights)

    return insights


if __name__ == "__main__":
    run_pipeline(
        business_name=CLIENT_NAME,
        business_type=BUSINESS_TYPE,
    )

def lambda_handler(event, context):
    """
    Entry point for AWS Lambda.
    Triggered by S3 when a new CSV is uploaded.
    """
    try:
        print("Lambda triggered. Event:", event)
        insights = run_pipeline(
            business_name=CLIENT_NAME,
            business_type=BUSINESS_TYPE,
        )
        return {
            "statusCode": 200,
            "body": "Pipeline completed successfully."
        }
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise