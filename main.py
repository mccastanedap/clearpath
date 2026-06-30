import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote_plus
import pandas as pd
from src.email_sender import send_weekly_insights, send_csv_error
from src.validate import validate_sales_df, CSVValidationError

# --- Parche para multiprocessing en Lambda (no soporta SemLock) ---
import multiprocessing.synchronize

class _NoOpSemlock:
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def acquire(self, *args, **kwargs):
        return True
    def release(self, *args, **kwargs):
        pass

class _NoOpLock:
    def __init__(self, *args, **kwargs):
        self._semlock = _NoOpSemlock()
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def acquire(self, *args, **kwargs):
        return True
    def release(self, *args, **kwargs):
        pass

multiprocessing.synchronize.SemLock = _NoOpLock
# --- Fin del parche ---

from src.aws import get_s3_client
from src.clean import clean_sales_data
from src.config import (
    BUSINESS_TYPE,
    CLIENT_NAME,
    DEV_CLIENT_ID,
    REPORT_RECIPIENT_EMAIL,
    S3_BUCKET_NAME,
)
from src.database import get_profile_by_user_id, load_to_database
from src.email_sender import send_weekly_insights
from src.insights import generate_insights
from src.queries import daily_revenue, product_velocity, top_products
from src.s3 import read_csv_from_s3


def run_pipeline(business_name=None, business_type=None, recipient_email=None,
                 s3_key=None, user_uid=None):
    """
    Runs the full Clearpath pipeline:
    1. Clean raw sales data
    2. Load into database
    3. Run queries
    4. Generate AI insights
    5. Email insights to client

    In the multi-client (Lambda) flow, the caller passes the client's
    business_name, business_type and recipient_email (from public.profiles),
    plus the exact s3_key of the uploaded file to process, and user_uid (the
    Supabase Auth UID) which is used as client_id to keep each client's data
    separated in the warehouse.

    For local/manual runs, these can be omitted and fall back to config.py
    (CLIENT_NAME / BUSINESS_TYPE / REPORT_RECIPIENT_EMAIL), reverting to the
    old behavior of picking the most recent file under uploads/{CLIENT_NAME}/.
    """

    # Fall back to config values for local/manual runs.
    business_name = business_name or CLIENT_NAME
    business_type = business_type or BUSINESS_TYPE
    recipient_email = recipient_email or REPORT_RECIPIENT_EMAIL

    # Lambda passes the real Supabase Auth UID; local/manual runs have none,
    # so use the fixed DEV_CLIENT_ID so load and queries share one tenant.
    client_id = user_uid or DEV_CLIENT_ID

    # Step 1 - Clean
    print("Cleaning data...")

    if s3_key:
        # Multi-client flow: process exactly the file from the S3 event.
        file_key = s3_key
        print(f"Reading file from event: {file_key}")
    else:
        # Local/manual fallback: pick the most recent file for this client.
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=f'uploads/{CLIENT_NAME}/'
        )

        objects = response.get('Contents', [])
        if not objects:
            raise ValueError(f"No files found in S3 for client: {CLIENT_NAME}")

        latest_file = sorted(objects, key=lambda x: x['LastModified'], reverse=True)[0]
        file_key = latest_file['Key']
        print(f"Reading latest file: {file_key}")

    # Read the uploaded file. If pandas can't parse it (empty, binary, wrong
    # format), turn that into a friendly, client-facing error.
    try:
        raw_df = read_csv_from_s3(
            bucket_name=S3_BUCKET_NAME,
            file_key=file_key
        )
    except (pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError) as e:
        raise CSVValidationError(
            "We couldn't read this file. Please make sure it's a valid CSV "
            "(exported from Excel or your point-of-sale system) and not an Excel, "
            "PDF, or other format.",
            detail=f"read_csv_from_s3 failed: {type(e).__name__}: {e}",
        )

    # Validate contents (columns, at least one usable row).
    warning = validate_sales_df(raw_df)
    if warning:
        print(f"CSV warning: {warning}")

    clean_df = clean_sales_data(raw_df)

    # Step 2 - Load
    print("Loading to database...")
    load_to_database(clean_df, table_name='sales', client_id=client_id)

    # Step 2.5 - Run dbt models so marts reflect this batch's data
    print("Running dbt models...")
    dbt_dir = Path(__file__).resolve().parent / "clearpath_dbt"
    from dbt.cli.main import dbtRunner
    dbt_result = dbtRunner().invoke(
        ["run", "--profiles-dir", str(dbt_dir), "--project-dir", str(dbt_dir),
         "--log-path", "/tmp/dbt_logs", "--target-path", "/tmp/dbt_target",
         "--threads", "1"]
    )
    if not dbt_result.success:
        print("DBT FAILED:", dbt_result.exception)
        raise RuntimeError(f"dbt run failed: {dbt_result.exception}")
    print("dbt run completed successfully")

    # Step 3 - Query
    print("Running queries...")
    top_df = top_products(client_id=client_id)
    revenue_df = daily_revenue(client_id=client_id)
    velocity_df = product_velocity(client_id=client_id)

    # Step 4 - Insights (now returns {"headline": ..., "steps": [...]})
    print("Generating insights...")
    insights = generate_insights(
        top_df, revenue_df, velocity_df,
        business_name, business_type
    )

    # Step 4.5 - Build the report the email needs, from data we already have
    top_sorted = top_df.sort_values("total_sold", ascending=False)
    top_row = top_sorted.iloc[0] if not top_sorted.empty else None

    rev = revenue_df.copy()
    rev["date"] = pd.to_datetime(rev["date"])
    rev = rev.sort_values("date")
    last7 = float(rev.tail(7)["total_revenue"].sum())
    prev7 = float(rev.iloc[-14:-7]["total_revenue"].sum()) if len(rev) >= 14 else None

    delta_pct = None
    if prev7 and prev7 > 0 and len(rev) >= 14:
        pct = round((last7 - prev7) / prev7 * 100)
        if -100 <= pct <= 200:   # ignore absurd jumps from irregular uploads
            delta_pct = pct

    today = datetime.now()
    week_start = today - timedelta(days=6)
    next_report = today + timedelta(days=7)

    report = {
        "business_name": business_name,
        "week_range": f"{week_start:%b %d} - {today:%b %d, %Y}",
        "next_report": f"{next_report:%A, %b %d}",
        "top_product_name": str(top_row["product_name"]) if top_row is not None else "-",
        "top_product_units": int(top_row["total_sold"]) if top_row is not None else 0,
        "top_product_revenue": f"${float(top_row['total_revenue']):,.0f}" if top_row is not None else "$0",
        "week_revenue": f"${last7:,.0f}",
        "delta_pct": delta_pct,
        "headline": insights["headline"],
        "steps": insights["steps"],
    }

    print("Insights:", insights["headline"])
    for s in insights["steps"]:
        print(f"  - {s['title']}: {s['description']}")

    # Step 5 - Email
    print("\nSending weekly insights email...")
    send_weekly_insights(recipient_email, report)

    return report


if __name__ == "__main__":
    run_pipeline(
        business_name=CLIENT_NAME,
        business_type=BUSINESS_TYPE,
    )

def lambda_handler(event, context):
    """
    Entry point for AWS Lambda.
    Triggered by S3 when a new CSV is uploaded to uploads/{user_uid}/file.csv.

    Resolves the client identity from the upload path (the Supabase Auth UID)
    and public.profiles, then runs the pipeline for that specific file.
    """
    try:
        print("Lambda triggered. Event:", event)

        # Extract the S3 object key from the event (S3 keys are URL-encoded).
        raw_key = event['Records'][0]['s3']['object']['key']
        s3_key = unquote_plus(raw_key)
        print(f"Processing S3 key: {s3_key}")

        # Parse the user UID: the segment right after "uploads/".
        parts = s3_key.split('/')
        if len(parts) < 3 or parts[0] != 'uploads' or not parts[1]:
            msg = (
                f"Unexpected S3 key format: '{s3_key}'. "
                "Expected 'uploads/{user_uid}/<file>.csv'. Aborting."
            )
            print(f"ERROR: {msg}")
            return {"statusCode": 400, "body": msg}

        user_uid = parts[1]
        print(f"Resolved user UID: {user_uid}")

        # Look up the client's profile by their Supabase Auth UID.
        profile = get_profile_by_user_id(user_uid)
        if profile is None:
            msg = f"No profile found for user {user_uid}"
            print(f"ERROR: {msg}")
            return {"statusCode": 404, "body": msg}

        print(
            f"Profile found for {user_uid}: "
            f"business_name={profile['business_name']!r}, "
            f"business_type={profile['business_type']!r}"
        )

        run_pipeline(
            business_name=profile["business_name"],
            business_type=profile["business_type"],
            recipient_email=profile["email"],
            s3_key=s3_key,
            user_uid=user_uid,
        )
        return {
            "statusCode": 200,
            "body": "Pipeline completed successfully."
        }
    except CSVValidationError as e:
        # Invalid upload: tell the client what to fix and stop.
        # Do NOT re-raise (retrying the same bad file won't help).
        print(f"CSV rejected for {user_uid}: {e.detail or e.client_message}")
        send_csv_error(profile["business_name"], profile["email"], e.client_message)
        return {"statusCode": 422, "body": f"CSV rejected: {e.client_message}"}
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise