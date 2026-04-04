"""
Clearpath Retail Analytics Pipeline
====================================
An Airflow DAG that runs every Monday at 8am to:
  1. Extract sales CSV from S3
  2. Clean/transform the data
  3. Load to SQLite
  4. Run dbt transformations
  5. Query aggregated results
  6. Generate Claude AI insights
  7. Email insights to the client
"""

import os
from datetime import datetime

from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ── Load .env so all tasks inherit credentials ────────────────────────────────
# find .env relative to the project root (one level up from dags/)
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_project_root, '.env'))

# ── Source imports ─────────────────────────────────────────────────────────────
from src.s3 import read_csv_from_s3
from src.clean import clean_sales_data
from src.database import load_to_database
from src.queries import top_products, daily_revenue, product_velocity
from src.insights import generate_insights
from src.email_sender import send_weekly_insights

# ── Default arguments ──────────────────────────────────────────────────────────
# These apply to every task in the DAG unless a task overrides them.
# 'retries': 1 means if a task fails Airflow will try it once more automatically.
default_args = {
    'owner': 'clearpath',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# ── DAG definition ─────────────────────────────────────────────────────────────
# schedule_interval='0 8 * * 1'  →  cron expression
#   ┌─ minute   (0)
#   │ ┌─ hour   (8  = 8am)
#   │ │ ┌─ day  (* = every day of month)
#   │ │ │ ┌─ month (* = every month)
#   │ │ │ │ ┌─ weekday (1 = Monday)
#   0 8 * * 1
# In plain English: "at 08:00 on every Monday"
#
# catchup=False → only run for today; don't backfill all missed runs since start_date.

with DAG(
    dag_id='clearpath_pipeline',
    default_args=default_args,
    schedule_interval='0 8 * * 1',
    catchup=False,
    description='Weekly Clearpath retail analytics pipeline',
) as dag:

    # ── TASK 1: extract ────────────────────────────────────────────────────────
    # Downloads the latest sales CSV from S3 and returns it as a JSON string.
    #
    # CONCEPT — XCom (Cross-Communication):
    #   When a PythonOperator callable returns a value, Airflow automatically
    #   stores it in XCom. The next task can retrieve it with xcom_pull().
    #   XCom is designed for small metadata (< ~48 KB); passing entire DataFrames
    #   works fine for modest datasets but should not be used for large files.
    #
    # AIRFLOW UI:
    #   In the Graph view this is the first box on the left. Click the box,
    #   then "XCom" in the task instance pop-up to see the stored JSON string.

    def extract():
        df = read_csv_from_s3('clearpath-retail-data', 'raw/sales.csv')
        return df.to_json()

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    # ── TASK 2: transform ──────────────────────────────────────────────────────
    # Pulls raw JSON from XCom, cleans it, returns clean JSON.
    #
    # CONCEPT — context / TaskInstance (ti):
    #   PythonOperator passes a 'context' dict to callables that use **context.
    #   context['ti'] is the TaskInstance object for the current run.
    #   ti.xcom_pull(task_ids='extract') fetches whatever task 'extract' returned.
    #
    # AIRFLOW UI:
    #   The second box in the Graph view. Its XCom entry will contain the
    #   cleaned DataFrame as JSON (fewer rows than extract if rows were dropped).

    def transform(**context):
        import pandas as pd
        raw_json = context['ti'].xcom_pull(task_ids='extract')
        df = pd.read_json(raw_json)
        clean_df = clean_sales_data(df)
        return clean_df.to_json()

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    # ── TASK 3: load ───────────────────────────────────────────────────────────
    # Pulls clean JSON and writes it to SQLite via load_to_database().
    # Returns nothing — no XCom produced here.
    #
    # AIRFLOW UI:
    #   Green square = data is now in the database. Logs will show row counts.

    def load(**context):
        import pandas as pd
        clean_json = context['ti'].xcom_pull(task_ids='transform')
        df = pd.read_json(clean_json)
        load_to_database(df, table_name='sales')

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # ── TASK 4: run_dbt ────────────────────────────────────────────────────────
    # Runs dbt models using the BashOperator.
    #
    # CONCEPT — BashOperator:
    #   Runs any shell command inside the Airflow worker process.
    #   We use it for dbt because dbt is a command-line tool, not a Python API.
    #
    # CONCEPT — Airflow Variables:
    #   Variables are key-value pairs stored in Airflow's metadata database.
    #   They let you configure DAGs without editing code.
    #   Set them once:
    #     airflow variables set project_root "/absolute/path/to/clearpath"
    #   Read them in code:   Variable.get('project_root')
    #   Read them in Jinja:  {{ var.value.project_root }}
    #
    # bash_command uses Jinja templating: {{ var.value.project_root }} is
    # resolved at runtime by Airflow's templating engine before the shell runs.
    #
    # AIRFLOW UI:
    #   Click the run_dbt box → "Log" → see the full dbt output (model names,
    #   row counts, pass/fail). A red square means a dbt model failed.

    run_dbt_task = BashOperator(
        task_id='run_dbt',
        bash_command='cd {{ var.value.project_root }}/clearpath_dbt && dbt run --profiles-dir .',
    )

    # ── TASK 5: run_queries ────────────────────────────────────────────────────
    # Runs the three analytical SQL queries and stores results via XCom.
    #
    # CONCEPT — XCom with dicts:
    #   We return a dict of JSON strings (one per query result). This is better
    #   than returning three separate XComs because downstream tasks only need
    #   one xcom_pull call. Each DataFrame is serialised to JSON so it survives
    #   the XCom round-trip through Airflow's database.
    #
    # AIRFLOW UI:
    #   XCom entry will be a JSON object with keys: top_products, daily_revenue,
    #   product_velocity. Click XCom in the task pop-up to inspect it.

    def run_queries():
        top_df = top_products()
        revenue_df = daily_revenue()
        velocity_df = product_velocity()
        return {
            'top_products': top_df.to_json(),
            'daily_revenue': revenue_df.to_json(),
            'product_velocity': velocity_df.to_json(),
        }

    run_queries_task = PythonOperator(
        task_id='run_queries',
        python_callable=run_queries,
    )

    # ── TASK 6: generate_insights ──────────────────────────────────────────────
    # Sends query results to Claude and returns the AI-generated insights text.
    #
    # CONCEPT — chained XCom:
    #   This task pulls from run_queries (task_ids='run_queries'), processes the
    #   data, and returns the insights string — which the next task will pull.
    #   XCom creates a data pipeline inside the orchestration layer itself.
    #
    # CONCEPT — Airflow Variables (again):
    #   client_name and business_type are stored as Variables so you can serve
    #   different clients by changing a Variable, not the code.
    #   Set them:
    #     airflow variables set client_name "Juice Bar NYC"
    #     airflow variables set business_type "juice bar / cafe"
    #
    # AIRFLOW UI:
    #   The XCom entry for this task will contain the full Claude response text.
    #   The task log shows the Claude model, token usage, and any API errors.

    def do_generate_insights(**context):
        import pandas as pd

        query_results = context['ti'].xcom_pull(task_ids='run_queries')

        top_df = pd.read_json(query_results['top_products'])
        revenue_df = pd.read_json(query_results['daily_revenue'])
        velocity_df = pd.read_json(query_results['product_velocity'])

        client_name = Variable.get('client_name', default_var='Our Client')
        business_type = Variable.get('business_type', default_var='retail store')

        insights = generate_insights(
            top_products_df=top_df,
            daily_revenue_df=revenue_df,
            velocity_df=velocity_df,
            business_name=client_name,
            business_type=business_type,
        )
        return insights

    generate_insights_task = PythonOperator(
        task_id='generate_insights',
        python_callable=do_generate_insights,
    )

    # ── TASK 7: send_email ─────────────────────────────────────────────────────
    # Pulls insights text from XCom and emails it to the client via SendGrid.
    #
    # CONCEPT — Airflow Variables for config:
    #   client_email is stored as a Variable so you can update the recipient
    #   without touching this file. In a multi-tenant setup you'd loop over a
    #   list of clients stored in a Variable or Airflow Connection.
    #
    #   Variables to set:
    #     airflow variables set client_name  "Juice Bar NYC"
    #     airflow variables set client_email "melissa.c.castaneda.p@gmail.com"
    #
    # AIRFLOW UI:
    #   When this box turns green the email has been sent. Logs show the
    #   SendGrid HTTP status code (202 = accepted for delivery).

    def do_send_email(**context):
        insights_text = context['ti'].xcom_pull(task_ids='generate_insights')
        client_name = Variable.get('client_name', default_var='Our Client')
        client_email = Variable.get('client_email')
        send_weekly_insights(client_name, client_email, insights_text)

    send_email_task = PythonOperator(
        task_id='send_email',
        python_callable=do_send_email,
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    # The >> operator means "this task must finish before the next one starts".
    # Airflow reads this chain to build the DAG graph you see in the UI.
    # If you want two tasks to run in parallel you put them on separate lines
    # without chaining them, e.g.:
    #     task_a >> [task_b, task_c] >> task_d
    #
    # Full linear pipeline:
    extract_task >> transform_task >> load_task >> run_dbt_task >> run_queries_task >> generate_insights_task >> send_email_task
