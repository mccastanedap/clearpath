from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.s3 import read_csv_from_s3
from src.clean import clean_sales_data
from src.database import load_to_database
from src.queries import top_products, daily_revenue, product_velocity
from src.insights import generate_insights

default_args = {
    'owner': 'clearpath',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='clearpath_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    def extract():
        df = read_csv_from_s3('clearpath-retail-data', 'raw/sales.csv')
        return df.to_json()

    def transform(**context):
        import pandas as pd
        raw_json = context['ti'].xcom_pull(task_ids='extract')
        df = pd.read_json(raw_json)
        clean_df = clean_sales_data(df)
        return clean_df.to_json()

    def load(**context):
        import pandas as pd
        clean_json = context['ti'].xcom_pull(task_ids='transform')
        df = pd.read_json(clean_json)
        load_to_database(df, table_name='sales')

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    extract_task >> transform_task >> load_task