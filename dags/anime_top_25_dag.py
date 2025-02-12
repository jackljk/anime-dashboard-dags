from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import the functions for ETL
from anime_top_25.extract import get_top_data
from anime_top_25.transform import parse_data
from anime_top_25.load import load_data


# Default arguments for the DAG.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'anime_top25_etl',
    default_args=default_args,
    schedule_interval='@daily',  # or adjust as needed
    catchup=False
)

# Task: Extract data from the Jikan API.
extract_top_data = PythonOperator(
    task_id='extract_top_data',
    python_callable=get_top_data,
    provide_context=True,
    dag=dag,
)

# Task: Transform the extracted data.
transform_top_data = PythonOperator(
    task_id='transform_top_data',
    python_callable=parse_data,
    provide_context=True,
    dag=dag,
)

# Task: Load the transformed data into DynamoDB.
load_top_data = PythonOperator(
    task_id='load_top_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies: extract then transform.
extract_top_data >> transform_top_data >> load_top_data
