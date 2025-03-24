from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from utils.callbacks import email_notification

# Import the functions for ETL
from anime_top_25.extract import get_top_data
from anime_top_25.transform import parse_data
from anime_top_25.load import load_data
from anime_top_25.validation import validate_transformed_data


# Default arguments for the DAG.
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'anime_top25_etl',
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,  # or adjust as needed
    catchup=False
) as dag:

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
    
    # Task: Validate the transformed data.
    validate_transformed_data_task = PythonOperator(
        task_id='validate_transformed_data',
        python_callable=validate_transformed_data,
        provide_context=True,
        dag=dag,
        on_failure_callback=email_notification,  # Notify on failure
    )

    # Task: Load the transformed data into DynamoDB.
    load_top_data = PythonOperator(
        task_id='load_top_data',
        python_callable=load_data,
        provide_context=True,
        dag=dag,
    )

    mark_done = BashOperator(
        task_id='mark_done',
        bash_command='touch /home/ubuntu/done_check/done.marker',
        dag=dag
    )

    # Set task dependencies: extract then transform.
    extract_top_data >> transform_top_data >> validate_transformed_data_task >> load_top_data >> mark_done
