# Use the official Airflow image as the base image.
FROM apache/airflow:2.4.2


# Install other dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" python-dotenv boto3 

