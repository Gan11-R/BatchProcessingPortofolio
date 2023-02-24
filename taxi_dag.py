"""
This DAG extracts data from a URL and uploads it to an Azure Blob Storage container. 
It then loads the data from the Blob Storage container into an Azure SQL Database table.

The DAG has two tasks:

extract_from_url_to_blob: This task extracts data from a URL and 
uploads it to an Azure Blob Storage container.

Load_from_blob_to_azure_sql: This task loads data from the Blob Storage container 
into an Azure SQL Database table. The DAG is scheduled to run at midnight on the 11th day of every month.

"""

from datetime import timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.utils.dates import days_ago
from Extract import ExtractToBlob
from Load import LoadToAzureSql
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Define the Extract and Load objects
extract_to_blob = ExtractToBlob(connection_string=os.environ.get('connection_string'),
                                container_name='yellow-taxi-trip',
                                start_year=2020)

load_to_azure_sql = LoadToAzureSql(server='ganisqldatabase.database.windows.net',
                                   database='datawarehouse',
                                   username=os.environ.get(
                                       'username_database'),
                                   password=os.environ.get(
                                       'password_database'),
                                   table_name='TAXI_TRIP')

# Define the default arguments for the DAG
default_args = {
    'owner': 'gani',  # The owner of the DAG
    'depends_on_past': False,  # Whether the DAG depends on past runs or not
    'start_date': days_ago(0),  # The start date of the DAG
    'email_on_failure': False,  # Whether to send an email on failure or not
    'email_on_retry': False,  # Whether to send an email on retry or not
    'retries': 1,  # Number of retries for a failed task
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'taxi_dag',  # The ID of the DAG
    default_args=default_args,
    schedule_interval='0 0 11 * *',  # Run at midnight on the 11th day of every month
)

extract_from_url_to_blob = PythonOperator(
    task_id='extract_from_url_to_blob',
    python_callable=extract_to_blob.upload_file_to_blob,
    dag=dag,
)

Load_from_blob_to_azure_sql = PythonOperator(
    task_id='Load_from_blob_to_azure_sql',
    python_callable=load_to_azure_sql.load_blob_data_to_azure,
    op_kwargs={'blob_list': extract_to_blob.list_all_files_in_blob(),  # The keyword arguments to be passed to the function
               'blob_account_name': 'ganidatalake',
               'blob_container_name': 'yellow-taxi-trip',
               'blob_account_key': os.environ.get('account_key')},
    dag=dag,
)

extract_from_url_to_blob > Load_from_blob_to_azure_sql
