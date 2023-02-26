from Extract import ExtractToBlob
from Load import LoadToAzureSql
from Transform import PysparkProcessing
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()
# Define the Extract and Load objects

extract_to_blob = ExtractToBlob(connection_string='DefaultEndpointsProtocol=https;AccountName=ganidatalake;AccountKey=/4czNSwC7SJjIZkl2Vg+p5rkW452Fnnzp3Q2YwEhKNCgk2ksRZr6tLILQhZ1oaDCmWTee5W5Wr4V+AStE7nZUw==;EndpointSuffix=core.windows.net',
                                container_name='yellow-taxi-trip',
                                start_year=2022)
'''
extract_to_blob.upload_file_to_blob()

load_to_azure_sql = LoadToAzureSql(server='ganisqldatabase.database.windows.net',
                                   database='datawarehouse',
                                   username='gani@ganisqldatabase',
                                   password='Tahun2023',
                                   table_name='TAXI_TRIP')

load_to_azure_sql.load_blob_data_to_azure(blob_list=extract_to_blob.list_all_files_in_blob(),
                                          blob_account_name='ganidatalake',
                                          blob_container_name='yellow-taxi-trip',
                                          blob_account_key='/4czNSwC7SJjIZkl2Vg+p5rkW452Fnnzp3Q2YwEhKNCgk2ksRZr6tLILQhZ1oaDCmWTee5W5Wr4V+AStE7nZUw==')
'''


'''
SCHEMA

root
 |-- VendorID: long (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: double (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: double (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: long (nullable = true)
 |-- DOLocationID: long (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- airport_fee: double (nullable = true)
 |-- trip_duration_minutes: double (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- day_of_week: integer (nullable = true)
 |-- day_name: string (nullable = true)
 |-- month_name: string (nullable = true)
 |-- quarter: integer (nullable = true)
'''