from Extract import ExtractToBlob
from Load import LoadToAzureSql

# Define the Extract and Load objects

extract_to_blob = ExtractToBlob(connection_string='DefaultEndpointsProtocol=https;AccountName=ganidatalake;AccountKey=/4czNSwC7SJjIZkl2Vg+p5rkW452Fnnzp3Q2YwEhKNCgk2ksRZr6tLILQhZ1oaDCmWTee5W5Wr4V+AStE7nZUw==;EndpointSuffix=core.windows.net',
                                container_name='yellow-taxi-trip',
                                start_year=2022)

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