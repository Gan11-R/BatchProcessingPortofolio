# Batch Processing Data Pipeline

## Workflow
![Pipeline Workflow](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Pipeline%20Workflow.png?raw=true)

## Data
The dataset used is yellow taxi trip data taken from the website NYC goverment [data source.](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) The data has a parquet format and is packed every month

The Yellow Taxi Trip Records dataset is a collection of data about yellow taxi trips taken in New York City. The dataset includes information about pickup and drop-off times and locations, fares, and other details of each trip. The data is collected from GPS devices installed in the taxis and covers trips taken in all five boroughs of New York City. The dataset is used by researchers and analysts to study transportation patterns and traffic flows, and to gain insights into urban life.

Sample Dataset:

|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|1|2022-01-01 00:35:40.000|2022-01-01 00:53:29.000|2|3,8|1|N|142|236|1|14,5|3|0,5|3,65|0|0,3|21,95|2,5|0|
|1|2022-01-01 00:33:43.000|2022-01-01 00:42:07.000|1|2,1|1|N|236|42|1|8|0,5|0,5|4|0|0,3|13,3|0|0|
|2|2022-01-01 00:53:21.000|2022-01-01 01:02:19.000|1|0,97|1|N|166|166|1|7,5|0,5|0,5|1,76|0|0,3|10,56|0|0|
|2|2022-01-01 00:25:21.000|2022-01-01 00:35:23.000|1|1,09|1|N|114|68|2|8|0,5|0,5|0|0|0,3|11,8|2,5|0|
|2|2022-01-01 00:36:48.000|2022-01-01 01:14:20.000|1|4,3|1|N|68|163|1|23,5|0,5|0,5|3|0|0,3|30,3|2,5|0|

## Technologies Used
This pipeline is built using the following technologies:
-   **Python** - a programming language used for data engineering and processing. The libraries and dependencies used are listed in [requirements.txt](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/requirements.txt)
-   **Apache Spark (PySpark)** - a fast and distributed processing engine used for large-scale data processing.
- **Apache Airflow** - to schedule and orchestrate the data engineering pipeline
-   **Azure Blob Storage** - to store the parquet data files in a scalable and cost-effective manner.
- **Azure SQL Database** - to store and manage the processed data from the Yellow Taxi Trip Records dataset in a structured and efficient manner after transformed with pyspark.
-   **Google Data Studio** - to create visualizations and reports based on the data from the Yellow Taxi Trip Records dataset stored in Azure SQL Database

## Project Structure
### Ingesting
In this process the parquet file is taken with the url then stored in blob storage. **The url that is retrieved is only the url of the file that has not been stored in blob storage**, so that when the python code is executed it will only retrieve new data and does not cause duplicate data. This process is run using [Extract.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Extract.py)

### Transform
Data is retrieved from blob storage using pyspark, then transforms by adding the following columns:

 - Compute the trip duration in minutes from the "tpep_pickup_datetime" and "tpep_dropoff_datetime".
 - Extract the year, month, day of the week, and quarter from the "tpep_pickup_datetime" column.
 - Format the day of the week and month name columns.
 
Additional column after the transformation process:
|trip_duration_minutes|year|month|day_of_week|day_name|month_name|quarter|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|17,816666666666666|2022|1|7|Saturday|January|1|
|8,4|2022|1|7|Saturday|January|1|
|8,966666666666667|2022|1|7|Saturday|January|1|
|10,033333333333333|2022|1|7|Saturday|January|1|
|37,53333333333333|2022|1|7|Saturday|January|1|

Transformation process with pyspark using [Transform.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Transform.py)

Then the data that has been transformed is uploaded to the Azure SQL database. **The upload process will only add data that is not yet in the database**, so there will be no duplication of data. This process uses [Load.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Load.py)

### Visualization
Data visualization using google data studio which retrieves data from azure sql database. The dashboard can be seen in the following [link](url).

### Scheduling
Python code is run once a month on day 11 using file [taxi_dag.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/taxi_dag.py)
Scheduling using apache airflow which is installed locally due to lack of resources, so the data may not be updated.


*NB: Python code is written based on OOP best practices
