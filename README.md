# Batch Processing Data Pipeline

## Workflow
![Pipeline Workflow](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Pipeline%20Workflow.png?raw=true)

## Data
The dataset used is yellow taxi trip data taken from the website NYC goverment [data source.](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) The data has a parquet format and is packed every month

The Yellow Taxi Trip Records dataset is a collection of data about yellow taxi trips taken in New York City. The dataset includes information about pickup and drop-off times and locations, fares, and other details of each trip. The data is collected from GPS devices installed in the taxis and covers trips taken in all five boroughs of New York City. The dataset is used by researchers and analysts to study transportation patterns and traffic flows, and to gain insights into urban life.

Dataset Schema:
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


Transformation process with pyspark using [Transform.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Transform.py)

Then the data that has been transformed is uploaded to the Azure SQL database. **The upload process will only add data that is not yet in the database**, so there will be no duplication of data. This process uses [Load.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/Load.py)

### Visualization
Data visualization using google data studio which retrieves data from azure sql database. The dashboard can be seen in the following [link](url).

### Scheduling
Python code is run once a month on day 11 using file [taxi_dag.py.](https://github.com/Gan11-R/BatchProcessingPortofolio/blob/main/taxi_dag.py)
Scheduling using apache airflow which is installed locally due to lack of resources, so the data may not be updated.

