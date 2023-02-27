from pyspark.sql import SparkSession
import findspark
from datetime import datetime, timedelta
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from pyspark.sql.functions import year, month, dayofweek, quarter, date_format, unix_timestamp
from pyspark.sql.types import DateType
import os

findspark.init()


class PysparkProcessing():
    """
    A class for processing data using PySpark.

    ...

    Attributes
    ----------
    spark : SparkSession
        an instance of the SparkSession class
    df : DataFrame
        the Spark DataFrame that will hold the loaded data

    Methods
    -------
    load_parquet_from_blob(account_name, container_name, file_name, blob_account_key, hours_expiry=1)
        loads a parquet file from an Azure Blob Storage container into a Spark DataFrame
    get_df()
        returns the Spark DataFrame that holds the loaded data
    df_preprocessing()
        performs some data preprocessing on the Spark DataFrame
    load_to_azure_sql(server, database, username, password, table_name)
        loads the preprocessed data from the Spark DataFrame into an Azure SQL Database table
    """

    def __init__(self):
        pass

    def load_parquet_from_blob(self, account_name, container_name, file_name, blob_account_key,  hours_expiry=1):
        """
        Loads a parquet file from an Azure Blob Storage container into a Spark DataFrame.

        Parameters
        ----------
        account_name : str
            the name of the Azure Blob Storage account
        container_name : str
            the name of the container where the parquet file is stored
        file_name : str
            the name of the parquet file
        blob_account_key : str
            the access key for the Azure Blob Storage account
        hours_expiry : int, optional
            the number of hours the SAS token should remain valid (default is 1)

        Returns
        -------
        None
        """

        # Generate a SAS token with read permission for the parquet file
        sas_i = generate_blob_sas(account_name=account_name,
                                  container_name=container_name,
                                  blob_name=file_name,
                                  account_key=blob_account_key,
                                  permission=BlobSasPermissions(read=True),
                                  expiry=datetime.now() + timedelta(hours=hours_expiry))

        # Create an instance of the SparkSession class
        self.spark = SparkSession.builder.appName("Spark Preprocessing")\
                                         .master('local')\
                                         .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6")\
                                         .getOrCreate()

        # Allow Spark to read from Blob remotely
        wasbs_path = f'wasbs://{container_name}@{account_name}.blob.core.windows.net/{file_name}'
        self.spark.conf.set("spark.sql.debug.maxToStringFields", "30")
        self.spark.conf.set(
            f'fs.azure.sas.{container_name}.{account_name}.blob.core.windows.net', sas_i)

        print(f"{file_name} loading in process")

        # Load the parquet file into a Spark DataFrame
        self.df = self.spark.read.parquet(wasbs_path)

        # Split filename into base name and extension
        base_name, extension = os.path.splitext(file_name)

        # Split base name to extract year and month
        file_year, file_month = base_name.split('_')[-1].split('-')
        file_year, file_month = int(file_year), int(file_month)

        # filter only data that has the year and month that match the file name
        # thus getting rid of the wrong year and month
        self.df = self.df.filter((year('tpep_pickup_datetime') == file_year) & (
            month('tpep_pickup_datetime') == file_month))

        print(f"{file_name} loaded in Dataframe")

    def get_df(self):
        """
        Returns the Spark DataFrame that holds the loaded data.

        Parameters
        ----------
        None

        Returns
        -------
        DataFrame
            the Spark DataFrame that holds the loaded data
        """
        return self.df

    def df_preprocessing(self):
        """
        Perform data preprocessing on the loaded PySpark DataFrame.

        This method performs the following operations on the PySpark DataFrame:
            1. Compute the trip duration in minutes from the "tpep_pickup_datetime" and "tpep_dropoff_datetime" columns.
            2. Extract the year, month, day of the week, and quarter from the "tpep_pickup_datetime" column.
            3. Format the day of the week and month name columns.

        The modified DataFrame is stored in the `df` attribute of the class instance.

        Returns:
            None
        """
        print("Transforming in process")

       # Extract year, month, day of week, and quarter from the "timestamp_unix" column, and format day and month names
        self.df = self.df.withColumn("trip_duration_minutes", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60) \
            .withColumn("year", year("tpep_pickup_datetime")) \
            .withColumn("month", month("tpep_pickup_datetime")) \
            .withColumn("day_of_week", dayofweek("tpep_pickup_datetime")) \
            .withColumn("day_name", date_format("tpep_pickup_datetime", "EEEE").cast(DateType())) \
            .withColumn("month_name", date_format("tpep_pickup_datetime", "MMMM").cast(DateType())) \
            .withColumn("quarter", quarter("tpep_pickup_datetime"))

        print("Transformed")

    def load_to_azure_sql(self, server, database, username, password, table_name):
        """
        Loads the transformed PySpark dataframe into an Azure SQL database table.

        Parameters:
        -----------
        server : str
            The fully qualified domain name (FQDN) of the Azure SQL server.
        database : str
            The name of the Azure SQL database.
        username : str
            The SQL server admin username.
        password : str
            The SQL server admin password.
        table_name : str
            The name of the table to write the data to.

        Returns:
        --------
        None

        """
        # Define the JDBC connection URL
        jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database}"

        # Define the connection properties
        properties = {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": username,
            "password": password,
            "encrypt": "true",
            "trustServerCertificate": "false",
            "hostNameInCertificate": "*.database.windows.net",
            "loginTimeout": "30"
        }

        # Write the data to the database
        print("Data Uploading in Process")
        self.df.write.jdbc(url=jdbc_url, table=table_name,
                           mode="append", properties=properties)

        # If the write operation is successful, commit the transaction
        print("Data successfully Uploaded to database.")
