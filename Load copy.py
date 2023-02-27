from Transform import PysparkProcessing
import pymssql
from datetime import datetime
from dateutil.relativedelta import relativedelta

class LoadToAzureSql():

    def __init__(self, server, database, username, password, table_name):
        """
        A class for loading data from Azure Blob Storage to Azure SQL Database.

        Parameters:
        -----------
        server: str
            Azure SQL Database server name.
        database: str
            Azure SQL Database name.
        username: str
            Azure SQL Database username.
        password: str
            Azure SQL Database password.
        table_name: str
            Name of the table to load data into.
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name

        # Initialize a PysparkProcessing object
        self.pyspark = PysparkProcessing()

    def last_data_in_azure_sql(self):
        """
        Returns the maximum date of the data that has been loaded into Azure SQL Database.

        Returns:
        --------
        datetime.datetime
            Maximum date of the data loaded into Azure SQL Database.
        """

        # Connect to the database
        with pymssql.connect(self.server, self.username,
                               self.password, self.database) as conn:
            # Create a cursor
            cursor = conn.cursor()

            # Check if the table exists
            query = f"SELECT COUNT(*) FROM sys.tables WHERE name = '{self.table_name}'"
            cursor.execute(query)
            if cursor.fetchone()[0] == 0:
                # Table doesn't exist, create it
                query = f"""
                CREATE TABLE {self.table_name} (
                    VendorID INT,
                    tpep_pickup_datetime DATETIME,
                    tpep_dropoff_datetime DATETIME,
                    passenger_count FLOAT,
                    trip_distance FLOAT,
                    RatecodeID FLOAT,
                    store_and_fwd_flag VARCHAR(1),
                    PULocationID INT,
                    DOLocationID INT,
                    payment_type INT,
                    fare_amount FLOAT,
                    extra FLOAT,
                    mta_tax FLOAT,
                    tip_amount FLOAT,
                    tolls_amount FLOAT,
                    improvement_surcharge FLOAT,
                    total_amount FLOAT,
                    congestion_surcharge FLOAT,
                    airport_fee FLOAT,
                    trip_duration_minutes FLOAT,
                    year INT,
                    month INT,
                    day_of_week INT,
                    day_name VARCHAR(10),
                    month_name VARCHAR(10),
                    quarter INT
                );
                """
                cursor.execute(query)
            
            # Execute a SQL query to get the maximum date in the table
            query = f"SELECT MAX(tpep_pickup_datetime) FROM {self.table_name}"
            cursor.execute(query)
            
            # Fetch the result
            last_data = cursor.fetchone()[0]

        return last_data
    
    def load_blob_data_to_azure(self, blob_list, blob_account_name, blob_container_name, blob_account_key):
        """
        Load data from a list of Parquet files stored in Azure Blob Storage to an Azure SQL database.
        only load files that have not been loaded in Azure SQL

        Parameters:
        -----------
        blob_list : list
            A list of blob names to be loaded to the Azure SQL database.
        blob_account_name : str
            The name of the Azure Blob Storage account where the Parquet files are stored.
        blob_container_name : str
            The name of the container within the Blob Storage account where the Parquet files are stored.
        blob_account_key : str
            The access key for the Azure Blob Storage account.

        Returns:
        --------
        None
        """
        # Check if there is any data in the Azure SQL table
        if self.last_data_in_azure_sql() == None:
            # If there is no data, load all blob files to the Azure SQL table
            for file in blob_list:
                # Load the data from the blob file to a PySpark DataFrame
                self.pyspark.load_parquet_from_blob(account_name=blob_account_name,
                                                    container_name=blob_container_name,
                                                    blob_account_key=blob_account_key,
                                                    file_name=file)
                # Preprocess the PySpark DataFrame
                self.pyspark.df_preprocessing()
                try:
                    # Load the preprocessed PySpark DataFrame to the Azure SQL table
                    self.pyspark.load_to_azure_sql(server=self.server,
                                                database=self.database,
                                                username=self.username,
                                                password=self.password,
                                                table_name=self.table_name)
                except Exception as e:
                    print(f"Error occurred: {e}")
        else:
            if self.last_data_in_azure_sql() == None:
                file_not_inserted = blob_list[0]

            # If there is data in the Azure SQL table,
            # load only the blob files that contain data newer than the data in the table
            while True:
                try:
                    # Get the maximum date in the Azure SQL table
                    last_data = self.last_data_in_azure_sql()
                    # Extract the year and month from the maximum date
                    year_month = last_data.strftime('%Y-%m')
                    # convert month year to datetime so it can be add one month
                    year_month_date = datetime.strptime(year_month, "%Y-%m").date()

                    # add a month so that you can get the url for the next month in the last file on the blob
                    new_date = year_month_date + relativedelta(months=1)
                    # convert back into string
                    year_month_str = new_date.strftime("%Y-%m")
                                # Construct the name of the blob file that contains data newer than the data in the table
                    file_not_inserted = f"yellow_tripdata_{year_month_str}.parquet"
                    # Load the data from the blob file to a PySpark DataFrame
                    self.pyspark.load_parquet_from_blob(account_name=blob_account_name,
                                                        container_name=blob_container_name,
                                                        blob_account_key=blob_account_key,
                                                        file_name=file_not_inserted)
                    # Preprocess the PySpark DataFrame
                    self.pyspark.df_preprocessing()
                    try:
                        # Load the preprocessed PySpark DataFrame to the Azure SQL table
                        self.pyspark.load_to_azure_sql(server=self.server,
                                                    database=self.database,
                                                    username=self.username,
                                                    password=self.password,
                                                    table_name=self.table_name)
                    except Exception as e:
                        print(f"Error occurred: {e}")
                # If there are no blob files that contain data newer than the data in the table, exit the loop
                except :
                    print('all data in blob uploaded')
                    break