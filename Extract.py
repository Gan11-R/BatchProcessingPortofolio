import requests
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from dateutil.relativedelta import relativedelta


class ExtractToBlob():

    def __init__(self, connection_string, container_name, start_year) -> None:
        """
        Initialize the ExtractToBlob class.

        Parameters
        ----------
        connection_string : str
            The connection string for the Azure Blob Storage account.
        container_name : str
            The name of the container to upload the taxi data to.
        start_year : int
            The start year for downloading taxi data if the container is empty.
        """
        self.__start_year = start_year

        self.__blob_service_client = BlobServiceClient.from_connection_string(
            connection_string)
        self.__container_client = self.__blob_service_client.get_container_client(
            container_name)

    def list_all_files_in_blob(self):
        """
        List all the files in the blob storage container.

        Returns
        -------
        list of str
            A list of the names of all the files in the container.
            example = ["yellow_tripdata_2022-11.parquet", "yellow_tripdata_2022-12.parquet"]
        """
        blob_list = self.__container_client.list_blobs()
        return [blob.name for blob in blob_list]

    def get_taxi_url_to_download(self):
        """
        Get the url to download the next month's file from the last file that was in the blob container. 
        If there is no file in the container, get the url according to the year that was input 
        at the class instance

        Returns
        -------
        tuple of str
            A tuple containing the URL to download the file and the name of the file.
            example = ("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-12.parquet", 
            yellow_tripdata_2022-12.parquet)
        """
        try:
            # get the year and month of the last filename in the blob container list
            # example: name file is yellow_taxitrip-2022-12.parquet get 2022-12
            blob_names = self.list_all_files_in_blob()[-1]
            month_year = blob_names.split("_")[2].split(".")[0]
        except IndexError:
            # If no files are found in the container,
            # set the month_year to the start year minus 1 and December
            # example if start year is 2022, it will set 2021 December
            self.__start_year -= 1
            month_year = f"{self.__start_year}-12"

        # convert month year to datetime so it can be add one month
        month_year_date = datetime.strptime(month_year, "%Y-%m").date()

        # add a month so that you can get the url for the next month in the last file on the blob
        new_date = month_year_date + relativedelta(months=1)
        # convert back into string
        year_month_str = new_date.strftime("%Y-%m")

        # get url by adding the base pattern url to year and month
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        name_file = f"yellow_tripdata_{year_month_str}.parquet"
        url = base_url + name_file

        return url, name_file

    def upload_file_to_blob(self):
        """
        Download the next month's taxi data file and upload it to the Azure Blob Storage container, 
        using a loop until there are no new files in the data source
        """
        while True:
            # call get url method
            urlSource = self.get_taxi_url_to_download()
            # get only the url from tupple output
            url = urlSource[0]
            response = requests.get(url)

            # if next month's files haven't been released then it's out of the loop
            if response.status_code != 200:
                print("[files are up to date]")
                break

            # get the name file from tupple output
            file_name = urlSource[1]
            # define blob client
            blob_client = self.__container_client.get_blob_client(file_name)
            print(f"[uploading] {file_name}")
            # upload file to blob
            blob_client.upload_blob(response.content, overwrite=True)
            print(f"{file_name} [OK]")
