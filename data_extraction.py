import time 
import pandas as pd
import tabula
import logging
import os 
import requests
import boto3
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table
from io import BytesIO
from urllib.parse import urlparse
from database_utils import DatabaseConnector

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataExtractor:
    """
    A class that provides methods to extract data from various sources including 
    databases, APIs, PDFs, and AWS S3 storage.

    Attributes:
        no_store_endpoint (str): The AWS endpoint to retrieve the number of stores.
        store_info_endpoint (str): The endpoint to access individual store information.
        pdf_path (str): The file path to the PDF document for data extraction.
        s3_dates_url (str): The S3 URL for date-related data.
        s3_products_url (str): The S3 URL for product-related data.
    
    """
    
    def __init__(self):
        """
        Initializes the DataExtractor class by loading endpoints, file paths, 
        and S3 URLs from environment variables.
        """
        try: 
            # Load environment variables from .env file
            load_dotenv()
            self.no_stores_endpoint = os.getenv('NO_STORES_ENDPOINT')
            self.store_info_endpoint = os.getenv('STORE_INFO_ENDPOINT')
            self.no_stores = 451 # this is the result from list_number_of_stores
        
        except ValueError as ve:
            logging.error(f"Error loading environment variables: {ve}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise     

    def read_data_from_table(self, table_name):         
        """
        Extracts data from a specified table in an AWS RDS database.

        Args:
            table_name (str): The name of the table to extract data from.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the specified table.
        """
        
        logging.info('read_data_from_table is working')

        # make an instance of the DatabaseConnector() object 
        db_connector = DatabaseConnector()  
        
        # make a database engine by using the method init_db_engine() of the instance of the DatabaseConnector object
        engine = db_connector.init_db_engine(prefix="RDS") 
        
        # Create MetaData object, needed to store and manage information about the database's structure  
        metadata = MetaData()

        # try to create a table object 
            # table_name = what table to look at  
            # metadata = helps the table object find the table
            # autoload_with = telling SQLAlchemy to use this specific connection to access the database and load the table’s details.
        try: 
            table = Table(table_name, metadata, autoload_with=engine)
        except Exception as e: 
            logging.error(f"Error reflecting table {table_name}: {e}")
            return None
         
        # try to get data from table object 
        try: 
            with engine.connect() as connection: # creates a connection 
                select_query = table.select() # requests some data from the table 
                result_of_query = connection.execute(select_query) # executing select_query and storing in result_of_query 
                data = result_of_query.fetchall() # takes results from connection.execute(select_query) and stores them in data
                df = pd.DataFrame(data, columns=[column.name for column in table.columns]) # convert the result to a DataFrame
        except Exception as e: 
            logging.error(f"Error making database connection / retrieving data {table_name}: {e}") 
            return None 

        return df

    def read_rds_table(self): 
        
        """
        Retrieves the table names from an RDS database and converts them into a DataFrame.

        Args:
            None

        Returns:
            pd.DataFrame: A DataFrame containing the names of the tables in the RDS database. 
        """

        logging.info('read_rds_table is working')

        db_connector = DatabaseConnector()
        
        # uses list_db_tables to get the table names 
        name_of_table = db_connector.list_db_tables() 

        # makes a dataframe out of the names that have been retrieved from list_db_tables method 
        dataframe = pd.DataFrame(name_of_table) 

        return dataframe

    def retrieve_pdf_data(self, pdf_path):             
        """
        Extracts table data from a PDF document.

        Args:
            None

        Returns:
            pd.DataFrame: A DataFrame containing the combined data from all tables in the PDF. 
        """
        
        logging.info('retrieve_pdf_data is working')
        
        # read_pdf returns a list of DataFrames in the PDF 
        df = tabula.read_pdf(pdf_path, pages='all')  

        # concatinates the tables together into a df      
        combined_df = pd.concat(df, ignore_index=True)                

        return combined_df 
            
  
    def list_number_of_stores(self):
        """
        Retrieves the total number of stores from the API endpoint.

        Args:
            None

        Returns:
            dict: A JSON object containing the status code and the number of stores. 
        """

        logging.info('list_number_of_stores is working')

        db_connector = DatabaseConnector() 

        # get the api headers 
        headers = db_connector.headers

        # attempt to get the number of stores, with an exception block. Returns either the json response or the exception message 
        try: 
            with requests.get(self.no_stores_endpoint, headers=headers) as response: 
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
                return response.json()
        except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error {http_err} has occurred - status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f'An error occurred: {e}')
            return None

    def retrieve_stores_data(self):   
        """
        Iterates through store numbers to retrieve store information via the API and compiles it into a DataFrame.

        Args:
            no_stores (int): The total number of stores to iterate through.

        Returns:
            pd.DataFrame: A DataFrame containing the data for all stores. 
        """
        
        logging.info('retrieve_stores_data is working')
    
        db_connector = DatabaseConnector() 

        # get the API key via the read_api_key method 
        headers = db_connector.headers

        # creating an empty list for the store details to go into 
        store_data_list = []
        store_skipped_list = [] 

        # iterating through the list of stores and creating the specific store endpoint using the number of the store  
        for store_number in range (0, self.no_stores):  
            store_suffix = str(store_number) 
            store_info_endpoint = f'{self.store_info_endpoint}{store_suffix}'   

            retry_count = 0
            max_retries = 5
            backoff_factor = 2

            while retry_count < max_retries:
                try: 
                    with requests.get(store_info_endpoint, headers=headers) as response:
                        response.raise_for_status()  # Check for HTTP errors
                        store_data_list.append(response.json())
                        break  # Exit the retry loop if the request is successful
                except requests.exceptions.RequestException as e:
                    if response.status_code == 429:  # Too Many Requests
                        retry_count += 1
                        wait_time = backoff_factor ** retry_count
                        logging.error(f'Rate limit exceeded for store {store_number}. Retrying in {wait_time} seconds...')
                        time.sleep(wait_time)
                    else:
                        logging.error(f'An error occurred for store {store_number}: {e}')
                        store_skipped_list.append({"store_number": store_number, "error": str(e)})
                        break  # Exit the loop for other types of errors

        # making a dataframe out of the list of store data 
        df_store = pd.DataFrame(store_data_list)
        df_skipped = pd.DataFrame(store_skipped_list)
        
        return df_store


    def extract_from_s3(self, uri):

        """
        Extracts data from an AWS S3 URI. The data can be either in CSV or JSON format.

        Args:
            uri (str): The S3 URI of the file to extract.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the S3 file. 
        """

        logging.info('extract_from_s3 is working')

        # setting parsed_url to be the parsed uri passed in as an argument  
        parsed_url = urlparse(uri)
        
        # splitting out the bucket and key from the URI, with an else statement to handle invalid URIs 
        if parsed_url.scheme == 'https':
            bucket = parsed_url.netloc.split('.')[0]
            key = parsed_url.path.lstrip('/')
        elif parsed_url.scheme == 's3':
            bucket = parsed_url.netloc
            key = parsed_url.path.lstrip('/')
        else:
            raise ValueError(f"Invalid URI scheme: {parsed_url.scheme}")

        # creating a boto3 client to 'talk' to S3  
        s3 = boto3.client('s3')
    
        # check the file extension to determine the format that needs to be extracted into the dataframe 
        file_extension = key.split('.')[-1].lower()
    
        # Using a context manager to handle the BytesIO buffer
        with BytesIO() as buffer:
            # Download the file from S3 into the buffer
            s3.download_fileobj(bucket, key, buffer)
        
            # Move to the beginning of the buffer to read its content
            buffer.seek(0)
            
            # Check if file extension is csv or json, and then load the data into a pandas DataFrame accordingly
            if file_extension == 'csv':
                df = pd.read_csv(buffer)
            elif file_extension == 'json':
                df = pd.read_json(buffer)
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")
        
        return df 



