import pandas as pd
import tabula
import logging
import os 
from dotenv import load_dotenv
import requests
import boto3
import yaml 
import re
from sqlalchemy import MetaData, Table
from io import StringIO
from io import BytesIO
from urllib.parse import urlparse
from database_utils import DatabaseConnector

#logging.basicConfig(filename='api_client.log', level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataExtractor:
    """
    This class contains the methods to extract data  

    Attributes: 
        no_store_endpoint: the AWS endpoint that returns the number of stores the company has 
        store_info_endpoint: the end point to access store information 
        pdf_path: the path to the PDF file used in read_data_from_pdf
        header: the API key to access the stores 
    
    """
    
    def __init__(self):
        self.no_stores_endpoint = os.getenv('NO_STORES_ENDPOINT')
        self.store_info_endpoint = os.getenv('STORE_INFO_ENDPOINT')
        self.pdf_path = os.getenv('PDF_PATH')
        self.s3_dates_url = os.getenv('S3_DATES_URL') 
        self.s3_products_url = os.getenv('S3_PRODUCTS_URL') 

    def read_data_from_table(self, table_name):         
        """
        This method gets the data from a specific table in AWS RDS 
        
        Args: 
            table_name: the name of the table you're trying to get

        Returns: 
            dataframe: of the table you put in as an argument 
        """
        
        #so I know that the method is working 
        logging.info('read_data_from_table is working')

        # make an instance of the DatabaseConnector() object because I'm going to use some of the methods of that class
        db_connector = DatabaseConnector()  
        
        # make a database 'key' by using the method init_db_engine() from the instance of the DatabaseConnector object I've created
        engine = db_connector.init_db_engine(prefix="RDS") 
        
        # Create MetaData object, I need this so that I have a way to store and manage information about the database's structure  
        metadata = MetaData()

        # try to create a table object (a table object gives details about a particuar table. We pass in details about that object) 
            # table_name = what table to look at
            # metadata = helps the table object find the table
            # autoload_with = telling SQLAlchemy to use this specific connection to access the database and load the table’s details.
        try: 
            table = Table(table_name, metadata, autoload_with=engine)
        except Exception as e: 
            logging.error(f"Error reflecting table {table_name}: {e}")
            return None
         
        # try to get data from table object I've just created  
        try: 
            with engine.connect() as connection: # creates a connection 
                select_query = table.select() # requests some data from the table 
                result_of_query = connection.execute(select_query) # this tells the connection to get the actual data (which is the result of 'table.select()) 
                data = result_of_query.fetchall() # takes all the results from connection.execute(select_query) and stores them in a variable so they can be worked with
                df = pd.DataFrame(data, columns=[column.name for column in table.columns]) # convert the result to a DataFrame
        except Exception as e: 
            logging.error(f"Error making database connection / retrieving data {table_name}: {e}") 
            return None 

        # return a df of the data that comes back from that query  
        return df

    def read_rds_table(self): 
        
        """
        This method gets the table names from the RDS database   
        
        Args: 
            None

        Returns: 
            dataframe: of the table names in the database 
        """

        #so I know that the method is working 
        logging.info('read_rds_table is working')

        # make an instance of the DatabaseConnection class 
        db_connector = DatabaseConnector()
        
        # uses list_db_tables to get the table names 
        name_of_table = db_connector.list_db_tables() 

        # akes a dataframe out of the names that have been retrieved from list_db_tables method 
        dataframe = pd.DataFrame(name_of_table) 

        # returns it 
        return dataframe

    def retrieve_pdf_data(self):             
        """
        This method gets PDF table data from a PDF document.    
        
        Args: 
            None

        Returns: 
            dataframe: of the table(s) in the PDF 
        """
        
        #so I know that the method is working 
        logging.info('retrieve_pdf_data is working')

        # sets the pdf_path as the path that was passed in as an arguement 
        #pdf_path = 
        
        # read_pdf returns a list of DataFrames in the PDF 
        df = tabula.read_pdf(self.pdf_path, pages='all')  

        # concatinates the tables together into a df      
        combined_df = pd.concat(df, ignore_index=True)                
        
        # returns the df 
        return combined_df 
            
  
    def list_number_of_stores(self):
        """
        This method gets the number of stores from the API endpoint  
        
        Args: 
            None

        Returns: 
            json object: Status code and the number of stores  
        """

        #so I know that the method is working 
        logging.info('list_number_of_stores is working')

        # make an instance of the DatabaseConnector() object 
        db_connector = DatabaseConnector() 

        # get the api key via the read_api_key method  
        headers = db_connector.headers

        #attempt to get the number of stores, with an exception block. Returns either the json response or the exception message 
        try: 
            with requests.get(self.no_stores_endpoint, headers=headers) as response: 
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
                #print(response.json())
                return response.json()
        #new http error logging here 
        except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error {http_err} has occurred - status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f'An error occurred: {e}')
            return None

    def retrieve_stores_data(self, no_stores):   
        """
        Gets the data about the stores from the database, it iterates through the number of stores and on each iteration requests the data from the API. 
        It relies on getting the API key from the DatabaseConnector object  
        
        Args: 
            None

        Returns: 
            dataframe: The store data is combined and returned in a dataframe  
        """
        
        logging.info('retrieve_stores_data is working')
    
        # make an instance of the DatabaseConnector() object
        db_connector = DatabaseConnector() 

        # get the API key via the read_api_key method 
        headers = db_connector.headers
        
        # try:
        #     if no_stores == int:
        #         no_stores = no_stores  
        #     else: 
        #         raise TypeError("no_stores must be an integer")
        # except Exception as e:
        #     logging.error(f"An unexpected error occur with no_stores: {e}")
        #     raise

        no_stores = int(no_stores)

        # creating an empty list for the store details to go into 
        store_data_list = []
        store_skipped_list = [] 

        # iterating through the list of stores and creating the specific store endpoint using the number of the store  
        for store_number in range (0, no_stores): # number of stores is 451 #THINK HERE IT CAN BE '451' THAT'S PASSED IN (0, {passed_in})
            store_suffix = str(store_number) 
            store_info_endpoint = f'{self.store_info_endpoint}{store_suffix}' 
            #store_info_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_suffix}'  

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
        logging.info("Skipped Stores DataFrame:")
        print(df_skipped)
        
        # returning the store data dataframe 
        return df_store


    def extract_from_s3(self, uri):

        """
        This method extract data from an S3 URI. It parses the uri into the bucket and key and loads it into a buffer using bytesIO. 
        It then converts either json or csv data into a data frame and returns it. 
        
        Args: 
            uri or the S3 resource 

        Returns: 
            dataframe: Either the csv or json from the S3 URI as a dataframe  
        """
        #so I know that the method is working 
        logging.info('extract_from_s3 is working')

        # setting the uri pas the uri passed into the method 
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

        # creating a boto3 client (analogous to creating a customer service representative going to 'talk' to the S3 dept for me)
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
        
        # return the df
        return df 





instance = DataExtractor()

#table_name = instance.read_data_from_table('legacy_users') 
#print(table_name)

#rds_table = instance.read_rds_table() 
#print(rds_table)

#pdf_data = instance.retrieve_pdf_data()     
#print(pdf_data)

#no_stores = instance.list_number_of_stores()
#print(no_stores)

retrieve_stores_data = instance.retrieve_stores_data(451) 
#print(retrieve_stores_data)

#extract = instance.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
#print(extract)




# OLD TESTING CODE 

#instance = DataExtractor()
#df = instance.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
#print(combined_df.head(5)) 

#example = instance.extract_from_s3('s3://data-handling-public/products.csv')
#print(example)