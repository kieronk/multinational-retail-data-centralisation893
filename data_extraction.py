from sqlalchemy import MetaData, Table
import pandas as pd
import tabula
import logging
import requests
from database_utils import DatabaseConnector

logging.basicConfig(filename='api_client.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class DataExtractor:
    def __init__(self):
        self.no_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        self.header = {
            'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }  

    def read_data_from_table(self, table_name): # this method gets the data from the table 
        
        #make an instance of the DatabaseConnector() object because I'm going to use some of the methods of that class
        db_connector = DatabaseConnector()  
        
        #make a database 'key' by using the method init_db_engine() from the instance of the DatabaseConnector object I've created
        engine = db_connector.init_db_engine() 
        
        # Create MetaData object, I need this so that I have a way to store and manage information about the database's structure  
        metadata = MetaData()

        # try to create a table object (a table object gives details about a particuar table. We pass in details about that object) 
            # table_name = what table to look at
            # metadata = helps the table object find the table
            # autoload_with = telling SQLAlchemy to use this specific connection to access the database and load the table’s details.
        try: 
            table = Table(table_name, metadata, autoload_with=engine)
        except Exception as e: 
            print(f"Error reflecting table {table_name}: {e}")
            return None
         
        # try to get data from table object I've just created  
        try: 
            with engine.connect() as connection: # creates a connection 
                select_query = table.select() # requests some data from the table 
                result_of_query = connection.execute(select_query) # this tells the connection to get the actual data (which is the result of 'table.select()) 
                data = result_of_query.fetchall() # takes all the results from connection.execute(select_query) and stores them in a variable so they can be worked with
                df = pd.DataFrame(data, columns=[column.name for column in table.columns]) # convert the result to a DataFrame
        except Exception as e: 
            print(f"Error making database connection / retrieving data {table_name}: {e}") 
            return None 

        # return a df of the data that comes back from that query  
        return df

    def read_rds_table(self): #gets the table names 
        
        # make an instance of the DatabaseConnection class 
        db_connector = DatabaseConnector()
        
        #uses list_db_tables to get the table names 
        name_of_table = db_connector.list_db_tables() 
        
        #makes a dataframe out of the names that have been retrieved from list_db_tables method 
        dataframe = pd.DataFrame(name_of_table) 

        # returns it 
        return dataframe

    def retrieve_pdf_data(self, pdf_path):         
        #pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf" 
        pdf_path = pdf_path
        df = tabula.read_pdf(pdf_path, pages='all')  # read_pdf returns list of DataFrames      
        combined_df = pd.concat(df, ignore_index=True)                
        return combined_df 
   
    def list_number_of_stores(self):
        try: 
            with requests.get(self.no_stores_endpoint, headers=self.header) as response: 
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
                print(response.json())
                return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f'An error occurred: {e}')
            return None

    def retrieve_stores_data(self):   
        all_data = []
        for i in range (0, 3): #451 
            #print('started')
            store_suffix = str(i) 
            store_info_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_suffix}'  
            #print(store_suffix)
            #print(store_info_endpoint)
            try: 
                with requests.get(store_info_endpoint, headers=self.header) as response:
                    response.raise_for_status()  # Check for HTTP errors
                    data = response.json()
                    all_data.append(response.json())
            except requests.exceptions.RequestException as e:
                print(f'An error occurred: {e}')
                return None
        df = pd.DataFrame(all_data) 
        print(df)
        return df 

instance = DataExtractor()
instance.list_number_of_stores()
instance.retrieve_stores_data() 

        

# code below is to test that it works 

# Create an instance of DataExtractor
#instance = DataExtractor()

#combined_df = instance.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

#print(combined_df.head(5)) 



"""
# Read data from the 'legacy_users' table
df = instance.read_data_from_table('legacy_users')
"""

