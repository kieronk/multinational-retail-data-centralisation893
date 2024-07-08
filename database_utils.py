import yaml
import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy.inspection import inspect

class DatabaseConnector: 
    """
    This class contains the utilities methods, including getting credentials, uploading data to database and creating SQLalchemy engines.    

    Attributes: 
        None 
    
    """
    
    def _init__(self): 
        pass 

    def read_db_creds(self): 
        
        """
        This method reads the db credentials from a YAML file  
        
        Args: 
            None 

        Returns: 
            dictionary: a dictionary of the database credentials 
        """
        
        # so I know that the method is working 
        print('read_db_creds is working')

        # use a context manager to open the YAML file and read the database credentials 
        with open ('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/db_creds.yaml', 'r') as file: 
            db_creds = yaml.safe_load(file) #here safe load loads the content of the YAML file into a dictionary, and assigns it to a variable db_creds
        
        # return the credentials from the YAML file 
        return db_creds
    
    def read_my_db_creds(self): 
        
        """
        This method reads and returns the database credentials for my database from a YAML file 
         
        Args: 
            None 

        Returns: 
            dictionary: a dictionary of my database credentials    
        """
        
        # so I know that the method is working 
        print('read_my_db_creds is working')
        
        # using a context manager to open 'my_db_creds' which are the credentials to my postgres database 
        with open ('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/my_db_creds.yaml', 'r') as file: 
            my_db_creds = yaml.safe_load(file)
        
        # returns the credentials 
        return my_db_creds

    def read_api_key(self): 
        """
        This method reads the API key from a YAML file, for use with other methods that retrieve data from AWS   
        
        Args: 
            None

        Returns: 
            dictionary: containing the API key
        """
        
        # so I know that the method is working 
        print('read_api_key is working')

        # this uses a context manager to read the api key from a yaml file 
        with open ('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/api_key.yaml', 'r') as file: 
            config = yaml.safe_load(file) # safe load loads the content of the YAML file into a dictionary, and assigns it to a variable header   
            #print(config)
        # Extract the API key and create the header
        api_key = config.get('api_key')
        if not api_key:
            raise ValueError("API key not found in the YAML file")
        
        headers = {'x-api-key': api_key}
        
        # return the headers for use in other methods 
        return headers 
    

    def init_db_engine(self): 
        
        """
        This method creates the engine to use with the db 
        
        Args: 
            None 

        Returns: 
            engine: a SQLAlchemy engine  
        """
        
        # so I know that the method is working 
        print('init_db_engine is working')
        
        # this gets the credentials by using the read_db_creds 
        creds = self.read_db_creds() 
        
        # this makes a URL to connect to the database using the credentials 
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}" 

        # this created an engine using 'create_engine()' 
        engine = create_engine(db_url)

        # returns the engine so that it can be used by other methods 
        return engine 

    def list_db_tables(self): 
        
        """
        This method lists the tables in the database 
        
        Args: 
            None 

        Returns: 
            list: a list of the table names in the database   
        """
        
        # so I know that the method is working 
        print('list_db_tables is working')
        
        #creates a engine to connect to the database by using the 'init_db_engine()' method  
        engine = self.init_db_engine() 
    
        #creating an inspector object, which is like a librarian who looks up things in the library (i.e. database). I pass it the 'engine' as it needs a engine to 'power' and manage it's connection to the database 
        inspector = inspect(engine) 

        # getting the tables name using the get_table_names method of the inspector object from SQLAlchemy 
        table_names = inspector.get_table_names()
        
        # returning the list of table names 
        return table_names

    
    def init_my_db_engine(self):   
        
        """
        This method this creates the engine (the thing that manages the connection) for my database
        
        Args: 
            None 

        Returns: 
            engine: the engine for my database   
        """
        
        # so I know that the method is working 
        print('init_my_db_engine is working')
        
        # this gets my database credentials by using the read_db_creds 
        my_db_creds = self.read_my_db_creds()  
        
        #this makes a URL to connect to the database using the credentials 
        my_db_url = f"postgresql+psycopg2://{my_db_creds['USER']}:{my_db_creds['PASSWORD']}@{my_db_creds['HOST']}:{my_db_creds['PORT']}/{my_db_creds['DATABASE']}" 

        # creates the engine using the url with the credentials in it 
        engine = create_engine(my_db_url)       

        # Test the connection
        try:
            with engine.connect() as connection:
                print("Connection to the PostgreSQL database was successful!")
        except Exception as e:
            print(f"An error occurred: {e}")

        #return the engine for use with other functions 
        return engine 

    def upload_to_db(self, dataframe, table_name):
        
        """
        This method uploads dataframes to my database 
        
        Args: 
            dataframe: the dataframe to be uploads 
            table_name: the name that you want given to table when it's uploaded 

        Returns: 
            Prints a message to indicate whether the upload has been successful or raises and error if it hasn't  
        """
        
        # so I know that the method is working 
        print('upload_to_db is working')

        # run the init_my_db_engine method to get an engine for my database running 
        engine = self.init_my_db_engine() 

        #try to write the dataframe to my sql database, and raise an exception if it fails 
            # name = the name of the table where the df will written to
            # con = the db connection object 
            # if_exists = replace if exist already 
            # index_false = don't write the df index as a seperate column in the SQL tabel 
        try:
            dataframe.to_sql(name=table_name, con=engine, if_exists='replace', index=False) 
            print(f"Table '{table_name}' uploaded successfully.")
        except Exception as e:
            print(f"An error occurred while uploading the table: {e}")





# test if code works   
#example = DatabaseConnector() 
#example.read_db_creds()
#example.list_db_tables()
#example.read_my_db_creds() 
#example.read_api_key() 


