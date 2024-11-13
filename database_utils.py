import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.inspection import inspect

class DatabaseConnector: 
    """
    This class contains the utilities methods, including getting credentials, uploading data to database and creating SQLalchemy engines.    

    Attributes: 
        None 
    
    """
    
    def __init__(self):
        try:
            # Load environment variables from .env file
            load_dotenv()

            # Database credentials
            self.credentials = {
                'DB': {
                    'driver': 'postgresql+psycopg2',  # Local database driver
                    'host': os.getenv('DB_HOST'),
                    'user': os.getenv('DB_USER'),
                    'password': os.getenv('DB_PASSWORD'),
                    'database': os.getenv('DB_DATABASE'),
                    'port': os.getenv('DB_PORT')
                },
                'RDS': {
                    'driver': 'postgresql',  # RDS database driver
                    'host': os.getenv('RDS_HOST'),
                    'user': os.getenv('RDS_USER'),
                    'password': os.getenv('RDS_PASSWORD'),
                    'database': os.getenv('RDS_DATABASE'),
                    'port': os.getenv('RDS_PORT')
                }
            }

            # API key setup
            self.api_key = os.getenv('API_KEY')
            if not self.api_key:
                raise ValueError("API key not found in environment variables")

            # Store headers
            self.headers = {'x-api-key': self.api_key}

            # Check for missing DB credentials
            for db_type, creds in self.credentials.items():
                if not all(creds.values()):
                    raise ValueError(f"Missing one or more required {db_type} database environment variables")

        except ValueError as ve:
            print(f"Error loading environment variables: {ve}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    def init_db_engine(self, prefix="DB"):   
        
        """
        Creates an SQLAlchemy engine for the specified database type.

        Args:
            prefix (str): Either 'DB' for the local database or 'RDS' for the remote database.

        Returns:
            engine: A SQLAlchemy engine connected to the specified database.
        """
        # Get credentials based on the prefix
        creds = self.credentials.get(prefix)
    
        if not creds:
            raise ValueError(f"Invalid prefix '{prefix}'. Must be 'DB' or 'RDS'.")

        # Construct the database URL, including the driver and credentials 
        db_url = f"{creds['driver']}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

        # Print statement to verify the function is working (for testing/debugging)
        print(f"init_db_engine is working for {prefix} database")

        # Create and return the SQLAlchemy engine
        engine = create_engine(db_url)
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
        #engine = self.init_db_engine() PREFIX DB 
        engine = self.init_db_engine(prefix="RDS")
    
        #creating an inspector object, which is like a librarian who looks up things in the library (i.e. database). I pass it the 'engine' as it needs a engine to 'power' and manage it's connection to the database 
        inspector = inspect(engine) 

        # getting the tables name using the get_table_names method of the inspector object from SQLAlchemy 
        table_names = inspector.get_table_names()
        
        # returning the list of table names 
        return table_names

    def upload_to_db(self, dataframe, table_name):
        """
        This method uploads dataframes to my database 

        Args: 
            dataframe: the dataframe to be uploaded 
            table_name: the name that you want given to the table when it's uploaded 

        Returns: 
            Prints a message to indicate whether the upload has been successful or raises an error if it hasn't  
        """

        # so I know that the method is working 
        print('upload_to_db is working')

        # run the init_my_db_engine method to get an engine for my database running 
        #engine = self.init_my_db_engine()
        engine = self.init_db_engine(prefix="DB")

        try:
            # Upload the dataframe to the database
            dataframe.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(f"Table '{table_name}' uploaded successfully.")

        except Exception as e:
            print(f"An error occurred while uploading the table: {e}")

    def drop_table(self, engine, table_name):
        """
        This method drops specified tables from the database 

        Args: 
            engine: SQL engine  
            table_name: the name that you want dropped 

        Returns: 
            Prints a message to indicate the table has been dropped successfully 
        """
        
        with engine.connect() as connection:
            with connection.begin():
                drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                connection.execute(text(drop_sql))
                print(f"Table '{table_name}' dropped successfully.")
    
    def reset_database(self):
        """
        This method drops tables from the database in order to 'reset' it, using the drop_table method   

        Args: 
            None
        
        Returns: 
            A print message indicatoring that the tables have been dropped successfully    
        """
        
        # Initialize the engine
        engine = self.init_db_engine(prefix="DB")

        # Drop all tables
        tables_to_drop = [
            'orders_table',
            'dim_products',
            'dim_users',
            'dim_store_details',
            'dim_date_times',
            'dim_card_details'
        ]

        for table in tables_to_drop:
            self.drop_table(engine, table)


connector = DatabaseConnector() 
temp = connector.list_db_tables() 
print(temp)
