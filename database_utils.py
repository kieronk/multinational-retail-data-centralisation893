import logging
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.inspection import inspect

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DatabaseConnector: 
    """
    A utility class for managing database connections, including loading credentials, 
    creating SQLAlchemy engines for both local and remote (RDS) databases, listing tables, 
    and handling data uploads and deletions.

    Attributes:
        credentials (dict): A dictionary containing database credentials for both local ('DB') 
            and remote ('RDS') databases, with each set including driver, host, user, password, 
            database, and port information.
        
        api_key (str): The API key loaded from environment variables, required for external API requests.

        headers (dict): A dictionary containing HTTP headers, with the API key included as an 
            'x-api-key' entry for use in API requests.
    """
    
    def __init__(self):
        """
        Initializes the DatabaseConnector by loading environment variables for database 
        credentials and API keys, setting up HTTP headers, and validating required environment 
        variables.
        
        Raises:
            ValueError: If any required environment variables for database credentials or the API key are missing.
            Exception: For any other issues that arise during initialization.
        """
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
        
        Raises:
            ValueError: If an invalid prefix is provided.
        """
        # Get credentials based on the prefix
        creds = self.credentials.get(prefix)
    
        if not creds:
            raise ValueError(f"Invalid prefix '{prefix}'. Must be 'DB' or 'RDS'.")

        # Construct the database URL, including the driver and credentials 
        db_url = f"{creds['driver']}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

        # Print statement to verify the function is working (for testing/debugging)
        logging.info(f"init_db_engine is working for {prefix} database")

        # Create and return the SQLAlchemy engine
        engine = create_engine(db_url)
        return engine    

    def list_db_tables(self): 
        
        """
        Lists the tables in the specified database by using SQLAlchemy's inspector.

        Args:
            None 

        Returns:
            list: A list of table names in the specified database.   
        """
        
        # so I know that the method is working 
        logging.info('list_db_tables is working')
        
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
        Uploads a Pandas DataFrame to the specified database.

        Args:
            dataframe (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name to assign to the table in the database.

        Returns:
            None

        Raises:
            Exception: If an error occurs during upload.  
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
        Drops a specified table from the database.

        Args:
            engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the database.
            table_name (str): The name of the table to be dropped.

        Returns:
            None
        """
        
        with engine.connect() as connection:
            with connection.begin():
                drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                connection.execute(text(drop_sql))
                print(f"Table '{table_name}' dropped successfully.")
    
    def reset_database(self):
        """
        Resets the database by dropping all tables defined in the `tables_to_drop` list.

        Args:
            None
        
        Returns:
            None    
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

#Testing code 
instance = DatabaseConnector() 
list = instance.list_db_tables()
print(list)


