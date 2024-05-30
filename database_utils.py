import yaml
import pandas as pd 
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy.inspection import inspect

class DatabaseConnector: 
    def _init__(self): 
        pass 

    def read_db_creds(self): # read the db credentials 
        with open ('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/db_creds.yaml', 'r') as file: 
            db_creds = yaml.safe_load(file) #here safe load loads the content of the YAML file into a dictionary, and assigns it to a variable db_creds
        print('read_db_creds is working')
        return db_creds
    
    def init_db_engine(self): # this creates the engine (the key) to use with the db 
        creds = self.read_db_creds() #this gets the credentials by using the read_db_creds 
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}" #this makes a URL to connect to the database using the credentials 

        engine = create_engine(db_url)
        print('init_db_engine is working')
        return engine 

    def list_db_tables(self): 
        engine = self.init_db_engine() #creates a bridge in to the database using 'engine' (engine manages connections 'keymaster'). Does this by using the method 'init_db_engine()'  
    
        inspector = inspect(engine) #creating an inspector object, which is like a librarian who looks up things in the library (i.e. database). I pass it the 'engine' as it needs the 'key' 

        table_names = inspector.get_table_names()
        print('list_db_tables is working')
        print(table_names)
        return table_names


    def upload_to_db(self, dataframe, table_name):
        """
        Uploads a Pandas DataFrame to the database as a new table.

        :param dataframe: Pandas DataFrame to be uploaded.
        :param table_name: Name of the table to be created in the database.
        """
        engine = self.init_db_engine() 

        try:
            dataframe.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(f"Table '{table_name}' uploaded successfully.")
        except Exception as e:
            print(f"An error occurred while uploading the table: {e}")

# test if code works   
example = DatabaseConnector() 
example.read_db_creds()
example.list_db_tables()

# next stage is to try and get the upload_to_db function working.
# I'm using this example dataframe to test it 

example_df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})


example.upload_to_db(example_df, 'example_table')

