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


    def read_my_db_creds(self): # gets the credentails for my local db 
        with open ('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/my_db_creds.yaml', 'r') as file: 
            my_db_creds = yaml.safe_load(file)
        print('my local db creds is working')
        return my_db_creds
    
    def init_my_db_engine(self): # this creates the engine (the key) to use with the my local database  
        my_db_creds = self.read_my_db_creds() #this gets the credentials by using the read_db_creds 
        my_db_url = f"postgresql+psycopg2://{my_db_creds['USER']}:{my_db_creds['PASSWORD']}@{my_db_creds['HOST']}:{my_db_creds['PORT']}/{my_db_creds['DATABASE']}" #this makes a URL to connect to the database using the credentials 

        # Create the engine
        engine = create_engine(my_db_url)       

        # Test the connection
        try:
            with engine.connect() as connection:
                print("Connection to the PostgreSQL database was successful!")
        except Exception as e:
            print(f"An error occurred: {e}")

        return engine 

    def upload_to_db(self, dataframe, table_name):
        
        engine = self.init_my_db_engine() 

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
# example_df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})




