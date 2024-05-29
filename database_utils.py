import yaml
from sqlalchemy import create_engine
from sqlalchemy.inspection import inspect

class DatabaseConnector: 
    def _init__(self): 
        pass 

    def read_db_creds(self): 
        with open ('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/db_creds.yaml', 'r') as file: 
            db_creds = yaml.safe_load(file) #here safe load loads the content of the YAML file into a dictionary, and assigns it to a variable db_creds
        print('read_db_creds is working')
        return db_creds
    
    def init_db_engine(self):
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

  
example = DatabaseConnector() 
example.read_db_creds()
example.list_db_tables()


"""


creds = example.read_db_creds()
print(creds)

  def test_init_db_engine(self):
        my_test_instance = DatabaseConnector() 

    # Call the init_db_engine method
        engine = my_test_instance.init_db_engine()

    # Check if engine is not None
        assert engine is not None, "Engine object is None, initialization failed."

    # Optionally, you can check if the engine is of the expected type
    #assert isinstance(engine, create_engine), "Engine object is not of the expected type."

# Run the test


 def list_db_tables(self):
        # Get engine
        engine = self.init_db_engine()

        # Get list of table names
        table_names = engine.table_names()

        return table_names
        """