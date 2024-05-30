from sqlalchemy import MetaData, Table
import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):
        pass

    def read_data_from_table(self, table_name):
        db_connector = DatabaseConnector()  #make an instance of the DatabaseConnector() object because I'm going to use some of the methods of that class 
        engine = db_connector.init_db_engine() #make a database 'key' by using the method init_db_engine() from the instance of the DatabaseConnector object I've created
        
        # Create MetaData object, I need this so that I have a need a way to store and manage information about the database's structure. 
        # Without this catalog, it would be difficult to know how the data is organized and where to find it (it's like a list of all the what and where in the database) 
        metadata = MetaData()

        # A table object gives details about a particuar table. We pass in details about that object
        # table_name = what table to look at
        # metadata = helps the table object find the table 
        # autoload = tells SQLAlchemy to automatically load the table’s schema from the database
        # autoload_with = telling SQLAlchemy to use this specific connection to access the database and load the table’s details.
        try: 
            table = Table(table_name, metadata, autoload_with=engine)
        except Exception as e: 
            print(f"Error reflecting table {table_name}: {e}")
            return None
         
        try: 
            with engine.connect() as connection: # engine.connect() = creates a connection 
                select_query = table.select() # table.select() = requests some data from the table 
                result_of_query = connection.execute(select_query) # connection.execute(select_query) = this tells the connection to get the actual data (which is the result of 'table.select()) 
                data = result_of_query.fetchall() #result.fetchall() = takes all the results from connection.execute(select_query) and stores them in a vertiable so they can be worked with
                df = pd.DataFrame(data, columns=[column.name for column in table.columns]) # Convert the result to a DataFrame
        except Exception as e: 
            print(f"Error making database connection / retrieving data {table_name}: {e}") 
            return None 

        return df

    def read_rds_table(self):
        db_connector = DatabaseConnector()
        name_of_table = db_connector.list_db_tables() #uses list_db_tables to get the table names 
        dataframe = pd.DataFrame(name_of_table) #makes a dataframe out of the names that have been retrieved from list_db_tables method 
        return dataframe

# Create an instance of DataExtractor
instance = DataExtractor()

# Read data from the 'legacy_users' table
df = instance.read_data_from_table('legacy_users')

#display(df.head())  # Use display() function in Jupyter Notebook

df.head() 



"""


from sqlalchemy import MetaData, Table
import pandas as pd 
from database_utils import DatabaseConnector 

class DataExtractor:
    def __init__(self):
        # You can initialize any attributes here if needed
        pass

    def read_data_from_table(self, table_name):
        db_connector = DatabaseConnector()
        engine = db_connector.init_db_engine() 

        # Create MetaData object, 
        # I need this so that I have a need a way to store and manage information about the database's structure. 
        # Without this catalog, it would be difficult to know how the data is organized and where to find it (it's like a list of all the what and where in the database) 
        metadata = MetaData() 

        # A table object gives details about a particuar table. We pass in details about that object
        # table_name = what table to look at
        # metadata = helps the table object find the table 
        # autoload = tells SQLAlchemy to automatically load the table’s schema from the database
        # autoload_with = telling SQLAlchemy to use this specific connection to access the database and load the table’s details.
        try: 
            table = Table(table_name, metadata, autoload_with=engine)
        except Exception as e: 
            print(f"Error reflecting table {table_name}: {e}")
            return None

        # engine.connect() = creates a connection 
        # table.select() = requests some data from the table 
        # connection.execute(select_query) = this tells the connection to get the actual data (which is the result of 'table.select()) 
        #result.fetchall() = takes all the results from connection.execute(select_query) and stores them in a vertiable so they can be worked with 
        try: 
            with engine.connect() as connection:
                select_query = table.select()
                result_of_query = connection.execute(select_query)
                data = result_of_query.fetchall()
        except Exception as e: 
            print(f"Error making database connection / retrieving data {table_name}: {e}") 
            return None 

        return data
    

    def read_rds_table(self, table_name): 
        db_connector = DatabaseConnector()
        name_of_table = db_connector.list_db_tables() #uses list_db_tables to get the table names 
        dataframe = pd.DataFrame(name_of_table) #makes a dataframe out of the names that have been retrieved from list_db_tables method 
        return dataframe


instance = DataExtractor() 
dataframe = instance.read_rds_table('legacy_users')
#dataframe.info() 
dataframe.head(5) 

exp = instance.read_data_from_table('legacy_users') 
dataframe = instance.read_rds_table('legacy_users')

print(exp)
print(dataframe)
"""