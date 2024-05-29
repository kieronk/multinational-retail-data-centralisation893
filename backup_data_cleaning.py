# %%
from sqlalchemy import MetaData, Table
import pandas as pd
from IPython.display import display
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

# %%
print('this is the unique country values:')
print(df['country'].unique()) 

print('this is unique country_codes:')
print(df['country_code'].unique()) 

# %%
print('this is the country values:')
print(df['country'].unique(), '\n') 

list_of_country_values = df['country'].unique()

print('this is my list of country values:\n', list_of_country_values, '\n')

# List of elements to exclude
exclude_list = ['Germany', 'United Kingdom', 'United States', 'NULL']

# Create a new list excluding the specified elements
list_of_values_to_drop = [item for item in list_of_country_values if item not in exclude_list]

print('this is my list of values to drop:', list_of_values_to_drop, '\n')

# Convert the list to a string that can be used in the query
query_str = 'country in @list_of_values_to_drop'

# Get the indices of the rows that match any value in my_list
list_of_indices = df.query(query_str).index.tolist()

print('this is my list of indices', list_of_indices, '\n')

cleaned_country_values_df = df.drop(index=list_of_indices)

print('unique values in original dataframe:')
print(df['country'].unique(), '\n') 

print('unique values in new dataframe:')
print(cleaned_country_values_df['country'].unique(), '\n')
#display(new_df.head(30)) 

# %%
#this is just checking text 
print('this is the new_df country values:')
print(cleaned_country_values_df['country'].unique()) 

print('this is new_df country_code:')
print(cleaned_country_values_df['country_code'].unique()) 

# %%
#this is correcting the GGB to GB 

print('this is the country_codes values before cleaning:', cleaned_country_values_df['country_code'].unique(), '\n') 

# Create a copy of the original dataframe
cleaned_country_values_and_codes_df = cleaned_country_values_df.copy()

# Replace 'GGB' with 'GB' in the 'country_code' column
cleaned_country_values_and_codes_df['country_code'] = cleaned_country_values_and_codes_df['country_code'].replace('GGB', 'GB')

print('ths is cleaned_country_values_df:', cleaned_country_values_df['country_code'].unique(), '\n') 

print('ths is cleaned_country_values__and_codes_df:', cleaned_country_values_and_codes_df['country_code'].unique(), '\n') 




# %%
# create a df of all null roles so that I can inspect them. (Because I want to know whether to drop them or not) 

list_of_null_rows = df.query('country == "NULL" or country_code == "NULL"').index.tolist()

print(list_of_null_rows)

null_rows_df = df.loc[list_of_null_rows]

#display(null_rows_df)

display(cleaned_country_values_and_codes_df)

#cleaned_country_values_and_codes_df = cleaned_country_values_and_codes_df.drop(index=[866, 1022, 1805, 2103, 2437, 2739, 2764, 4984, 5307, 6920, 7737, 10013, 10224, 10988, 11443, 11598, 11761, 11864, 12092, 12584, 13855])

cleaned_country_values_and_codes_df = cleaned_country_values_and_codes_df.drop(index=list_of_null_rows)

print('now after cleaning')
display(cleaned_country_values_and_codes_df)



# %%
#this is just checking text 
print('this is the new_df country values:')
print(cleaned_country_values_and_codes_df['country'].unique()) 

print('this is new_df country_code:')
print(cleaned_country_values_and_codes_df['country_code'].unique()) 

# %%
#cleaning date_of_birth
cleaned_date_df = cleaned_country_values_and_codes_df.copy()

# get reference from before the conversion so I can check if it works 
original_date_of_birth_example = cleaned_date_df.iloc[360]['date_of_birth']
print('original date_of_birth before pd.to_datetime():\n', original_date_of_birth_example)

# Convert the 'date_of_birth' column to datetime, handling various formats
cleaned_date_df['date_of_birth'] = pd.to_datetime(cleaned_date_df['date_of_birth'], errors='coerce', format='mixed')

# Check which dates could not be converted
invalid_dates = cleaned_date_df[cleaned_date_df['date_of_birth'].isna()]
print('number of rows that convert to NaT', len(invalid_dates)) 
display('these are the invalid dates:', invalid_dates)

# Check the conversion worked by printing an example of a difficult date

date_of_birth_converted_example = cleaned_date_df.iloc[360]['date_of_birth']
print('again, the original date_of_birth before pd.to_datetime():\n', original_date_of_birth_example)
print('date_of_birth after pd.to_datetime:\n', date_of_birth_converted_example)


# %%
#cleaning join_date 
cleaned_join_date_df = cleaned_date_df.copy()

#get a reference date to check it's worked 
original_join_date_example = cleaned_join_date_df.iloc[202]['join_date']
print('original join_date:\n', original_join_date_example)

# Convert the join_date column to datetime, handling various formats
cleaned_join_date_df['join_date'] = pd.to_datetime(cleaned_join_date_df['join_date'], errors='coerce', format='mixed') 

# Check which dates could not be converted
invalid_dates = cleaned_join_date_df[cleaned_join_date_df['join_date'].isna()]
print('number of rows that convert to NaT', len(invalid_dates)) 
display('these are the invalid dates:', invalid_dates)

# Check the conversion worked by printing an example of a difficult date
join_date_converted_example = cleaned_join_date_df.iloc[202]['join_date']
print('original join_date:\n', original_join_date_example)
print('join_date after pd.to_datetime:\n', join_date_converted_example)

# %%
#lower case text fields 
lower_case_df = cleaned_join_date_df.copy()

#getting an example 
before_lower_case_example = lower_case_df.iloc[0]['first_name']
print('this is the name before transformation:', before_lower_case_example)

#convert it 
lower_case_df['first_name'] = lower_case_df['first_name'].str.lower()
lower_case_df['last_name'] = lower_case_df['last_name'].str.lower()

# stripping whitespace 
lower_case_df['first_name'] = lower_case_df['first_name'].str.strip()
lower_case_df['last_name'] = lower_case_df['last_name'].str.strip()

#dropping duplicates 
print('row count before drop duplicates', len(lower_case_df)) 
lower_case_df = lower_case_df.drop_duplicates()
print('row count after drop duplicates', len(lower_case_df)) 

#cleaning special characters and punctuation from addresses 
#address_before_transformation = lower_case_df.iloc[0]['address']
#print('this is address before transformation', address_before_transformation)
#lower_case_df['address'] = lower_case_df['address'].str.replace('ß', 's')
#lower_case_df['address_converted'] = lower_case_df['address'].str.replace(r'[^a-zA-Z\s]', '', regex=True)
#address_after_transformation = lower_case_df.iloc[0]['address_converted']
#print('this is address after transformation', address_after_transformation)

#print before and after examples to check it worked 
after_lower_case_example = lower_case_df.iloc[0]['first_name']
print('again, this is the name before transformation:', before_lower_case_example)
print('this is the name after transformation:', after_lower_case_example)


