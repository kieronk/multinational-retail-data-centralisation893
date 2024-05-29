from sqlalchemy import MetaData, Table
import pandas as pd
from IPython.display import display
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning: 
    def __init__ (self):
        pass 

    #def clean_data(self):
       
    def clean_country_values(self):
        # Create an instance of DataExtractor
        print('started clean_country_values method...')
        instance = DataExtractor()

        # Read data from the 'legacy_users' table
        df = instance.read_data_from_table('legacy_users')  
        #print(df.head)       
        
        #print('this is the country values:')
        #print(df['country'].unique(), '\n') 

        list_of_country_values = df['country'].unique()

        #print('this is my list of country values:\n', list_of_country_values, '\n')

        # List of elements to exclude
        exclude_list = ['Germany', 'United Kingdom', 'United States', 'NULL']

        # Create a new list excluding the specified elements
        list_of_values_to_drop = [item for item in list_of_country_values if item not in exclude_list]

        #print('this is my list of values to drop:', list_of_values_to_drop, '\n')

        # Convert the list to a string that can be used in the query
        query_str = 'country in @list_of_values_to_drop'

        # Get the indices of the rows that match any value in my_list
        list_of_indices = df.query(query_str).index.tolist()

        #print('this is my list of indices', list_of_indices, '\n')

        cleaned_country_values_df = df.drop(index=list_of_indices)

        print('unique values in original dataframe:')
        print(df['country'].unique(), '\n') 

        print('unique values in new dataframe:')
        print(cleaned_country_values_df['country'].unique(), '\n')
        #display(new_df.head(30)) 
        
        print('completed clean_country_values method about to return clean_country_values_df..')
        return cleaned_country_values_df


    def clean_country_codes(self): #this is correcting the GGB to GB
        
        #creating instance of datacleaning so that I can get the df from the clean_country_values function          
        #datacleaning_instance = DataCleaning() 
        #cleaned_country_values_df = datacleaning_instance.clean_country_values() 
        
        #running clean_country_values to get the df
        print('started clean_country_codes method...')
        cleaned_country_values_df = self.clean_country_values() 
        #print('run cleaned_country_values_df = self.clean_country_values()...')
        #print('this is the country_codes values before cleaning:', cleaned_country_values_df['country_code'].unique(), '\n') 

        # Create a copy of the original dataframe
        cleaned_country_values_and_codes_df = cleaned_country_values_df.copy()

        # Replace 'GGB' with 'GB' in the 'country_code' column
        cleaned_country_values_and_codes_df['country_code'] = cleaned_country_values_and_codes_df['country_code'].replace('GGB', 'GB')

        print('ths is cleaned_country_values_df:', cleaned_country_values_df['country_code'].unique(), '\n') 

        print('ths is cleaned_country_values__and_codes_df:', cleaned_country_values_and_codes_df['country_code'].unique(), '\n') 

        return cleaned_country_values_and_codes_df


    def drop_null_values(self):

        print('started drop_null_values method...')
        cleaned_country_values_and_codes_df = self.clean_country_codes() 

        # create a df of all null roles so that I can inspect them. (Because I want to know whether to drop them or not) 

        list_of_null_rows = cleaned_country_values_and_codes_df.query('country == "NULL" or country_code == "NULL"').index.tolist()

        #print(list_of_null_rows)
        #print('length of null rows:', len(list_of_null_rows))
        #print('before dropping rows:', len(cleaned_country_values_and_codes_df))

        #null_rows_df = cleaned_country_values_and_codes_df.loc[list_of_null_rows]

        #display(null_rows_df)

        #display(cleaned_country_values_and_codes_df)

        #cleaned_country_values_and_codes_df = cleaned_country_values_and_codes_df.drop(index=[866, 1022, 1805, 2103, 2437, 2739, 2764, 4984, 5307, 6920, 7737, 10013, 10224, 10988, 11443, 11598, 11761, 11864, 12092, 12584, 13855])

        cleaned_country_values_and_codes_df = cleaned_country_values_and_codes_df.drop(index=list_of_null_rows)

        print('now after cleaning')
        display(cleaned_country_values_and_codes_df)
        print('after dropping rows:', len(cleaned_country_values_and_codes_df))

datacleaning_instance = DataCleaning() 

datacleaning_instance.drop_null_values() 



"""

# %%




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


"""