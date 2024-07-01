from sqlalchemy import MetaData, Table
import pandas as pd
import logging 
import numpy as np
import re 
from IPython.display import display
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DataCleaning: 
    def __init__ (self):
        self.logger = logging.getLogger(self.__class__.__name__)
        instance = DataExtractor() 
       
    def clean_country_names(self): #this 
        
        #log starting of method
        print('started clean_country_names method...')

        # Create an instance of DataExtractor
        instance = DataExtractor()

        # Read data from the 'legacy_users' table
        df = instance.read_data_from_table('legacy_users')  
                
        # create a list of the unique country values in the df 
        list_of_country_names = df['country'].unique()

        # create a list of values to exlude to exclude
        exclude_list = ['Germany', 'United Kingdom', 'United States', 'NULL']

        # Create a new list excluding the specified elements
        list_of_names_to_drop = [item for item in list_of_country_names if item not in exclude_list]

        # Convert the list to a string that can be used in the query
        query_str = 'country in @list_of_names_to_drop'

        # Get the indices of the rows that match any value in my_list and store them in a list 
        list_of_indices = df.query(query_str).index.tolist()

        # create a new dataframe, which is copy of the dataframe with the unwanted rows dropped 
        cleaned_country_names_df = df.drop(index=list_of_indices)
        
        #log whether the method has run correctly 
        #print('unique values in original dataframe:')
        #print(df['country'].unique(), '\n') 
        #print('unique values in new dataframe:')
        #print(cleaned_country_names_df['country'].unique(), '\n')  

        #log that the method has run 
        print('clean_country_names method has run... \n')
        
        #return the cleaned df 
        return cleaned_country_names_df


    def clean_country_codes(self): #this is correcting the GGB in country codes to GB
        
        # log that the clean_country_codes method has started 
        print('started clean_country_codes method...')
        
        # running clean_country_names method to get the df
        cleaned_country_names_df = self.clean_country_names() 
        
        # create a copy of the clean_country_names dataframe
        cleaned_country_names_codes_df = cleaned_country_names_df.copy()

        # replace 'GGB' with 'GB' in the 'country_code' column of the cleaned_country_names_and_codes_df
        cleaned_country_names_codes_df['country_code'] = cleaned_country_names_codes_df['country_code'].replace('GGB', 'GB')

        # log the country_codes before and after cleaning to check that it's worked 
        #print('ths is cleaned_country_names_df:', cleaned_country_names_df['country_code'].unique(), '\n') 
        #print('ths is cleaned_country_names__and_codes_df:', cleaned_country_names_codes_df['country_code'].unique(), '\n') 

        #log that the method has run 
        print('clean_country_codes method has run... \n')

        #return the cleaned df 
        return cleaned_country_names_codes_df


    def drop_null_values_and_duplicates(self): # this drops the rows with NULL values and duplicate rows 

        # log that the method has started 
        print('started drop_null_values_and_duplicates method...')
        
        # create a copy of the cleaned df
        cleaned_country_names_codes_null_duplicates_df = self.clean_country_codes() 

        # create a list of all null roles  
        list_of_null_rows = cleaned_country_names_codes_null_duplicates_df.query('country == "NULL" or country_code == "NULL"').index.tolist()

        # log the number of null rows
        #print('length of null rows:', len(list_of_null_rows))
        
        # log the number of rows in the df before dropping rows 
        #print('before dropping rows:', len(cleaned_country_names_codes_null_duplicates_df))
 
        # drop the rows with null values 
        cleaned_country_names_codes_null_duplicates_df = cleaned_country_names_codes_null_duplicates_df.drop(index=list_of_null_rows)
        
        # log the number of rows in the df after dropping rows 
        #print('after dropping rows:', len(cleaned_country_names_codes_null_duplicates_df))

        #dropping duplicates 
        #print('row count before drop duplicates', len(cleaned_country_names_codes_null_duplicates_df)) 
        #cleaned_country_names_codes_null_duplicates_df = cleaned_country_names_codes_null_duplicates_df.drop_duplicates()
        #print('row count after drop duplicates', len(cleaned_country_names_codes_null_duplicates_df)) 

        #log that the method has run 
        print('drop_null_values_and_duplicates method has run... \n')

        # return the cleaned df 
        return cleaned_country_names_codes_null_duplicates_df
    
    def clean_dob(self): #cleaning date_of_birth
        
        #log starting of method
        print('started clean_dob method...')

        # get the cleaned df
        cleaned_country_names_codes_null_duplicates_dob_df = self.drop_null_values_and_duplicates()

        # get an example from before the conversion so I can check if it works (I know the row indexed by 360 would need converting)
        original_date_of_birth_example = cleaned_country_names_codes_null_duplicates_dob_df.iloc[360]['date_of_birth']
        
        # log before the conversion so that I can check it works 
        #print('original date_of_birth before pd.to_datetime():\n', original_date_of_birth_example)

        # convert the 'date_of_birth' column to datetime, handling the different formats
        cleaned_country_names_codes_null_duplicates_dob_df['date_of_birth'] = pd.to_datetime(cleaned_country_names_codes_null_duplicates_dob_df['date_of_birth'], errors='coerce', format='mixed')

        # Check which dates could not be converted
        invalid_dates = cleaned_country_names_codes_null_duplicates_dob_df[cleaned_country_names_codes_null_duplicates_dob_df['date_of_birth'].isna()]
        
        # log the number of rows that couldn't be converted in case of errors  
        #print('number of rows that convert to NaT', len(invalid_dates)) 

        # log examples to check the conversion worked by printing an example of a difficult date
        #date_of_birth_converted_example = cleaned_country_names_codes_null_duplicates_dob_df.iloc[360]['date_of_birth']
        #print('again, the original date_of_birth before pd.to_datetime():\n', original_date_of_birth_example)
        #print('date_of_birth after pd.to_datetime:\n', date_of_birth_converted_example)

        #log that the method has run 
        print('clean_dob method has run... \n')

        # return cleaned df 
        return cleaned_country_names_codes_null_duplicates_dob_df


    def clean_join_date(self): #cleaning join_date 
        
        #log starting of method
        print('started clean_join_date method...')

        # getting cleaned df 
        cleaned_country_names_codes_null_duplicates_dob_jd_df = self.clean_dob()

        # get an example from before the conversion so I can check if it works (I know the row indexed by 202 would need converting)
        original_join_date_example = cleaned_country_names_codes_null_duplicates_dob_jd_df.iloc[202]['join_date']

        # Convert the join_date column to datetime, handling various formats
        cleaned_country_names_codes_null_duplicates_dob_jd_df['join_date'] = pd.to_datetime(cleaned_country_names_codes_null_duplicates_dob_jd_df['join_date'], errors='coerce', format='mixed') 

        # Check which dates could not be converted
        invalid_dates = cleaned_country_names_codes_null_duplicates_dob_jd_df[cleaned_country_names_codes_null_duplicates_dob_jd_df['join_date'].isna()]
        
        # log the number of rows that couldn't be converted in case of errors 
        print('number of rows that convert to NaT', len(invalid_dates)) 

        # Check the conversion worked by printing an example of a difficult date
        #join_date_converted_example = cleaned_country_names_codes_null_duplicates_dob_jd_df.iloc[202]['join_date']
        #print('original join_date:\n', original_join_date_example)
        #print('join_date after pd.to_datetime:\n', join_date_converted_example)

        #log that the method has run 
        print('clean_join_date method has run... \n')

        return cleaned_country_names_codes_null_duplicates_dob_jd_df

    def cleaning_text_fields(self): # clean the text: lower case,  
        
        #log starting of method
        print('started clean_text_fields method...')

        # get the cleaned df  
        cleaned_country_names_codes_null_duplicates_dob_jd_text_df = self.clean_join_date()

        # get an example from before the conversion so I can check if it works (can be any index as they all need converting)
        #before_lower_case_example = cleaned_country_names_codes_null_duplicates_dob_jd_text_df.iloc[0]['first_name']
        #print('this is the name before transformation:', before_lower_case_example)

        #convert first_name and last_name  
        cleaned_country_names_codes_null_duplicates_dob_jd_text_df['first_name'] = cleaned_country_names_codes_null_duplicates_dob_jd_text_df['first_name'].str.lower()
        cleaned_country_names_codes_null_duplicates_dob_jd_text_df['last_name'] = cleaned_country_names_codes_null_duplicates_dob_jd_text_df['last_name'].str.lower()

        # log that it's worked 
        #after_lower_case_example = cleaned_country_names_codes_null_duplicates_dob_jd_text_df.iloc[0]['first_name']
        #print('this is the name after transformation:', after_lower_case_example)

        # strip whitespace from first_name and last_name fields
        cleaned_country_names_codes_null_duplicates_dob_jd_text_df['first_name'] = cleaned_country_names_codes_null_duplicates_dob_jd_text_df['first_name'].str.strip()
        cleaned_country_names_codes_null_duplicates_dob_jd_text_df['last_name'] = cleaned_country_names_codes_null_duplicates_dob_jd_text_df['last_name'].str.strip()

        #log that the method has run 
        print('clean_text_fields method has run... \n')

        cleaned_user_details_df = cleaned_country_names_codes_null_duplicates_dob_jd_text_df
        # return the cleaned df 
        return cleaned_user_details_df

    def clean_card_data(self):  
        
    # gettting the card data to clean 

        print('Started clean_card_data method. Running retrieve_pdf_data to get the data')
        # Create an instance of DataExtractor
        instance = DataExtractor()

        # Read data from the 'legacy_users' table
        df = instance.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

        #log starting of method
        #self.logger.info(f"Running retrieve_pdf_data with pdf_path: {pdf_path}")
        

    #cleaning expiry date column 
        print('started cleaning expiry date column')
        # Define a regular expression pattern for the MM/YY format
        pattern = r'^\d{2}/\d{2}$'

        # Use str.match to filter rows where 'expiry_date' matches the pattern
        df = df[df['expiry_date'].str.match(pattern)]

        # Reset the index 
        df = df.reset_index(drop=True)

    #adding datetime version of expiry date for calculations 

        print('started datetime conversion for expiry_date')

        df['datetime_expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')

    # removing incorrect values from the card providers column 
        
        print('started removing incorrect values from card providers column')

        #create a valid providers list  
        valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit','JCB 15 digit', 'Maestro', 'Mastercard', 'Discover','VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        # Filter the DataFrame to keep only rows with valid card providers
        df = df[df['card_provider'].isin(valid_providers)]

        #Reset the index of the filtered DataFrame
        df = df.reset_index(drop=True)

        cleaned_card_details_df = df 
        return cleaned_card_details_df

    def cleaning_store_details(self):
        
        #creating instance of dataextractor 
        instance = DataExtractor()
        
        #retrieving the data from the stores API
        df = instance.retrieve_stores_data()  
        
        #dropping rows with NA and 'longitude' column as it's not useful without latitude  
        df = df.dropna(axis=1)
        df = df.drop(columns=['longitude'])
        
        # filtering out items in locality that aren't real place names or NULL 
        pattern = r'^[a-zA-Z\s-]+$'
        df = df[df['locality'].str.match(pattern)]
        df['locality'] = df['locality'].replace('NULL', np.nan)
        df = df.dropna(subset=['locality'])
        
        # replacing incorrect spellings of continents 
        continent_replacements = {
            'eeEurope': 'Europe',
            'eeAmerica': 'America'
        }

        df['continent'] = df['continent'].replace(continent_replacements)
        
        #checking everything has worked 
        print(df['continent'].unique())
        print(df['store_type'].unique()) 
        print(df['country_code'].unique()) 
        print(df['continent'].unique())

        # converting opening date to datetime object 
        is_datetime_before = pd.api.types.is_datetime64_any_dtype(df['opening_date'])
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='%Y-%m-%d', errors='coerce')
        is_datetime_after = pd.api.types.is_datetime64_any_dtype(df['opening_date'])
        print(f"Is 'dates' column datetime64 dtype? {is_datetime_before}")
        print(f"Is 'dates' column datetime64 dtype? {is_datetime_after}")

        #returning the dataframe 
        return df 


    def convert_weights_to_kg(self):
        
        #creating instance of dataextractor 
        instance = DataExtractor()
            
        #retrieving the data from the stores API
        df = instance.extract_from_s3('s3://data-handling-public/products.csv') 
                
        # Define the regex pattern to match numbers and letters
        pattern = re.compile(r'([0-9.]+)([a-zA-Z]+)')

        # Conversion factors to kg
        conversion_factors = {
            'kg': 1,
            'g': 0.001,
            'oz': 0.0283495231,
            'ml': 0.001  # Assuming ml is equivalent to grams for water-based products
        }

        def convert_to_kg(weight):
            if pd.isna(weight):
                return None  # Handle NaN values
            match = pattern.match(str(weight))
            if match:
                number = float(match.group(1))  # Extract the numeric part
                unit = match.group(2).lower()  # Extract the unit part
                return number * conversion_factors.get(unit, 0)  # Convert to kg
            return None  # Handle cases where regex does not match
     
        # Apply the conversion to the 'weights' column
        df['weight_in_kg'] = df['weight'].apply(convert_to_kg) # self.df['weight_in_kg'] = self.df['weights'].apply(convert_to_kg)

        return df 

    def clean_orders_data(self):
        print('started clean_orders_data')
        instance = DataExtractor()
        df = instance.read_data_from_table('orders_table')
        df = df.drop('1', axis=1)
        df = df.drop('first_name', axis=1)
        df = df.drop('last_name', axis=1)
        print('about to return df clean_orders_data')
        print(df)
        return df 

    def clean_date_events(self):
        instance = DataExtractor()
        df = instance.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        year_regex = r'^\d{4}$'
        df = df[df['year'].str.match(year_regex)]
        return df 



# TESTING / CALLING CODE 

# creating data cleaning instance needed for running the methods in this class 
datacleaning_instance = DataCleaning() 

# creating database connector instance needed for running the methods in database_utils file  
databaseconnector_instance = DatabaseConnector() 

# cleaning the text fields in the legacy users table 
#cleaned_user_details_df = datacleaning_instance.cleaning_text_fields() 

#cleaning the card data from the PDF link 
#cleaned_card_details_df = datacleaning_instance.clean_card_data()

#uploading the cleaned legacy users table to the SQL database 
#databaseconnector_instance.upload_to_db(cleaned_user_details_df, 'dim_users')

#uploading the cleaned table from the PDF link to the SQL database 
#databaseconnector_instance.upload_to_db(cleaned_card_details_df, 'dim_card_details')

# fetching and cleanming the store data via the API 
#cleaned_store_info_df = datacleaning_instance.cleaning_store_details()

#uploading the cleaned store data from the API to the SQL database 
#databaseconnector_instance.upload_to_db(cleaned_store_info_df, 'dim_store_details')

#fetching and cleaning weight conversion on product details 
#cleaned_product_details_df = datacleaning_instance.convert_weights_to_kg() 

#uploading the cleaned product details from S3 to the SQL database 
#databaseconnector_instance.upload_to_db(cleaned_product_details_df, 'dim_products')

#fetching and cleaning order details  
#cleaned_orders_df = datacleaning_instance.clean_orders_data()

#uploading the cleaned order details to the SQL database 
#databaseconnector_instance.upload_to_db(cleaned_orders_df, 'orders_table')

#uploading the cleaned date events to the SQL database 
cleaned_date_events_df = datacleaning_instance.clean_date_events()

#uploading the cleaned order details to the SQL database 
databaseconnector_instance.upload_to_db(cleaned_date_events_df, 'dim_date_times')


"""
import yaml

with open('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/my_db_creds.yaml', 'r') as file_1:
    creds_1 = yaml.safe_load(file_1)

print(creds_1)  # Ensure this prints the updated credentials

"""

"""
# code below is to test that it works 

datacleaning_instance = DataCleaning() 

datacleaning_instance.clean_card_data()

"""

"""
# code below is to test that it works 

datacleaning_instance = DataCleaning() 

cleaned_df_all = datacleaning_instance.cleaning_text_fields() 

data_utils_instance = DatabaseConnector() 

data_utils_instance.upload_to_db(cleaned_df_all, 'dim_users')

import yaml

with open('/Users/kk/Documents/ai_core/Data_engineering/multinational-retail-data-centralisation893/my_db_creds.yaml', 'r') as file_1:
    creds_1 = yaml.safe_load(file_1)

print(creds_1)  # Ensure this prints the updated credentials

"""


"""
Code to clean addresses if needed 

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