import pandas as pd
import logging 
import numpy as np
import re 
from IPython.display import display
from sqlalchemy import MetaData, Table
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from unidecode import unidecode
from dateutil import parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DataCleaning: 
    
    """
    This class contains the methods to clean the data extracted using methods from the DataExtractor class 

    Attributes: 
        self.df: used to chain the cleaning methods on the 'legacy_users' data together 
        logger: logging is initialised to facilitate logging used in some of the methods  
    
    """
    
    def __init__ (self, df=None):
        self.df = df
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def drop_null_values_and_duplicates(self): 

        """
        This method retrieves the 'legacy_users' data and then drops rows with NULL values and any duplicate rows 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with NaN values and duplicates dropped 
        """
                    
        # LOGGING starting of method
        print('drop_null_values_and_duplicates is working')

        # Create an instance of DataExtractor so that I can use it's methods 
        instance = DataExtractor()

        # Read data from the 'legacy_users' table using the DataExtractor method 
        self.df = instance.read_data_from_table('legacy_users')  

        # LOGGING number of rows 
        num_rows = self.df.shape[0]
        print(f"Number of rows at start of cleaning legacy users data: {num_rows}")

        # LOGGING check null rows before cleaning  
        #null_rows = df.loc[df['country_code'] == 'NULL'] 
        #display('before cleaning', null_rows)

        # Drop rows with any NaN values
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)

        # LOGGING check null rows after cleaning  
        #null_rows = df.loc[df['country_code'] == 'NULL'] 
        #display('after cleaning', null_rows)

        # LOGGING print number of rows 
        #num_rows = df.shape[0]
        #print(f"Number of rows after taking out null: {num_rows}")

        # Drop the duplicates from the dataframe directly ('in place')  
        self.df = self.df.drop_duplicates()

        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows at end: {num_rows}")

        # return the cleaned df 
        return self
    
    
    def clean_country_codes(self):
        
        """
        This method replaces incorrect country codes with the correct country code 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned country codes 
        """
                    
        # LOGGING starting of method
        print('clean_country_codes is working')

        # replace 'GGB' with 'GB' in the 'country_code' column of the cleaned_country_names_and_codes_df
        self.df['country_code'] = self.df['country_code'].replace('GGB', 'GB')

        #return the cleaned df 
        return self

    def remove_garbage(self):
        
        """
        This method extracts and cleans the 'garbage' rows which are made up of random letters and numbers from the dataframe using boolean indexing 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe witn garbage rows removed  
        """
                    
        # LOGGING starting of method
        print('remove_garbage is working')

        # Create the regex 
        regex_country_code = '^[A-Z]{2}$'

        # LOGGING: Make a dataframe of the rows that will be dropped and print them  
        # dropped_rows = self.df.loc[~self.df['country_code'].str.match(regex_country_code)] 
        # display('before cleaning', dropped_rows)

        self.df = self.df[self.df['country_code'].str.match(regex_country_code)] 

        # LOGGING: Make a dataframe of the rows are left after the str.match to ensure they've all been cleaned 
        # dropped_rows = self.df.loc[~self.df['country_code'].str.match(regex_country_code)] 
        # display('after cleaning', dropped_rows)

        return self 


    def clean_country_names(self):  
        
        """
        This method extracts and cleans the table of country names from the database  
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe of the cleaned country names 
        """
                    
        # LOGGING starting of method
        print('clean_country_names is working')
        
        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows before country cleaning: {num_rows}")

        # Create regex pattern for filtering out country names that contain numbers, special characters (apart from '-')  
        pattern = r'^[a-zA-Z\s-]+$'

        # Filter the data frame to only contain items that match the regex  
        self.df = self.df[self.df['country'].str.match(pattern)] 

        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows after country cleaning: {num_rows}")
    
        #return the cleaned dataframe 
        return self
 

    def cleaning_text_fields(self):   
        """
        This method clean the columns 'first_name' and 'last_name' by making them lower case, stripping white space and removing special characters    
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned 'first_name' and 'last_name' columns
        """

        # LOGGING: starting of method
        print('clean_text_fields is working')

        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows before name cleaning: {num_rows}")


        #convert first_name and last_name to lower case 
        self.df['first_name'] = self.df['first_name'].str.lower()
        self.df['last_name'] = self.df['last_name'].str.lower()

        # LOGGING: log that it's worked 
        #after_lower_case_example = self.df.iloc[0]['first_name']
        #print('this is the name after transformation:', after_lower_case_example)

        # Strip whitespace from first_name and last_name fields
        self.df['first_name'] = self.df['first_name'].str.strip()
        self.df['last_name'] = self.df['last_name'].str.strip()

        # Remove special characters using unidecode
        self.df['first_name'] = self.df['first_name'].apply(unidecode)
        self.df['last_name'] = self.df['last_name'].apply(unidecode)

        # LOGGING: Check all remaining names match the pattern later used for casting the data 
        # create regex pattern for checking the names match  
        #regex_pattern = r'^[A-Za-z ._\'-]+$'

        # LOGGING Create dataframe of the names the regex doesn't apply to 
        #not_applied = self.df[~self.df['first_name'].str.match(regex_pattern, na=False)]

        # LOGGING Print names the pattern doesn't apply to, or a message that all names match 
        # if not not_applied.empty:
        #     print("Names not matching the pattern:")
        #     print(not_applied)
        # else:
        #     print("All names match the pattern.")

        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows after name cleaning: {num_rows}")

        # return the cleaned df 
        return self


    def clean_dob_and_join_date(self):
        
        """
        This method cleans and converts the 'date_of_birth' and 'join_date' columns to datetime format, 
        then drops any invalid dates by dropping 'NaT' 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned dob formatted as datetime 
        """

        # LOGGING: starting of method
        print('clean_dob method is working')

        # STEP 1: Define the converting function

        # Initialize a list to store invalid dates
        invalid_dates_list = []

        # Define a function to parse dates and standardize format that can be applied to date_of_birth and join_date
        def parse_date(date_str):
            try:
                # Attempt to parse the date string to a datetime object
                dt = parser.parse(date_str)
                # Convert to the desired format (YYYY-MM-DD)
                return dt.strftime('%Y-%m-%d')
            except (parser.ParserError, ValueError):
                # Append invalid date to the list
                invalid_dates_list.append(date_str)
                return np.nan  # Return NaN for invalid dates

        # STEP 2: cleaning date_of_birth 

        # # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows before dob cleaning: {num_rows}")

        # Apply the function to the 'date_of_birth' column
        self.df['date_of_birth'] = self.df['date_of_birth'].apply(parse_date)

        # Identify rows that would be null after conversion
        invalid_rows = self.df[self.df['date_of_birth'].isna()]

        # LOGGING: display the rows that would be converted to dull following the parse_date conversion 
        # display("Rows that would be converted to NULL:")
        # display(invalid_rows)

        # Drop rows with NaN (invalid dates)
        self.df = self.df.dropna(subset=['date_of_birth'])

        # LOGGING show the list of dates that couldn't be converted by parse_data function 
        # display("\nList of invalid dates:")
        # display(invalid_dates_list)

        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows after country cleaning: {num_rows}")

        # STEP 3: clean join date 

        # LOGGING print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows before join date cleaning: {num_rows}")

        # Initialize a list to store invalid dates
        # invalid_join_dates_list = []

        # Apply the function to the 'date_of_birth' column
        self.df['join_date'] = self.df['join_date'].apply(parse_date)

        # Identify rows that would be null after conversion
        invalid_join_rows = self.df[self.df['join_date'].isna()]

        # LOGGING display rows that would be converted to NULL 
        # display("Rows that would be converted to NULL:")
        # display(invalid_join_rows)

        # Drop rows with NaN (invalid dates)
        self.df = self.df.dropna(subset=['join_date'])

        # LOGGING display rows that are invalid (not converted)
        # display("\nList of invalid dates:")
        # display(invalid_dates_list)

        # LOGGING: print number of rows 
        # num_rows = self.df.shape[0]
        # print(f"Number of rows after cleaning join_date: {num_rows}")

        # LOGGING: print number of rows 
        num_rows = self.df.shape[0]
        print(f"Number of rows at end of cleaning legacy users data: {num_rows}")

        # return cleaned df 
        return self


    def clean_legacy_users_data(self): 
        """
        This method calls all the data cleaning methods on the 'legacy_users' data   
        
        Args: 
            None 

        Returns: 
            dataframe: A cleaned dataframe of the legacy users data
        
        """
        
        return (self
                .drop_null_values_and_duplicates()
                .clean_country_codes()
                .remove_garbage() 
                .clean_country_names()
                .cleaning_text_fields()
                .clean_dob_and_join_date()
                .df)


    def clean_card_data(self):  
        """
        This method retrieves a table from a pdf using the DataExtractor method 'retrieve_pdf_data'. It then: 
        1. Cleans the 'expiry_date' column by removing incorrect values and converting it to datetime. 
        2. Cleans card numbers via a regex 
        3. Converts date_payment_confirmed to datetime 
        4. Removes incorrect card providers from the 'card_provider' column,
        It then resets the index 

        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned 'expiry_date', 'card_number', 'date_payment_confirmed' and 'card_provider' columns
        """

        # Gettting the card data to clean 

        # LOGGING: start of method 
        print('Started clean_card_data method. Running retrieve_pdf_data to get the data')
        
        # Create an instance of DataExtractor
        instance = DataExtractor()

        # Read data from the 'legacy_users' table
        df = instance.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        
        # LOGGING: show the number of rows at the start of the cleaning processs
        num_rows = df.shape[0]
        print(f"Number of rows before cleaning card data: {num_rows}")

        # STEP 1: Cleaning expiry date column

        # LOGGING: starting the expiry data cleaning 
        print('started cleaning expiry date column')

        # Define a regular expression pattern for the MM/YY format
        pattern_exp = r'^\d{2}/\d{2}$'

        # Use str.match to filter rows where 'expiry_date' matches the pattern
        df = df[df['expiry_date'].str.match(pattern_exp)].reset_index(drop=True)

        # Convert expiry_date to datetime, coerce errors to NaT using .loc to avoid SettingWithCopyWarning
        df.loc[:, 'datetime_expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')

        # LOGGING: Identify rows that will be removed (rows with NaT in 'datetime_expiry_date')
        # removed_expiry_dates = df[df['datetime_expiry_date'].isna()]

        # LOGGING: show the dates that would be removed
        # print("Expiry date datetime conversion that will be removed:")
        # display(removed_expiry_dates)

        # Reset the index
        df = df.reset_index(drop=True)

        # LOGGING: show number rows after cleaning expiry date 
        # num_rows = df.shape[0]
        # print(f"Number of rows after cleaning expiry date: {num_rows}")


        # STEP 2: Cleaning card number
        
        # LOGGING: show number of rows at start of cleaning 
        # num_rows = df.shape[0]
        # print(f"Number of rows before card number cleaning: {num_rows}")

        # Define a regular expression pattern for numbers only
        pattern_card = r'^\d+$'
   
        # Ensure all elements in the 'card_number' column are strings
        df['card_number'] = df['card_number'].astype(str)

        # Remove '??' from the strings in the 'card_number' column
        df['card_number'] = df['card_number'].str.replace('?', '', regex=False)

        # Use apply with a lambda function to filter rows where 'card_number' matches the pattern
        df = df[df['card_number'].apply(lambda x: bool(re.match(pattern_card, x)))].reset_index(drop=True)


        # STEP 3: Cleaning date_payment_confirmed
        
        # LOGGING: show number of rows at start of cleaning
        # num_rows = df.shape[0]
        # print(f"Number of rows before date_payment_confirmed cleaning: {num_rows}")

        # Initialize a list to store invalid dates
        invalid_dates_list = []

        # Function to parse dates and standardize format
        def parse_date(date_str):
            try:
                # Attempt to parse the date string to a datetime object
                dt = parser.parse(date_str)
                # Convert to the desired format (YYYY-MM-DD)
                return dt.strftime('%Y-%m-%d')
            except (parser.ParserError, ValueError):
                # Append invalid date to the list
                invalid_dates_list.append(date_str)
                return np.nan  # Return NaN for invalid dates

        # Apply the function to the 'date_payment_confirmed' column
        df.loc[:, 'date_payment_confirmed'] = df['date_payment_confirmed'].apply(parse_date)

        # LOGGING: show dates that couldn't be converted from parse_date function
        #display("\nList of invalid dates:")
        #display(invalid_dates_list)

        # LOGGING: Identify and print rows that are NULL and so will be removed by df.dropna(subset=['date_payment_confirmed'])
        #invalid_rows = df[df['date_payment_confirmed'].isna()]
        #display("Rows that will be dropped:")
        #display(invalid_rows)

        # Drop rows with NaN in 'date_payment_confirmed'
        df = df.dropna(subset=['date_payment_confirmed'])

        # LOGGING: show number of rows after date_payment_confirmed cleaning 
        #num_rows = df.shape[0]
        #print(f"Number of rows after date_payment_confirmed cleaning: {num_rows}")

        # STEP 4: removing incorrect values from the card providers column 

        # LOGGING start of section 
        print('started removing incorrect values from card providers column')

        # LOGGING rows before card_provider cleaning  
        # num_rows = df.shape[0]
        # print(f"Number of rows before card provider cleaning: {num_rows}")

        # Create a valid providers list  
        valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit','JCB 15 digit', 'Maestro', 'Mastercard', 'Discover','VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        # Filter the DataFrame to keep only rows with valid card providers
        df = df[df['card_provider'].isin(valid_providers)]

        # LOGGING: check to see which have been removed 
        removed_providers = df[~df['card_provider'].isin(valid_providers)]
        display("Card provider rows that will be removed:")
        display(removed_providers)

        #Reset the index of the filtered DataFrame
        df = df.reset_index(drop=True)

        # LOGGING rows before card_provider cleaning
        # num_rows = df.shape[0]
        # print(f"Number of rows after card cleaning: {num_rows}")

        return df


    def cleaning_store_details(self):

        """
        This method retrieves the stores date using the DataExtractor method 'retrieve_store_data'. Then goes through the following steps: 
        1. Updates the webstore record (index 0) to have placeholder values to ensure it isn't treated as NULL 
        2. Removes the non-sensical records and NULL records from 'lat' column 
        3. Cleans 'staff_numbers' via a function that removes letters from staff_numbers 
        4. Replaces incorrect spellings in the 'continent' column 
        5. Filters out items in the column 'locality' that aren't real place names or NULL
        6. Converts opening date to datetime object and drops NA rows     
                
        Args: 
            None 

        Returns: 
            dataframe: A cleaned dataframe which has: 
                webstore record with placeholder values
                cleaned: 'lat', 'staff_numbers', 'continent', 'locality', and'opening_date' columns 

        """

        # LOGGING 
        print('Started cleaning_store_details')

        #creating instance of dataextractor 
        instance = DataExtractor()
        
        #retrieving the data from the stores API
        df = instance.retrieve_stores_data()  
        
        num_rows = df.shape[0]
        print(f"Number of rows store data before cleaning: {num_rows}")

        # STEP 1 Update the webstore to have placeholder values to ensure it isn't treated as NULL 

        # LOGGING: 
        # before = df.iloc[0]
        # display(before)

        # Define the index of the record to update
        index_to_update = 0

        # Define the values to replace 'N/A' with for the specific record
        df.loc[index_to_update, 'address'] = 'online'
        df.loc[index_to_update, 'longitude'] = 1
        df.loc[index_to_update, 'lat'] = 1
        df.loc[index_to_update, 'locality'] = 'online'
        df.loc[index_to_update, 'latitude'] = 1

        # LOGGING:
        # after = df.iloc[0]
        # display(after)


        # STEP 2 remove garbage records and NULL from lat 

        # Define the regex pattern to match invalid lat values
        pattern = r'^[A-Za-z0-9]+$'

        # Create a boolean mask for rows to keep: rows that do not match the pattern and are not 'NULL'
        mask = ~df['lat'].str.contains(pattern, na=False) & ~df['lat'].isin(['NULL'])

        # LOGGING:    
        #display(df[~mask])

        # Filter the DataFrame using the mask
        df = df[mask]

        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows store data after dropping lat column: {num_rows}")

        #STEP 3 cleaning staff numbers

        # Define function to remove letters from the staff_numbers
        def clean_staff_numbers(value):
            # Use regex to remove non-digit characters
            cleaned_value = re.sub(r'\D', '', value)
            return cleaned_value if cleaned_value else '0'  # Return '0' if the result is an empty string

        # LOGGING:
        # Create a boolean mask for rows that contain non-digit characters
        #mask = df['staff_numbers'].str.contains(r'\D', na=False)

        # LOGGING:
        # Display the rows before they are changed
        #print("Rows before cleaning:")
        #display(df[mask])

        # Apply the function to clean the staff_numbers column
        df['staff_numbers'] = df['staff_numbers'].apply(clean_staff_numbers)

        # Convert the cleaned staff_numbers column to integer
        df['staff_numbers'] = df['staff_numbers'].astype(int)

        # LOGGING:
        # Display the same rows after they have been cleaned
        #print("Rows after cleaning:")
        #display(df[mask])

        # STEP 4: cleaning continents 

        # replacing incorrect spellings of continents 
        continent_replacements = {
            'eeEurope': 'Europe',
            'eeAmerica': 'America'
        }

        df['continent'] = df['continent'].replace(continent_replacements)

        num_rows = df.shape[0]
        print(f"Number of rows store data after cleaning continent: {num_rows}")

        # STEP 5, cleaning locality 

        # filtering out items in locality that aren't real place names or NULL 
        pattern = r'^[a-zA-Z\s-]+$'
        locality_mask = df['locality'].str.match(pattern, na=False) | (df.index == 0)

        # LOGGING: Capture the rows that will be dropped
        #rows_dropped_by_locality = df[~locality_mask | df['locality'].replace('NULL', np.nan).isna()]
        #print("Rows that will be dropped during locality cleaning:")
        #print(rows_dropped_by_locality)

        # Apply the locality mask
        df = df[locality_mask]

        # Replace 'NULL' with np.nan and drop rows where 'locality' is NaN
        df['locality'] = df['locality'].replace('NULL', np.nan)
        df = df.dropna(subset=['locality'])

        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows store data after cleaning locality: {num_rows}")

        # STEP 6, Converting opening_date to datetime  

        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows before opening_date cleaning: {num_rows}")

        # Initialize a list to store invalid dates
        invalid_dates_list = []

        # Function to parse dates and standardize format
        def parse_date(date_str):
            try:
                # Attempt to parse the date string to a datetime object
                dt = parser.parse(date_str)
                # Convert to the desired format (YYYY-MM-DD)
                return dt.strftime('%Y-%m-%d')
            except (parser.ParserError, ValueError):
                # Append invalid date to the list
                invalid_dates_list.append(date_str)
                return np.nan  # Return NaN for invalid dates

        # Apply the function to the 'date_of_birth' column
        df['opening_date'] = df['opening_date'].apply(parse_date)

        # LOGGING:
        # Identify rows that would be null after conversion
        #invalid_rows = df[df['opening_date'].isna()]
        #display("Rows that would be converted to NULL:")
        #display(invalid_rows)

        # Drop rows with NaN (invalid dates)
        df = df.dropna(subset=['opening_date'])

        # LOGGING:
        #display("\nList of invalid dates:")
        #display(invalid_dates_list)
        
        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows after opening_date cleaning: {num_rows}")

        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows store data after cleaning: {num_rows}")
        
        return df 


    def clean_products_table(self):
        
        """
        This method retrieves a CSV of products from s3 using the extract_from_s3 method of the DataExtractor class. It then cleans the data via the following steps: 
        1. Creates a new column in the df 'weight_in_kg' and fills it with the weights from the 'weight' column converted to kg 
        2. Cleans mispelt values in the 'removed' column 
        3. Clean 'category' column via regex 
        4. Convert 'date_added' column to a datetime object 

        Args: 
            None 

        Returns: 
            dataframe: A dataframe with the new 'weight_in_kg' column, cleaned 'removed', 'category' and 'date_added' columns 

        """

        #LOGGING start of method 
        print('started clean_products_table')

        #creating instance of dataextractor 
        instance = DataExtractor()
            
        #retrieving the data from the stores API
        df = instance.extract_from_s3('s3://data-handling-public/products.csv') 

        # LOGGING:
        #num_rows = df.shape[0]
        #display(f"Number of rows before cleaning: {num_rows}")
        
        #STEP 1: Add column with weights in kg 

        # Define the regex pattern to match numbers and letters
        weight_pattern = re.compile(r'([0-9.]+)([a-zA-Z]+)')

        # Define the regex pattern to match the weight multiplier format
        weight_multiplier = re.compile(r'^(\d+)\s*x\s*(\d+)([a-zA-Z]+)$')

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
            
            match1 = weight_pattern.match(str(weight))
            match2 = weight_multiplier.match(str(weight))
            
            if match1:
                number = float(match1.group(1))  # Extract the numeric part
                unit = match1.group(2).lower()  # Extract the unit part
                return number * conversion_factors.get(unit, 0)  # Convert to kg
            
            elif match2:
                multiplier = int(match2.group(1))  # Get the multiplier
                amount = float(match2.group(2))  # Extract the amount
                unit = match2.group(3).lower()  # Extract the unit
                return (multiplier * amount) * conversion_factors.get(unit, 0)  # Convert to kg
            
            return None  # Handle cases where regex does not match

        # Apply the conversion to the 'weight' column
        df['weight_in_kg'] = df['weight'].apply(convert_to_kg)

        # LOGGING:
        #num_rows = df.shape[0]
        #display(f"Number of rows after conversion: {num_rows}")

        # STEP 2: Clean mispelt values

        # Correct the misspelled value
        df['removed'] = df['removed'].str.replace('Still_avaliable', 'Still_available')

        # LOGGING:
        #num_rows = df.shape[0]
        #display(f"Number of rows after replace still available: {num_rows}")

        # Define valid values
        valid_values = ['Still_available', 'Removed']

        # LOGGING:
        # Identify rows that will be removed
        #rows_to_remove = df[~df['removed'].isin(valid_values)]
        # Display the list of items that will be removed
        #print("Items to be removed:")
        #print(rows_to_remove)

        # Filter the DataFrame to keep only 'Still_available' or 'Removed'
        df = df[df['removed'].isin(valid_values)]

        # LOGGING:
        #num_rows = df.shape[0]
        #display(f"Number of rows after cleaning of removed column: {num_rows}")

        # LOGGING:
        # Capture rows with NaN values before dropping them
        #rows_with_na = df[df.isna().any(axis=1)]
        #display("Rows with NaN values:")
        #display(rows_with_na)

        # Drop rows with NaN values
        df = df.dropna(axis=0)

        # LOGGING:
        #num_rows = df.shape[0]
        #display(f"Number of rows after dropna: {num_rows}")

        # STEP 3: Clean 'category' column 

        # Define the regex pattern for the 'category' column
        category_pattern = re.compile(r'^[a-zA-Z\-]+$')

        # Capture rows that will be dropped by the category filter
        rows_dropped_by_category = df[~df['category'].apply(lambda x: bool(category_pattern.match(str(x))))]

        display("Rows that will be dropped by category filter:")
        display(rows_dropped_by_category)

        # Filter rows based on the category pattern
        df = df[df['category'].apply(lambda x: bool(category_pattern.match(str(x))))]

        num_rows = df.shape[0]
        display(f"Number of rows after category filter: {num_rows}")

        # STEP 4: convert date_added to datetime object 

        # LOGGING:
        # num_rows = df.shape[0]
        # print(f"Number of rows before opening_date cleaning: {num_rows}")

        # Initialize a list to store invalid dates
        invalid_dates_list = []

        # Function to parse dates and standardize format
        def parse_date(date_str):
            try:
                # Attempt to parse the date string to a datetime object
                dt = parser.parse(date_str)
                # Convert to the desired format (YYYY-MM-DD)
                return dt.strftime('%Y-%m-%d')
            except (parser.ParserError, ValueError):
                # Append invalid date to the list
                invalid_dates_list.append(date_str)
                return np.nan  # Return NaN for invalid dates

        # Apply the function to the 'date_of_birth' column
        df['date_added'] = df['date_added'].apply(parse_date)

        # LOGGING:
        # Identify rows that would be null after conversion
        #invalid_rows = df[df['date_added'].isna()]
        #display("Rows that would be converted to NULL:")
        #display(invalid_rows)

        # Drop rows with NaN (invalid dates)
        df = df.dropna(subset=['date_added'])

        # LOGGING:
        # display("\nList of invalid dates:")
        # display(invalid_dates_list)

        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows after date_added cleaning: {num_rows}")

        # LOGGING:
        #num_rows = df.shape[0]
        #print(f"Number of rows product data after cleaning: {num_rows}")

        return df 

    def clean_orders_data(self):
        
        """
        This method retrieves the 'orders_table' via the 'read_data_from_table' method from the DataExtractor 
        It then drops column '1', 'first_name' and 'last_name'
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with column '1', 'first_name' and 'last_name' dropped  

        """
        
        #LOGGING, the start of the method 
        print('started clean_orders_data')
        
        #creating instance of dataextractor
        instance = DataExtractor()
        
        # get the 'orders_table' data via the 'read_data_from_table' method, and assign it to df 
        df = instance.read_data_from_table('orders_table')
        
        # LOGGING:
        # num_rows = df.shape[0]
        # print(f"Number of rows orders data before cleaning: {num_rows}") 

        # drop unwanted columns 
        df = df.drop('1', axis=1)
        df = df.drop('first_name', axis=1)
        df = df.drop('last_name', axis=1)
        
        # LOGGING:
        # num_rows = df.shape[0]
        # print(f"Number of rows orders data after cleaning: {num_rows}")

        # return the cleaned df 
        return df 

    def clean_date_events(self):
        """
        This method retrieves the 'date_events' from an S3 resource using the DataExtractor's class 'extract_from_s3' 
        It then cleans the 'year' format and converts it to a datetime object  
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with the 'year' format cleaned 

        """
        
        # LOGGING
        print('started clean_data_events')

        #creating instance of dataextractor
        instance = DataExtractor()
        
        # extract the 'date_events' data from the S3 resource 
        df = instance.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        
        # LOGGING:
        # num_rows = df.shape[0]
        # print(f"Number of rows date times before cleaning: {num_rows}")

        # define the regex pattern for cleaning the year 
        year_regex = r'^\d{4}$'
        
        # apply the regex pattern to clean the year 
        df = df[df['year'].str.match(year_regex)]
 
        # LOGGING:
        # num_rows = df.shape[0]
        # print(f"Number of rows date times after year cleaning: {num_rows}")

        df['complete_timestamp'] = pd.to_datetime(df['year'] + '-' + df['month'] + '-' + df['day'] + ' ' + df['timestamp'], format='%Y-%m-%d %H:%M:%S')

        # LOGGING:
        # num_rows = df.shape[0]
        # print(f"Number of rows date times after date_events cleaning: {num_rows}")

        # return the cleaned dataframe 
        return df 


# TESTING / CALLING CODE 

#CREATING INSTANCES 

# creating data cleaning instance needed for running the methods in this class 
datacleaning_instance = DataCleaning() 

# creating database connector instance needed for running the methods in database_utils file  
databaseconnector_instance = DatabaseConnector() 

# dropping data bases 
databaseconnector_instance.reset_database() 
 
# LEGACY USER DATA 

# fetching and cleaning legacy users data 
clean_legacy_users_df = datacleaning_instance.clean_legacy_users_data() 

# uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_legacy_users_df, 'dim_users')

# CARD DATA 

# fetching and cleaning card data 
clean_card_data_df = datacleaning_instance.clean_card_data() 

# uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_card_data_df, 'dim_card_details')

# STORE DETAILS 

# fetching and cleaning card data 
clean_store_data_df = datacleaning_instance.cleaning_store_details()

# uploading store_details data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_store_details' 
databaseconnector_instance.upload_to_db(clean_store_data_df, 'dim_store_details')

# CLEAN PRODUCTS 

# # fetching and cleaning products data 
clean_weights_df = datacleaning_instance.clean_products_table()

# # uploading products data to database, using 'upload_to_db method of DatabaseConnector class, and called the products data 'dim_products' 
databaseconnector_instance.upload_to_db(clean_weights_df, 'dim_products')

# ORDERS TABLE  

# fetching and cleaning orders data 
clean_orders_df = datacleaning_instance.clean_orders_data()

# uploading orders data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'orders_table' 
databaseconnector_instance.upload_to_db(clean_orders_df, 'orders_table')

# DATE EVENTS  

# fetching and date events data 
clean_date_events_df = datacleaning_instance.clean_date_events()

# uploading date events data to database, using 'upload_to_db method of DatabaseConnector class, and called the date events data 'dim_date_times' 
databaseconnector_instance.upload_to_db(clean_date_events_df, 'dim_date_times')