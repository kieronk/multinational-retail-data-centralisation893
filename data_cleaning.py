import pandas as pd
import logging 
import numpy as np
import re 
import os 
from dotenv import load_dotenv
from IPython.display import display
from sqlalchemy import MetaData, Table
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from unidecode import unidecode
from dateutil import parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DataCleaning: 
    
    """
    This class contains methods to clean data extracted using methods from the DataExtractor class.

    Attributes:
        df (pd.DataFrame): Used for chaining cleaning methods on the 'legacy_users' data.
        pdf_path (str): Path to the PDF file for extracting data.
        s3_dates_url (str): URL for the S3 bucket containing dates data.
        s3_products_url (str): URL for the S3 bucket containing products data.  
    
    """
    
    def __init__ (self, df=None):
        """
        Initializes the DataCleaning class with optional dataframe and environment variables.

        Args:
            df (pd.DataFrame, optional): Dataframe for chaining cleaning methods. Defaults to None.
        
        """
        # Load environment variables from .env file
        load_dotenv()
        
        try: 
            # iniitalise self.df for chaining together cleaning methods  
            self.df = df
            
            # gets urls for the data that will be extracted and cleaned  
            self.pdf_path = os.getenv('PDF_PATH')
            self.s3_dates_url = os.getenv('S3_DATES_URL') 
            self.s3_products_url = os.getenv('S3_PRODUCTS_URL')
    
        except ValueError as ve:
            logging.error(f"Error loading environment variables: {ve}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise 

    def drop_null_values_and_duplicates(self): 

        """
        Drops rows with NULL values and duplicates from the 'legacy_users' table.

        Args:
            None

        Returns:
            DataCleaning: Self, with cleaned dataframe. 
        """
                    
        logging.info('drop_null_values_and_duplicates is working')

        # Create an instance of DataExtractor
        instance = DataExtractor()

        # Read data from the 'legacy_users' table using the DataExtractor method 
        self.df = instance.read_data_from_table('legacy_users')  

        # Drop rows with any NaN values
        self.df.replace('NULL', np.nan, inplace=True)
        self.df.dropna(inplace=True)

        # Drop the duplicates from the dataframe directly ('in place')  
        self.df = self.df.drop_duplicates()

        # return the cleaned df 
        return self
    
    
    def clean_country_codes(self):
        
        """
        Replaces incorrect country codes with valid codes in the 'country_code' column.

        Args:
            None

        Returns:
            DataCleaning: Self, with cleaned dataframe.
        """
                    
        logging.info('clean_country_codes is working')

        # replace 'GGB' with 'GB' in the 'country_code' column of the cleaned_country_names_and_codes_df
        self.df['country_code'] = self.df['country_code'].replace('GGB', 'GB')

        #return the cleaned df 
        return self

    def remove_garbage(self):
        
        """
        Removes rows with invalid country codes (random letters/numbers) using regex.

        Args:
            None

        Returns:
            DataCleaning: Self, with cleaned dataframe.  
        """
                    
        logging.info('remove_garbage is working')

        # Create the regex 
        regex_country_code = '^[A-Z]{2}$'

        # Apply the regex
        self.df = self.df[self.df['country_code'].str.match(regex_country_code)] 

        return self 


    def clean_country_names(self):  
        
        """
        Removes invalid country names containing numbers or special characters.

        Args:
            None

        Returns:
            DataCleaning: Self, with cleaned dataframe. 
        """
                    
        logging.info('clean_country_names is working')
        
        # Create regex pattern for filtering out country names that contain numbers, special characters (apart from '-')  
        pattern = r'^[a-zA-Z\s-]+$'

        # Filter the data frame to only contain items that match the regex  
        self.df = self.df[self.df['country'].str.match(pattern)] 
   
        #return the cleaned dataframe 
        return self
 

    def cleaning_text_fields(self):   
        """
        Cleans 'first_name' and 'last_name' by converting to lowercase, stripping whitespace, 
        and removing special characters.

        Args:
            None

        Returns:
            DataCleaning: Self, with cleaned dataframe.
        """

        logging.info('clean_text_fields is working')

        #convert first_name and last_name to lower case 
        self.df['first_name'] = self.df['first_name'].str.lower()
        self.df['last_name'] = self.df['last_name'].str.lower()

        # Strip whitespace from first_name and last_name fields
        self.df['first_name'] = self.df['first_name'].str.strip()
        self.df['last_name'] = self.df['last_name'].str.strip()

        # Remove special characters using unidecode
        self.df['first_name'] = self.df['first_name'].apply(unidecode)
        self.df['last_name'] = self.df['last_name'].apply(unidecode)

         # return the cleaned df 
        return self


    def clean_dob_and_join_date(self):
        
        """
        Cleans and converts 'date_of_birth' and 'join_date' columns to datetime format,
        dropping invalid dates.

        Args:
            None

        Returns:
            DataCleaning: Self, with cleaned dataframe.
        """

        logging.info('clean_dob_and_join_date method is working')

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

        # Apply the function to the 'date_of_birth' column
        self.df['date_of_birth'] = self.df['date_of_birth'].apply(parse_date)

        # Drop rows with NaN (invalid dates)
        self.df = self.df.dropna(subset=['date_of_birth'])

        # STEP 3: clean join date 

        # Apply the function to the 'date_of_birth' column
        self.df['join_date'] = self.df['join_date'].apply(parse_date)

        # Drop rows with NaN (invalid dates)
        self.df = self.df.dropna(subset=['join_date'])

        # return cleaned df 
        return self


    def clean_legacy_users_data(self): 
        """
        Cleans the 'legacy_users' data by applying multiple cleaning methods.

        Args:
            None

        Returns:
            pd.DataFrame: Fully cleaned dataframe of legacy users.
        
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
        Cleans card data retrieved from a PDF, including:
        - 'expiry_date' column: Removes incorrect values and converts to datetime.
        - 'card_number' column: Cleans with regex.
        - 'date_payment_confirmed' column: Converts to datetime.
        - 'card_provider' column: Removes invalid providers.

        Args:
            None

        Returns:
            pd.DataFrame: Cleaned card data.
        """

        logging.info('Started clean_card_data method. Running retrieve_pdf_data to get the data')
        
        # Create an instance of DataExtractor
        instance = DataExtractor()

        # Read data from the card details pdf
        df = instance.retrieve_pdf_data(self.pdf_path)

        # STEP 1: Cleaning expiry date column

        logging.info('started cleaning expiry date column')

        # Define a regular expression pattern for the MM/YY format
        pattern_exp = r'^\d{2}/\d{2}$'

        # Use str.match to filter rows where 'expiry_date' matches the pattern
        df = df[df['expiry_date'].str.match(pattern_exp)].reset_index(drop=True)

        # Convert expiry_date to datetime, coerce errors to NaT using .loc to avoid SettingWithCopyWarning
        df.loc[:, 'datetime_expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')

        # Reset the index
        df = df.reset_index(drop=True)

        # STEP 2: Cleaning card number
        
        # Define a regular expression pattern for numbers only
        pattern_card = r'^\d+$'
   
        # Ensure all elements in the 'card_number' column are strings
        df['card_number'] = df['card_number'].astype(str)

        # Remove '??' from the strings in the 'card_number' column
        df['card_number'] = df['card_number'].str.replace('?', '', regex=False)

        # Use apply with a lambda function to filter rows where 'card_number' matches the pattern
        df = df[df['card_number'].apply(lambda x: bool(re.match(pattern_card, x)))].reset_index(drop=True)

        # STEP 3: Cleaning date_payment_confirmed
        
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

        # Drop rows with NaN in 'date_payment_confirmed'
        df = df.dropna(subset=['date_payment_confirmed'])

        # STEP 4: removing incorrect values from the card providers column 

        logging.info('started removing incorrect values from card providers column')

        # Create a valid providers list  
        valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit','JCB 15 digit', 'Maestro', 'Mastercard', 'Discover','VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        # Filter the DataFrame to keep only rows with valid card providers
        df = df[df['card_provider'].isin(valid_providers)]

        #Reset the index of the filtered DataFrame
        df = df.reset_index(drop=True)

        return df


    def cleaning_store_details(self):

        """
        Cleans store details by:
        - Setting placeholder values for webstore records.
        - Removing invalid and NULL values in 'lat' column.
        - Cleaning 'staff_numbers' column.
        - Replacing incorrect continent names.
        - Cleaning 'locality' and 'opening_date' columns.

        Args:
            None

        Returns:
            pd.DataFrame: Cleaned store details dataframe. 

        """

        logging.info('Started cleaning_store_details')

        # creating instance of dataextractor 
        instance = DataExtractor()
        
        # retrieving the data from the stores API
        df = instance.retrieve_stores_data()  
        
        # STEP 1 Update the webstore to have placeholder values to ensure it isn't treated as NULL 

        # Define the index of the record to update
        index_to_update = 0

        # Define the values to replace 'N/A' with for the specific record
        df.loc[index_to_update, 'address'] = 'online'
        df.loc[index_to_update, 'longitude'] = 1
        df.loc[index_to_update, 'lat'] = 1
        df.loc[index_to_update, 'locality'] = 'online'
        df.loc[index_to_update, 'latitude'] = 1

        # STEP 2 remove garbage records and NULL from lat 

        # Define the regex pattern to match invalid lat values
        pattern = r'^[A-Za-z0-9]+$'

        # Create a boolean mask for rows to keep: rows that do not match the pattern and are not 'NULL'
        mask = ~df['lat'].str.contains(pattern, na=False) & ~df['lat'].isin(['NULL'])

        # Filter the DataFrame using the mask
        df = df[mask]

        # STEP 3 cleaning staff numbers

        # Define function to remove letters from the staff_numbers
        def clean_staff_numbers(value):
            # Use regex to remove non-digit characters
            cleaned_value = re.sub(r'\D', '', value)
            return cleaned_value if cleaned_value else '0'  # Return '0' if the result is an empty string

        # Apply the function to clean the staff_numbers column
        df['staff_numbers'] = df['staff_numbers'].apply(clean_staff_numbers)

        # Convert the cleaned staff_numbers column to integer
        df['staff_numbers'] = df['staff_numbers'].astype(int)

        # STEP 4: cleaning continents 

        # replacing incorrect spellings of continents 
        continent_replacements = {
            'eeEurope': 'Europe',
            'eeAmerica': 'America'
        }

        df['continent'] = df['continent'].replace(continent_replacements)

        # STEP 5, cleaning locality 

        # filtering out items in locality that aren't real place names or NULL 
        pattern = r'^[a-zA-Z\s-]+$'
        locality_mask = df['locality'].str.match(pattern, na=False) | (df.index == 0)

        # Apply the locality mask
        df = df[locality_mask]

        # Replace 'NULL' with np.nan and drop rows where 'locality' is NaN
        df['locality'] = df['locality'].replace('NULL', np.nan)
        df = df.dropna(subset=['locality'])

        # STEP 6, Converting opening_date to datetime  

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

        # Drop rows with NaN (invalid dates)
        df = df.dropna(subset=['opening_date'])
       
        return df 


    def clean_products_table(self):
        
        """
        Cleans the products table by:
        - Adding a 'weight_in_kg' column converted from 'weight'.
        - Fixing misspelled values in the 'removed' column.
        - Cleaning the 'category' column using regex.
        - Converting 'date_added' to datetime format.

        Args:
            None

        Returns:
            pd.DataFrame: Cleaned products dataframe. 

        """

        logging.info('started clean_products_table')

        # creating instance of dataextractor 
        instance = DataExtractor()
            
        # retrieving the data from the stores API
        df = instance.extract_from_s3(self.s3_products_url) 
       
        # STEP 1: Add column with weights in kg 

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

        # STEP 2: Clean mispelt values

        # Correct the misspelled value
        df['removed'] = df['removed'].str.replace('Still_avaliable', 'Still_available')

        # Define valid values
        valid_values = ['Still_available', 'Removed']

        # Filter the DataFrame to keep only 'Still_available' or 'Removed'
        df = df[df['removed'].isin(valid_values)]

        # Drop rows with NaN values
        df = df.dropna(axis=0)

        # STEP 3: Clean 'category' column 

        # Define the regex pattern for the 'category' column
        category_pattern = re.compile(r'^[a-zA-Z\-]+$')

        # Capture rows that will be dropped by the category filter
        rows_dropped_by_category = df[~df['category'].apply(lambda x: bool(category_pattern.match(str(x))))]

        # Filter rows based on the category pattern
        df = df[df['category'].apply(lambda x: bool(category_pattern.match(str(x))))]

        # STEP 4: convert date_added to datetime object 

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

        # Drop rows with NaN (invalid dates)
        df = df.dropna(subset=['date_added'])

        return df 

    def clean_orders_data(self):
        
        """
        Cleans the 'orders_table' by dropping unnecessary columns ('1', 'first_name', 'last_name').

        Args:
            None

        Returns:
            pd.DataFrame: Cleaned orders dataframe. 

        """
        
        logging.info('started clean_orders_data')
        
        # creating instance of dataextractor
        instance = DataExtractor()
        
        # get the 'orders_table' data via the 'read_data_from_table' method, and assign it to df 
        df = instance.read_data_from_table('orders_table')
        
        # drop unwanted columns 
        df = df.drop('1', axis=1)
        df = df.drop('first_name', axis=1)
        df = df.drop('last_name', axis=1)
        
        # return the cleaned df 
        return df 

    def clean_date_events(self):
        """
        Cleans the 'date_events' table by:
        - Validating and cleaning the 'year' column.
        - Creating a 'complete_timestamp' column by combining date and time columns.

        Args:
            None

        Returns:
            pd.DataFrame: Cleaned date events dataframe.

        """
        
        logging.info('started clean_data_events')

        #creating instance of dataextractor
        instance = DataExtractor()
        
        # extract the 'date_events' data from the S3 resource 
        df = instance.extract_from_s3(self.s3_dates_url)
        
        # define the regex pattern for cleaning the year 
        year_regex = r'^\d{4}$'
        
        # apply the regex pattern to clean the year 
        df = df[df['year'].str.match(year_regex)]

        df['complete_timestamp'] = pd.to_datetime(df['year'] + '-' + df['month'] + '-' + df['day'] + ' ' + df['timestamp'], format='%Y-%m-%d %H:%M:%S')

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