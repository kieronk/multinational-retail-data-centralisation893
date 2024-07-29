import pandas as pd
import logging 
import numpy as np
import re 
from IPython.display import display
from sqlalchemy import MetaData, Table
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

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
       
    def clean_country_names(self):  
        
        """
        This method extracts and cleans the table of country names from the database using boolean indexing 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe of the cleaned country names 
        """
                    
        #log starting of method
        print('clean_country_names is working')

        # Create an instance of DataExtractor so that I can use it's methods 
        instance = DataExtractor()

        # Read data from the 'legacy_users' table using the DataExtractor method 
        self.df = instance.read_data_from_table('legacy_users')  
        
        # regex pattern for filtering out names that contain numbers, special characters (apart from '-')  
        pattern = r'^[a-zA-Z\s-]+$'
        
        #filters the data frame on 2 boolean indexes: the regex boolean index and whether it contains 'NULL' 
        self.df = self.df[self.df['country'].str.match(pattern)] # & (df['country'] != 'NULL')]
    
        #return the cleaned dataframe 
        return self


    def clean_country_codes(self):
        
        """
        This method replaces incorrect country codes 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe of the cleaned country names and codes 
        """
                    
        #log starting of method
        print('clean_country_codes is working')

        # replace 'GGB' with 'GB' in the 'country_code' column of the cleaned_country_names_and_codes_df
        self.df['country_code'] = self.df['country_code'].replace('GGB', 'GB')

        #return the cleaned df 
        return self


    def drop_null_values_and_duplicates(self): 

        """
        This method drops rows in the 'country' and 'country_code' columns with NULL values
        Then it drops any duplicate rows 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe of the cleaned country names and codes 
        """
                    
        #log starting of method
        print('drop_null_values_and_duplicates is working')

        # filter the dataframe to exclude rows where both the country and country_code columns have the value 'NULL'
        self.df = self.df[(self.df['country']!= 'NULL') | (self.df['country_code'] != 'NULL')]

        # drop the duplicates from the dataframe directly ('in place')  
        self.df = self.df.drop_duplicates()

        # return the cleaned df 
        return self

    def clean_dob(self): #cleaning date_of_birth
        
        """
        This method converts the 'date_of_birth' column to datetime format, 
        then drops any invalid dates by dropping 'NaT' 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned dob formatted as datetime 
        """

        #log starting of method
        print('clean_dob method is working')

        # DEBUGGING: get an example from before the conversion so I can check if it works (I know the row indexed by 360 would need converting)
        #original_date_of_birth_example = cleaned_dob_df.iloc[360]['date_of_birth']

        # convert the 'date_of_birth' column to datetime, handling the different formats
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], errors='coerce', format='mixed')

        # DEBUGGING: Check which dates could not be converted
        #invalid_dates = cleaned_dob_df[cleaned_dob_df['date_of_birth'].isna()]
        
        # DEBUGGING: log the number of rows that couldn't be converted in case of errors  
        #print('number of rows that convert to NaT', len(invalid_dates)) 

        # DEBUGGING: log examples to check the conversion worked by printing an example of a difficult date
        #date_of_birth_converted_example = cleaned_dob_df.iloc[360]['date_of_birth']
        #print('the original date_of_birth before pd.to_datetime():\n', original_date_of_birth_example)
        #print('date_of_birth after pd.to_datetime:\n', date_of_birth_converted_example)

        # dropping rows with NaT values
        self.df = self.df.dropna(subset=['date_of_birth'])

        # return cleaned df 
        return self

    def clean_join_date(self): #cleaning join_date 
        
        """
        This method converts the 'join_date' column to datetime format, 
        then drops any invalid dates by dropping 'NaT' 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned join date 
        """

        #log starting of method
        print('clean_join_date is working')

        #DEBUGGING: get an example from before the conversion so I can check if it works (I know the row indexed by 202 would need converting)
        #original_join_date_example = self.df.iloc[202]['join_date']

        # Convert the join_date column to datetime, handling various formats
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce', format='mixed') 

        # DEBUGGING: Check which dates could not be converted
        #invalid_dates = self.df[self.df['join_date'].isna()]
        
        # DEBUGGING: log the number of rows that couldn't be converted in case of errors 
        #print('number of rows that convert to NaT', len(invalid_dates)) 

        # DEBUGGING: Check the conversion worked by printing an example of a difficult date
        #join_date_converted_example = self.df.iloc[202]['join_date']
        #print('original join_date:\n', original_join_date_example)
        #print('join_date after pd.to_datetime:\n', join_date_converted_example)

        # dropping rows with NaT values
        self.df = self.df.dropna(subset=['join_date'])

        return self 


    def cleaning_text_fields(self):   
        """
        This method clean the columns 'first_name' and 'last_name' by converting them to lower case and removing white spaces  
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned 'first_name' and 'last_name' columns
        """

        #log starting of method
        print('clean_text_fields is working')

        # DEBUGGING: get an example from before the conversion so I can check if it works (can be any index as they all need converting)
        #before_lower_case_example = self.df.iloc[0]['first_name']
        #print('this is the name before transformation:', before_lower_case_example)

        #convert first_name and last_name  
        self.df['first_name'] = self.df['first_name'].str.lower()
        self.df['last_name'] = self.df['last_name'].str.lower()

        # DEBUGGING: log that it's worked 
        #after_lower_case_example = self.df.iloc[0]['first_name']
        #print('this is the name after transformation:', after_lower_case_example)

        # strip whitespace from first_name and last_name fields
        self.df['first_name'] = self.df['first_name'].str.strip()
        self.df['last_name'] = self.df['last_name'].str.strip()

        # return the cleaned df 
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
                .clean_country_names()
                .clean_country_codes()
                .drop_null_values_and_duplicates()
                .clean_dob()
                .clean_join_date()
                .cleaning_text_fields()
                .df)


    def clean_card_data(self):  
        
        """
        This method retrieves a table from a pdf using the DataExtractor method 'retrieve_pdf_data', 
        It then cleans the 'expiry_date' column by removing incorrect values and converting it to datetime. 
        It then removes incorrect card providers from the 'card_provider' column, resets the index 

        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned 'expiry_date' and 'card_provider' columns
        """

    # gettting the card data to clean 

        # LOGGING 
        print('Started clean_card_data method. Running retrieve_pdf_data to get the data')
        
        # Create an instance of DataExtractor
        instance = DataExtractor()

        # Read data from the 'legacy_users' table
        df = instance.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

        #log starting of method
        #self.logger.info(f"Running retrieve_pdf_data with pdf_path: {pdf_path}")
        
    #cleaning expiry date column 
        
        # LOGGING 
        print('started cleaning expiry date column')
        
        # Define a regular expression pattern for the MM/YY format
        pattern = r'^\d{2}/\d{2}$'

        # Use str.match to filter rows where 'expiry_date' matches the pattern
        df = df[df['expiry_date'].str.match(pattern)]

        # Reset the index 
        df = df.reset_index(drop=True)

    #adding datetime version of expiry date for calculations 

        # LOGGING 
        print('started datetime conversion for expiry_date')

        #add a column 'datetime_expiry_date' to the dateframe which is filled with 'expiry_date' converted to datetime 
        df['datetime_expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')

    # removing incorrect values from the card providers column 
        
        # LOGGING 
        print('started removing incorrect values from card providers column')

        #create a valid providers list  
        valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit','JCB 15 digit', 'Maestro', 'Mastercard', 'Discover','VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        # Filter the DataFrame to keep only rows with valid card providers
        df = df[df['card_provider'].isin(valid_providers)]

        #Reset the index of the filtered DataFrame
        df = df.reset_index(drop=True)

        return df

    def cleaning_store_details(self):

        """
        This method retrieves the stores date using the DataExtractor method 'retrieve_store_data' 
        It convert latitiude to a number, then drops rows with missing data
        It drops the 'lat' column 
        It then filters out items in the column 'locality' that aren't real place names or NULL
        It then replaces incorrect spellings in the 'continent' column  
        It then converts 'opening_date' to a datetime object and drops NaT 
        
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with cleaned 'longitude', 'latitude', 'locality', 'continent' and'opening_date' columns 

        """

        # LOGGING 
        print('Started cleaning_store_details')

        #creating instance of dataextractor 
        instance = DataExtractor()
        
        #retrieving the data from the stores API
        df = instance.retrieve_stores_data()  
        
        print('before cleaning: ', df.info())

        # Clean the latitude column
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df = df.dropna(subset=['latitude'])

        # Dropping the 'latitude' column as it's empty 
        df = df.drop(columns=['lat'])
    
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
        
        # LOGGING: checking everything has worked 
        #print(df['continent'].unique())
        #print(df['store_type'].unique()) 
        #print(df['country_code'].unique()) 
        #print(df['continent'].unique())

        # LOGGING: checking of datetime before is datetime64 datetype  
        #is_datetime_before = pd.api.types.is_datetime64_any_dtype(df['opening_date'])
        
        # converting opening date to datetime object 
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='%Y-%m-%d', errors='coerce')
        
        # dropping any rows which contain missing values  
        df = df.dropna(axis=0)

        # LOGGING: checking the conversion worked 
        #is_datetime_after = pd.api.types.is_datetime64_any_dtype(df['opening_date'])
        #print(f"Is 'dates' column datetime64 dtype? {is_datetime_before}")
        #print(f"Is 'dates' column datetime64 dtype? {is_datetime_after}")

        print('after cleaning: ', df.info())
        #returning the dataframe 
        return df 


    def convert_weights_to_kg(self):
        
        """
        This method retrieves a CSV of products from s3 using the extract_from_s3 method of the DataExtractor class 
        Then creates a new column in the df 'weight_in_kg' and fills it with the weights from the 'weight' column converted to kg 
        It also removes any rows with missing data, and removes rows with incorrect data 
        Args: 
            None 

        Returns: 
            dataframe: A dataframe with the new 'weight_in_kg' column 

        """

        #LOGGING start of method 
        print('started convert_weights_to_kg')

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

        # dropping any rows which contain missing values 
        df = df.dropna(axis=0)

        # identifying and dropping incorrect values 
        category_pattern = re.compile(r'^[a-zA-Z\-]+$')

        # filters the data frame based on the result of the boolean, where only items matching the pattern are included 
        df = df[df['category'].apply(lambda x: bool(category_pattern.match(x)))]
   
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
        
        # drop unwanted columns 
        df = df.drop('1', axis=1)
        df = df.drop('first_name', axis=1)
        df = df.drop('last_name', axis=1)
        
        # return the cleaned df 
        return df 

    def clean_date_events(self):
        """
        This method retrieves the 'date_events' from an S3 resource using the DataExtractor's class 'extract_from_s3' 
        It then cleans the 'year' format 
        
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
        
        # define the regex pattern for cleaning the year 
        year_regex = r'^\d{4}$'
        
        # apply the regex pattern to clean the year 
        df = df[df['year'].str.match(year_regex)]
 
        # return the cleaned dataframe 
        return df 




# TESTING / CALLING CODE 

#CREATING INSTANCES 

# creating data cleaning instance needed for running the methods in this class 
datacleaning_instance = DataCleaning() 

# creating database connector instance needed for running the methods in database_utils file  
databaseconnector_instance = DatabaseConnector() 

# # LEGACY USER DATA 

# fetching and cleaning legacy users data 
clean_legacy_users_df = datacleaning_instance.clean_legacy_users_data() 

# uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_legacy_users_df, 'dim_users')

# #CARD DATA 

# fetching and cleaning card data 
clean_card_data_df = datacleaning_instance.clean_card_data() 

# uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_card_data_df, 'dim_card_details')

# STORE DETAILS 

# fetching and cleaning card data 
clean_store_data_df = datacleaning_instance.cleaning_store_details()

# uploading store_details data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_store_details' 
databaseconnector_instance.upload_to_db(clean_store_data_df, 'dim_store_details')

# # WEIGHT TO KG (CLEAN PRODUCTS TABLE)

# # fetching and cleaning card data 
clean_weights_df = datacleaning_instance.convert_weights_to_kg()

# # uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_weights_df, 'dim_products')

# # ORDERS TABLE  

# fetching and cleaning card data 
clean_orders_df = datacleaning_instance.clean_orders_data()

# # uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_orders_df, 'orders_table')

# # DATE EVENTS  

# # fetching and cleaning card data 
clean_date_events_df = datacleaning_instance.clean_date_events()

# # uploading legacy users data to database, using 'upload_to_db method of DatabaseConnector class, and called the legacy users data 'dim_users' 
databaseconnector_instance.upload_to_db(clean_date_events_df, 'dim_date_times')



# OLD BELOW 





#databaseconnector_instance.upload_to_db(cleaned_orders_df, 'orders_table')

# ----

# cleaning the text fields in the legacy users table 

#databaseconnector_instance.upload_to_db(cleaned_orders_df, 'orders_table')

#uploading the cleaned date events to the SQL database 
#cleaned_date_events_df = datacleaning_instance.clean_date_events()

#uploading the cleaned order details to the SQL database 
#databaseconnector_instance.upload_to_db(cleaned_date_events_df, 'dim_date_times')
#databaseconnector_instance.upload_to_db(cleaned_date_events_df, 'dim_date_times')





#Code to clean addresses if needed 

#cleaning special characters and punctuation from addresses 
#address_before_transformation = lower_case_df.iloc[0]['address']
#print('this is address before transformation', address_before_transformation)
#lower_case_df['address'] = lower_case_df['address'].str.replace('ß', 's')
#lower_case_df['address_converted'] = lower_case_df['address'].str.replace(r'[^a-zA-Z\s]', '', regex=True)
#address_after_transformation = lower_case_df.iloc[0]['address_converted']
#print('this is address after transformation', address_after_transformation)

#print before and after examples to check it worked 
#after_lower_case_example = lower_case_df.iloc[0]['first_name']
#print('again, this is the name before transformation:', before_lower_case_example)
#print('this is the name after transformation:', after_lower_case_example)
