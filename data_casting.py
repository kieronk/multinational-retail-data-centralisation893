import pandas as pd
from database_utils import DatabaseConnector
import re 
from sqlalchemy import create_engine, text, insert 
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import SQLAlchemyError


# HELPER FUNCTIONS: these functions all support the data casting and analysis 

 
def fetch_data(connection, table_name, limit=5):
    """
        This function fetches data from the specified table in the local SQL database
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of data to fetch  
            limit: number of rows to display from the database 

        Returns: 
            Results of query  
    """
    fetch_data_query = f"SELECT * FROM {table_name} LIMIT {limit};"
    result = connection.execute(text(fetch_data_query))
    return result.fetchall()

 
def check_column_type(connection, table_name, column_name):
    """
        This function checks the column data type to see if the column type has been correctly converted
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table  
            column_name: the name of the column 

        Returns: 
            Results of query  
    """
    
    check_column_type_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND column_name = '{column_name}';
    """
    result = connection.execute(text(check_column_type_query))
    return result.fetchone()

def get_max_length(connection, table_name, column_name):
    """
        This function determines the maximum length of the longest record in the specified column, e.g. the length of the longest string in a given column 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table  
            column_name: the name of the column   

        Returns: 
            If possible, the length of the longest record in a column. 
            It will return a error message if this cannot be round and set 255 as a default max_length  
    """
    try:
        max_length_sql = f"""
        SELECT MAX(LENGTH(CAST("{column_name}" AS TEXT))) AS max_length
        FROM {table_name};
        """
        result = connection.execute(text(max_length_sql)).fetchone()
        
        if result and result[0] is not None:
            return result[0]
        else:
            print(f"Warning: {table_name}.{column_name} has no non-null values or an error occurred.")
            return 255  # Default length
    except Exception as e:
        print(f"Error retrieving max length for {table_name}.{column_name}: {e}")
        return 255  # Default length in case of error
 
def remove_pound_symbol(connection, table_name, column_name):
    """
        This function removes the pound symbol (£) and replaces it with an empty string
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table  
            column_name: the name of the column   

        Returns: 
            Nothing  
    """
    remove_pound_sql = f"""
    UPDATE {table_name}
    SET {column_name} = REPLACE({column_name}, '£', '')
    WHERE {column_name} LIKE '£%';
    """
    connection.execute(text(remove_pound_sql))

 
def add_weight_categories(connection, table_name, weight_column, new_column):
    """
        This function adds a new column of weight categories to the database based on the weights in an existing weight column
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table  
            column_name: the name of the column
            weight_column: the name of the original column that contains the weights  
            new_column: the name you want to give to the new column that is created    

        Returns: 
            Nothing  
    """
    add_column = f"""
    ALTER TABLE {table_name}
    ADD COLUMN {new_column} VARCHAR(20)
    """
    connection.execute(text(add_column))  
    
    update_weights = f"""
    UPDATE {table_name}
    SET {new_column} = CASE
        WHEN {weight_column} < 2 THEN 'Light'
        WHEN {weight_column} >= 2 AND weight_in_kg < 40 THEN 'Mid_Sized'
        WHEN {weight_column} >= 40 AND weight_in_kg < 140 THEN 'Heavy'
        WHEN {weight_column} >= 140 THEN 'Truck_Required'
        ELSE 'Unknown' 
    END;
    """
    connection.execute(text(update_weights))

def add_primary_key(connection, table_name, column_name):       
    """
        This function attempts to add a primary key to a specified table by following these steps: 
        1. Checks if a primary key already exists, if so, it returns a print statement stating this and exists the function   
        2. Removes rows with NULL values
        3. Checks the column has unique values, if not it returns a print statement stating this  
        4. Adds the primary key 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table  
            column_name: the name of the column   

        Returns: 
            A print statement stating whether a primary key has been added or not   
    """
    # Check if the primary key constraint already exists
    check_pk_sql = f"""
    SELECT constraint_name
    FROM information_schema.table_constraints
    WHERE table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY';
    """
    result = connection.execute(text(check_pk_sql))
    pk_exists = result.fetchone() is not None
    
    if pk_exists:
        print(f"Primary key already exists for {table_name}, skipping addition.")
        return
    
    # Remove rows with null values in the column
    remove_nulls_sql = f"""
    DELETE FROM {table_name}
    WHERE {column_name} IS NULL;
    """
    connection.execute(text(remove_nulls_sql))
        
    # Ensure the column has unique values
    check_unique_sql = f"""
    SELECT {column_name}, COUNT(*) 
    FROM {table_name}
    GROUP BY {column_name} 
    HAVING COUNT(*) > 1; 
    """
    result = connection.execute(text(check_unique_sql))
    
    duplicates = [row[0] for row in result]
    
    if duplicates:
        print("Duplicates found, cannot add primary key.")
    else:
        # Create constraint name  
        constraint_name = f"{table_name}_pk"

        # Add primary key to specified table 
        add_pk_sql = f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT {constraint_name} PRIMARY KEY ({column_name});
        """
        connection.execute(text(add_pk_sql))

        print(f"Primary key added to {table_name} on column {column_name}.")

def add_foreign_key(connection, table_name, column_name, referenced_table, referenced_column):
    """
        This function attempts to add a foreign key to a specified column in a table by following these steps: 
        1. Checks if the foreign key constraint already exists for the specific column, if so, it returns a print statement stating this    
        2. Removes rows with NULL values
        3. Creates a constraint name   
        4. Adds the foreign key  
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table  
            column_name: the name of the column   

        Returns: 
            A print statement stating whether a foreign key has been added or not   
    """
    # Check if the foreign key constraint already exists for the specific column
    check_fk_sql = f"""
    SELECT tc.constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_name = '{table_name}' AND kcu.column_name = '{column_name}' AND tc.constraint_type = 'FOREIGN KEY';
    """
    result = connection.execute(text(check_fk_sql))
    fk_exists = result.fetchone() is not None
    
    if fk_exists:
        print(f"Foreign key already exists for {table_name}.{column_name}, skipping addition.")
        return
    
    # Remove rows with null values in the column
    remove_nulls_sql = f"""
    DELETE FROM {table_name}
    WHERE {column_name} IS NULL;
    """
    connection.execute(text(remove_nulls_sql))
    
    # Create constraint name  
    constraint_name = f"{table_name}_{column_name}_fk"

    # Add foreign key to specified table 
    add_fk_sql = f"""
    ALTER TABLE {table_name}
    ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name}) REFERENCES {referenced_table} ({referenced_column});
    """
    connection.execute(text(add_fk_sql))

    print(f"Foreign key added to {table_name}.{column_name} referencing {referenced_table}.{referenced_column}.")

def get_primary_keys(connection, table_name):
    """
        This function gets the primary keys for a specified table   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table   

        Returns: 
            Primary keys   
    """
    inspector = inspect(connection)
    primary_keys = inspector.get_pk_constraint(table_name)['constrained_columns']
    return primary_keys

def get_foreign_keys(connection, table_name):
    """
        This function gets the foreign keys for a specified table  
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table   

        Returns: 
            Foreign keys   
    """
    inspector = inspect(connection)
    foreign_keys = inspector.get_foreign_keys(table_name)
    return foreign_keys


def print_invalid_rows(connection, table_name, column_name, regex_pattern):
    """
        This function gets the foreign keys for a specified table  
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column
            regex_pattern: regex pattern to be used for finding invalid rows in a specified table and column   

        Returns: 
            If items are found that don't match the regex pattern, it returns a list of these     
    """
    find_invalid_sql = f"""
    SELECT "{column_name}"
    FROM "{table_name}"
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    
    result = connection.execute(text(find_invalid_sql)).fetchall()
    if result:
        print(f"Rows in {table_name}.{column_name} to be set to NULL:")
        for row in result:
            print(row)


# CLEANING FUNCTIONS: these functions all clean different types of data 

def clean_text_data(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r"^[A-Za-z ._''-]+$"
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_text_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_text_sql))

def clean_numbers(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^[-]?[0-9]*\.?[0-9]+$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_numbers_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_numbers_sql))

def clean_card_number(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^[0-9]+$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_card_number_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_card_number_sql))

def clean_ean(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^[0-9]+$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_ean_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_ean_sql))

def clean_exp_date(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^\d{2}/\d{2}$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_exp_date_sql = f"""
    UPDATE {table_name}
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_exp_date_sql))

def clean_store_code(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^[A-Za-z0-9]+-[A-Za-z0-9]+$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_store_code_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_store_code_sql))

def clean_product_code(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^[a-zA-Z0-9][a-zA-Z0-9]-[a-zA-Z0-9]+$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_product_code_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE NOT (CAST("{column_name}" AS TEXT) ~ '{regex_pattern}');
    """
    connection.execute(text(clean_product_code_sql))

def clean_uuid(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they don't match the regex pattern.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    regex_pattern = r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
    print_invalid_rows(connection, table_name, column_name, regex_pattern)
    clean_uuid_sql = f"""
    UPDATE {table_name}
    SET {column_name} = NULL
    WHERE TRIM(CAST({column_name} AS TEXT)) !~* '{regex_pattern}';
    """  
    connection.execute(text(clean_uuid_sql))

def clean_date_data(connection, table_name, column_name):
    """
        This function sets values from the specific column in a table to NULL if they can't be converted to the specific date format.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    clean_date_sql = f"""
    UPDATE {table_name}
    SET {column_name} = TO_DATE({column_name}, 'YYYY-MM-DD')
    WHERE {column_name} IS NOT NULL;
    """
    connection.execute(text(clean_date_sql))


# CONVERTING FUNCTIONS: function to cast different datatypes 

def convert_to_uuid(connection, table_name, column_name): 
    """
        This function converts a specified column to UUID format   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    convert_date_uuid = f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name} TYPE UUID
        USING {column_name}::UUID;
        """
    connection.execute(text(convert_date_uuid))
 
def convert_to_varchar(connection, table_name, column_name, length):
    """
        This function convert a specified column in a table to VARCHAR with the specified length  
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column 
            length: length to which the VARCHAR should be set (e.g. 255)  

        Returns: 
            Nothing      
    """
    convert_to_var_sql = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN "{column_name}" TYPE VARCHAR({length}) USING CAST("{column_name}" AS VARCHAR({length})),
    ALTER COLUMN "{column_name}" DROP NOT NULL;
    """
    connection.execute(text(convert_to_var_sql))

def convert_to_smallint(connection, table_name, column_name): 
    """
        This function converts data in a specified column in a table to smalint    
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    convert_small_int = f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name} TYPE SMALLINT
        USING CAST({column_name} AS SMALLINT);
        """
    connection.execute(text(convert_small_int))

def convert_to_date(connection, table_name, column_name): 
    """
        This function converts data in a specified column in a table to date format    
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    convert_date_sql = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name} TYPE DATE
    USING {column_name}::DATE;
    """
    connection.execute(text(convert_date_sql))

# Create function to convert date to smalint 
def convert_to_float(connection, table_name, column_name): 
    """
        This function converts data in a specified column in table to float format   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    convert_float_sql = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name} TYPE FLOAT
    USING {column_name}::FLOAT;
    """
    connection.execute(text(convert_float_sql))
 
def convert_to_boolean(connection, table_name, column_name, new_column_name, condition_1, condition_2):
    """
        This function converts the values in specified column in a table to boolean and adds them to a new column. It has the following steps:   
        1. Checks if the specific column name exists already, if not it adds it.
        2. Converts the values in a specified column to boolean based on the specific conditions, then fills the new column with the boolean values.   
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column with the values that should be converted 
            new_column_name: name of the new column to which the boolean values should be added  
            condition_1: the condition in which the boolean should be set to TRUE
            condition_2: the condition in which the boolean should be set to FALSE 

        Returns: 
            Nothing      
    """

    # Check if the new column already exists
    check_column_sql = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND column_name = '{new_column_name}';
    """
    result = connection.execute(text(check_column_sql)).fetchone()

    # If the column does not exist, add it
    if not result:
        add_column_sql = f"""
        ALTER TABLE {table_name}
        ADD COLUMN {new_column_name} BOOLEAN;
        """
        connection.execute(text(add_column_sql))
    
    convert_boolean_sql = f"""
    UPDATE {table_name}
    SET {new_column_name} = CASE 
        WHEN {column_name} = '{condition_1}' THEN TRUE 
        WHEN {column_name} = '{condition_2}' THEN FALSE 
        ELSE NULL 
    END; 
    """ 
    connection.execute(text(convert_boolean_sql))

# FUNCTIONS TO CLEAN AND CONVERT: These functions bring together the cleaning and casting functions into 1 function for ease of reading  

def num_to_varchar_any(connection, table_name, column_name):
    """
        This function cleans and converts an integer or float to any varchar (must be a number with no special characters)    
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_numbers(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name) 
    convert_to_varchar(connection, table_name, column_name, length)

def text_to_varchar_any(connection, table_name, column_name):
    """
        This function cleans and convert any text to varchar which is determined by max length of the text (no special characters in text)    
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_text_data(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, length)

def text_to_varchar_255(connection, table_name, column_name):
    """
        This function cleans and converts any text to varchar 255 (no special characters in text)  
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_text_data(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, 255)

def store_code_to_varchar(connection, table_name, column_name):
    """
        This function cleans and converts store_code to varchar 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_store_code(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name) 
    convert_to_varchar(connection, table_name, column_name, length)

def exp_to_varchar_any(connection, table_name, column_name):
    """
        This function cleans and converts expiry data to varchar 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_exp_date(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name) 
    convert_to_varchar(connection, table_name, column_name, length)

def text_uuid_to_uuid(connection, table_name, column_name):
    """
        This function cleans and converts any text UUID to actual UUID
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_uuid(connection, table_name, column_name)
    convert_to_uuid(connection, table_name, column_name)

def text_date_to_date(connection, table_name, column_name):
    """
        This function cleans and converts any text date to dateformat 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_date_data(connection, table_name, column_name)
    convert_to_date(connection, table_name, column_name)

def bigint_to_smallint(connection, table_name, column_name):
    """
        This function cleans and converts bigint to small int
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_numbers(connection, table_name, column_name)
    convert_to_smallint(connection, table_name, column_name)

def text_to_float(connection, table_name, column_name):
    """
        This function cleans and converts text data to float format 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_numbers(connection, table_name, column_name)
    convert_to_float(connection, table_name, column_name)

def card_num_to_varchar(connection, table_name, column_name):
    """
        This function cleans and converts card number to varchar (their inconsistency makes it useful to have a specific function for them) 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_card_number(connection, table_name, column_name)
    max_length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, max_length)


def ean_to_varchar(connection, table_name, column_name):
    """
        This function cleans and converts EAN column to var char (their inconsistency makes it useful to have a specific function for them) 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_ean(connection, table_name, column_name)
    max_length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, max_length)

def product_to_varchar(connection, table_name, column_name):
    """
        This function cleans and converts product codes to var char (their inconsistency makes it useful to have a specific function for them) 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_product_code(connection, table_name, column_name)
    max_length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, max_length)

def text_to_boolean(connection, table_name, column_name, new_column_name, condition_1, condition_2): 
    """
        This function cleans and converts text to boolean 
        
        Args: 
            connection: connection to the database (i.e. SQL Alchemy engine)
            table_name: name of table
            column_name: name of column with the values that should be converted 
            new_column_name: name of the new column to which the boolean values should be added  
            condition_1: the condition in which the boolean should be set to TRUE
            condition_2: the condition in which the boolean should be set to FALSE   

        Returns: 
            Nothing      
    """
    connection = connection 
    table_name = table_name
    column_name = column_name
    new_column_name = new_column_name
    condition_1 = condition_1
    condition_2 = condition_2
    clean_text_data(connection, table_name, column_name)
    convert_to_boolean(connection, table_name, column_name, new_column_name, condition_1, condition_2)

# Function to run all operations
def run_all_operations():
    """
        This function runs all the functions for casting the data. It has the following steps: 
        1. Creates and instance of DatabaseConnector() in order that it can use the init_my_db_engine() method of that class 
        2. Creates the SQL engine using init_my_db_engine
        3. Connects to the database and ensures the transaction if commited 
        4. In a try / expect block 
            Attempts to run the cleaning and converting function
            Adds primary keys to the 'orders_table'
            Adds foreign kyes to the other tables 
            Prints the primary and foreign keys 
            If the functions couldn't be run, it prints an error message with an error code 
        6. Prints a message to confirm the function has been run     
        
        Args: 
            None    

        Returns: 
            Nothing      
    """
    
    # Create instance of a DatabaseConnector  
    instance = DatabaseConnector() 
    # Create an engine by using the init_my_db_engine() method of DatabaseConnector 
    engine = instance.init_my_db_engine()
    
    #try to do engine.connect() 
    with engine.connect() as connection:

        # Ensure the transaction is committed 
        with connection.begin():      

            #put the attempt to run the functions in a try block 
            try:
                
                # ORDERS TABLE                 
                text_uuid_to_uuid(connection, 'orders_table', 'date_uuid')
                text_uuid_to_uuid(connection, 'orders_table', 'user_uuid')
                card_num_to_varchar(connection,'orders_table', 'card_number')
                store_code_to_varchar(connection, 'orders_table', 'store_code')
                store_code_to_varchar(connection, 'orders_table', 'store_code')
                bigint_to_smallint(connection, 'orders_table', 'product_quantity')

                # DIM USERS TABLE 
                text_to_varchar_255(connection, 'dim_users', 'first_name')
                text_to_varchar_255(connection, 'dim_users', 'last_name')
                text_date_to_date(connection, 'dim_users', 'date_of_birth')
                text_to_varchar_any(connection, 'dim_users', 'country_code')
                text_uuid_to_uuid(connection, 'dim_users', 'user_uuid')
                text_date_to_date(connection, 'dim_users', 'join_date')

                # DIM_STORE_DETAILS
                text_to_float(connection, 'dim_store_details', 'longitude')
                text_to_varchar_255(connection, 'dim_store_details', 'locality')            
                store_code_to_varchar(connection, 'dim_store_details', 'store_code')
                bigint_to_smallint(connection, 'dim_store_details', 'staff_numbers')
                text_date_to_date(connection, 'dim_store_details', 'opening_date')
                text_to_varchar_255(connection, 'dim_store_details', 'store_type') 
                text_to_float(connection, 'dim_store_details', 'latitude')
                text_to_varchar_any(connection, 'dim_store_details', 'country_code')
                text_to_varchar_255(connection, 'dim_store_details', 'continent')

                # DIM PRODUCTS 
                remove_pound_symbol(connection, 'dim_products', 'product_price')
                add_weight_categories(connection, 'dim_products', 'weight_in_kg', 'weight_category')
                text_to_float(connection, 'dim_products', 'product_price')
                text_to_float(connection, 'dim_products', 'weight_in_kg')
                ean_to_varchar(connection, 'dim_products', 'EAN')
                product_to_varchar(connection, 'dim_products', 'product_code')
                text_date_to_date(connection, 'dim_products', 'date_added')          
                text_uuid_to_uuid(connection, 'dim_products', 'uuid')
                text_to_boolean(connection, 'dim_products', 'removed', 'is_removed', 'Still_avaliable', 'Removed')

                # DIM DATE TIMES 
                num_to_varchar_any(connection, 'dim_date_times', 'day')
                num_to_varchar_any(connection, 'dim_date_times', 'year')
                num_to_varchar_any(connection, 'dim_date_times', 'month')
                text_to_varchar_any(connection, 'dim_date_times', 'time_period')
                text_uuid_to_uuid(connection, 'dim_date_times', 'date_uuid')

                # DIM CARD DETAILS 
                exp_to_varchar_any(connection, 'dim_card_details', 'expiry_date')
                card_num_to_varchar(connection,'dim_card_details', 'card_number')
                text_date_to_date(connection,'dim_card_details', 'date_payment_confirmed')
                add_primary_key(connection, 'dim_card_details', 'card_number')
                add_primary_key(connection, 'dim_date_times', 'date_uuid')
                add_primary_key(connection, 'dim_products', 'product_code')
                add_primary_key(connection, 'dim_store_details', 'store_code')
                add_primary_key(connection, 'dim_users', 'user_uuid')

                # adding foreign keys 
                add_foreign_key(connection, 'orders_table', 'card_number', 'dim_card_details', 'card_number')
                add_foreign_key(connection, 'orders_table', 'date_uuid', 'dim_date_times', 'date_uuid')
                add_foreign_key(connection, 'orders_table', 'product_code', 'dim_products', 'product_code')
                add_foreign_key(connection, 'orders_table', 'store_code', 'dim_store_details', 'store_code')
                add_foreign_key(connection, 'orders_table', 'user_uuid', 'dim_users', 'user_uuid')

                # view primary keys 
                primary_keys = get_primary_keys(connection, 'orders_table')
                print(f"Primary keys for table 'orders_table': {primary_keys}")

                # view foreign keys
                foreign_keys = get_foreign_keys(connection, 'orders_table')
                print(f"Foreign keys for table 'orders_table': {foreign_keys}")

            except SQLAlchemyError as e:
                print(f"An error occurred: {e}")

    print('End of call')

run_all_operations()