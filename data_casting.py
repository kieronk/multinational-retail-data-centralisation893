import pandas as pd
from database_utils import DatabaseConnector
import re 
from sqlalchemy import create_engine, text, insert 
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import SQLAlchemyError


# HELPER FUNCTIONS: these functions all support the data casting and analysis 

# Create function to fetch data from the table in my local SQL server so that I can get the data to perform checks on it 
def fetch_data(connection, table_name, limit=5):
    fetch_data_query = f"SELECT * FROM {table_name} LIMIT {limit};"
    result = connection.execute(text(fetch_data_query))
    return result.fetchall()

# Function to check the column data type to check if the column type has been correctly converted 
def check_column_type(connection, table_name, column_name):
    check_column_type_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND column_name = '{column_name}';
    """
    result = connection.execute(text(check_column_type_query))
    return result.fetchone()

# Function to determine the maximum length of values in the column
def get_max_length(connection, table_name, column_name):
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

# Function to remove the pound symbol (£) and replace with a empty string 
def remove_pound_symbol(connection, table_name, column_name):
    remove_pound_sql = f"""
    UPDATE {table_name}
    SET {column_name} = REPLACE({column_name}, '£', '')
    WHERE {column_name} LIKE '£%';
    """
    connection.execute(text(remove_pound_sql))

# Adds new column of weight categories to the database 
def add_weight_categories(connection, table_name, weight_column, new_column):
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

# adds primary key to specified table 
def add_primary_key(connection, table_name, column_name):       
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

# Function to identify and print orphan rows
def find_and_report_orphans(connection, table_name, column_name, referenced_table, referenced_column):
    find_orphans_sql = f"""
    SELECT {column_name}
    FROM {table_name}
    WHERE {column_name} IS NOT NULL
    AND {column_name} NOT IN (SELECT {referenced_column} FROM {referenced_table});
    """
    result = connection.execute(text(find_orphans_sql))
    orphans = result.fetchall()
    
    orphan_count = len(orphans)
    
    if orphan_count > 0:
        print(f"Found {orphan_count} orphaned records in {table_name} and column {column_name}:")
        for orphan in orphans:
            print(orphan)
    else:
        print(f"No orphaned records found in {table_name} and column {column_name}.")
    
    return orphan_count, orphans

# Function to remove any orphaned rows identified via 'find and report orphan records' 
def remove_orphans(connection, table_name, column_name, referenced_table, referenced_column):
    # Find and report orphaned records
    orphan_count, orphans = find_and_report_orphans(connection, table_name, column_name, referenced_table, referenced_column)
    
    if orphan_count > 0:
        # Remove orphaned records
        remove_orphans_sql = f"""
        DELETE FROM {table_name}
        WHERE {column_name} IS NOT NULL
        AND {column_name}::text NOT IN (SELECT {referenced_column}::text FROM {referenced_table});
        """
        connection.execute(text(remove_orphans_sql))
        print(f"Orphaned records removed from {table_name}.")
    else:
        print(f"No orphaned records to remove from {table_name}.")

# Adds foreign key to specified table and column 
def add_foreign_key(connection, table_name, column_name, referenced_table, referenced_column):
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
    
    # Remove orphaned records
    remove_orphans(connection, table_name, column_name, referenced_table, referenced_column)
    
    # Create constraint name  
    constraint_name = f"{table_name}_{column_name}_fk"

    # Add foreign key to specified table 
    add_fk_sql = f"""
    ALTER TABLE {table_name}
    ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name}) REFERENCES {referenced_table} ({referenced_column});
    """
    connection.execute(text(add_fk_sql))

    print(f"Foreign key added to {table_name}.{column_name} referencing {referenced_table}.{referenced_column}.")

# Gets primary keys for inspection 
def get_primary_keys(connection, table_name):
    inspector = inspect(connection)
    primary_keys = inspector.get_pk_constraint(table_name)['constrained_columns']
    return primary_keys

# Gets foreign keys for inspection 
def get_foreign_keys(connection, table_name):
    inspector = inspect(connection)
    foreign_keys = inspector.get_foreign_keys(table_name)
    return foreign_keys


# CLEANING FUNCTIONS: these functions all clean different types of data 

# Create function to clean uuid with regex 
def clean_uuid(connection, table_name, column_name):
    clean_uuid = f"""
    UPDATE {table_name}
    SET {column_name} = NULL
    WHERE TRIM(CAST({column_name} AS TEXT)) !~* '^[a-f0-9]{{8}}-[a-f0-9]{{4}}-[a-f0-9]{{4}}-[a-f0-9]{{4}}-[a-f0-9]{{12}}$';
    """  
    # Convert the SQL string to a TextClause object and execute the query
    connection.execute(text(clean_uuid))

# Create function to clean numeric data (both integer and floats) data with regex by ensuring they are numbers 
def clean_numbers(connection, table_name, column_name):
    clean_numbers_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^[-]?[0-9]*\\.?[0-9]+$';
    """
    connection.execute(text(clean_numbers_sql))

# Create function to clean card numbers 
def clean_card_number(connection, table_name, column_name):
    clean_card_number_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^[0-9]+$';
    """
    connection.execute(text(clean_card_number_sql))

# Create function to clean EAN
def clean_ean(connection, table_name, column_name):
    clean_ean_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^[0-9]+$';
    """
    connection.execute(text(clean_ean_sql))

# Create function to clean expiry dates 
def clean_exp_date(connection, table_name, column_name):
    clean_numbers_sql = f"""
    UPDATE {table_name}
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^\\d{2}/\\d{2}$';
    """
    connection.execute(text(clean_numbers_sql))

# Create function to clean store codes  
def clean_store_code(connection, table_name, column_name):
    clean_numbers_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^[A-Za-z0-9]+-[A-Za-z0-9]+$';
    """
    connection.execute(text(clean_numbers_sql))

# Create function to clean product codes 
def clean_product_code(connection, table_name, column_name):
    clean_product_code_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^[a-zA-Z0-9][a-zA-Z0-9]-[a-zA-Z0-9]+$';
    """
    connection.execute(text(clean_product_code_sql))

# Create function to text data 
def clean_text_data(connection, table_name, column_name):
    clean_text_sql = f"""
    UPDATE "{table_name}"
    SET "{column_name}" = NULL
    WHERE CAST("{column_name}" AS TEXT) !~ '^[A-Za-z_]+$';
    """
    connection.execute(text(clean_text_sql))

# Create function to clean date data in the form of text 
def clean_date_data(connection, table_name, column_name):
    clean_date_sql = f"""
    UPDATE {table_name}
    SET {column_name} = 
    TO_DATE(
        REGEXP_REPLACE(
            CAST({column_name} AS TEXT), 
            '\\((\\d+), (\\d+), (\\d+), \\d+, \\d+\\)', 
            '\\1-\\2-\\3'
        ), 
        'YYYY-MM-DD'
    )
    WHERE CAST({column_name} AS TEXT) ~ '\\(\\d+, \\d+, \\d+, \\d+, \\d+\\)';
    """
    connection.execute(text(clean_date_sql))


# CONVERTING FUNCTIONS: function to cast different datatypes 

# Create function to convert a specified column to UUID 
def convert_to_uuid(connection, table_name, column_name): 
    convert_date_uuid = f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name} TYPE UUID
        USING {column_name}::UUID;
        """
    # Convert the SQL string to a TextClause object and execute the query
    connection.execute(text(convert_date_uuid))

# Function to convert the column to VARCHAR with the determined maximum length 
def convert_to_varchar(connection, table_name, column_name, length):
    convert_to_var_sql = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN "{column_name}" TYPE VARCHAR({length}) USING CAST("{column_name}" AS VARCHAR({length})),
    ALTER COLUMN "{column_name}" DROP NOT NULL;
    """
    connection.execute(text(convert_to_var_sql))

# Create function to convert bigint to smalint 
def convert_to_smallint(connection, table_name, column_name): 
    convert_date_uuid = f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name} TYPE SMALLINT
        USING CAST({column_name} AS SMALLINT);
        """
    # Convert the SQL string to a TextClause object and execute the query
    connection.execute(text(convert_date_uuid))

# Create function to convert to date 
def convert_to_date(connection, table_name, column_name): 
    convert_date_sql = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name} TYPE DATE
    USING {column_name}::DATE;
    """
    connection.execute(text(convert_date_sql))

# Create function to convert date to smalint 
def convert_to_float(connection, table_name, column_name): 
    convert_date_sql = f"""
    ALTER TABLE {table_name}
    ALTER COLUMN {column_name} TYPE FLOAT
    USING {column_name}::FLOAT;
    """
    connection.execute(text(convert_date_sql))

# Create function to convert column to boolean. It checks if the column exists already, if not it adds it, then fills the column 
def convert_to_boolean(connection, table_name, column_name, new_column_name, condition_1, condition_2):
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

# Create function to clean and convert an integer or float to any varchar (must be a number with no special characters) 
def num_to_varchar_any(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_numbers(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name)
    #print(length) 
    convert_to_varchar(connection, table_name, column_name, length)

# Create function to clean and convert any text to varchar which is determined by max length of the text (no special characters in text)
def text_to_varchar_any(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_text_data(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name)
    #print(length) 
    convert_to_varchar(connection, table_name, column_name, length)

# Create function to clean and convert any text to varchar 255 (no special characters in text)
def text_to_varchar_255(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_text_data(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, 255)

# Create function to clean and convert store code to varchar 
def store_code_to_varchar(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_store_code(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name) 
    convert_to_varchar(connection, table_name, column_name, length)

# Create function to clean and convert expiry data to varchar 
def exp_to_varchar_any(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_exp_date(connection, table_name, column_name)
    length = get_max_length(connection, table_name, column_name)
    #print(length) 
    convert_to_varchar(connection, table_name, column_name, length)

# Create function to clean and convert any text UUID to actual UUID
def text_uuid_to_uuid(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_uuid(connection, table_name, column_name)
    convert_to_uuid(connection, table_name, column_name)

# Create function to clean and convert text date to date 
def text_date_to_date(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_date_data(connection, table_name, column_name)
    convert_to_date(connection, table_name, column_name)

# Create function to clean and convert bigint to small int 
def bigint_to_smallint(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_numbers(connection, table_name, column_name)
    convert_to_smallint(connection, table_name, column_name)

# Create function to clean and convert text to float 
def text_to_float(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_numbers(connection, table_name, column_name)
    convert_to_float(connection, table_name, column_name)

# Create function to clean and convert card number to varchar (their inconsistency makes it useful to have a specific function for them) 
def card_num_to_varchar(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_card_number(connection, table_name, column_name)
    max_length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, max_length)

# Create function to clean and convert EAN column to var char (their inconsistency makes it useful to have a specific function for them) 
def ean_to_varchar(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_ean(connection, table_name, column_name)
    max_length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, max_length)

# Create function to clean and convert product codes to var char (their inconsistency makes it useful to have a specific function for them) 
def product_to_varchar(connection, table_name, column_name):
    connection = connection 
    table_name = table_name
    column_name = column_name
    clean_product_code(connection, table_name, column_name)
    max_length = get_max_length(connection, table_name, column_name)
    convert_to_varchar(connection, table_name, column_name, max_length)

# Create function to clean and convert to boolean 
def text_to_boolean(connection, table_name, column_name, new_column_name, condition_1, condition_2): 
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
                
                # Cleaning then converting the date_uuid
                # clean_uuid(connection, 'orders_table', 'date_uuid')
                # convert_to_uuid(connection, 'orders_table', 'date_uuid')
                text_uuid_to_uuid(connection, 'orders_table', 'date_uuid')
                
                # # Cleaning then converting the user_uuid
                # clean_uuid(connection, 'orders_table', 'user_uuid')
                # convert_to_uuid(connection, 'orders_table', 'user_uuid')
                text_uuid_to_uuid(connection, 'orders_table', 'user_uuid')

                # # Cleaning then converting the card_number
                # clean_numbers(connection, 'orders_table', 'card_number')
                # max_length = get_max_length(connection, 'orders_table', 'card_number')
                # convert_to_varchar(connection, 'orders_table', 'card_number', max_length)
                #num_to_varchar_any(connection, 'orders_table', 'card_number')
                # just deleted 
                # clean_card_number(connection, 'orders_table', 'card_number')
                # max_length = get_max_length(connection, 'orders_table', 'card_number')
                # convert_to_varchar(connection, 'orders_table', 'card_number', max_length)
                card_num_to_varchar(connection,'orders_table', 'card_number')

                # # Cleaning then converting store_code
                # clean_store_or_product_codes(connection, 'orders_table', 'store_code')
                # max_length = get_max_length(connection, 'orders_table', 'store_code')
                # convert_to_varchar(connection, 'orders_table', 'store_code', max_length)
                store_code_to_varchar(connection, 'orders_table', 'store_code')

                # # Cleaning then converting product_code
                # clean_store_or_product_codes(connection, 'orders_table', 'product_code')
                # max_length = get_max_length(connection, 'orders_table', 'product_code')
                # convert_to_varchar(connection, 'orders_table', 'product_code', max_length)
                # #print('Convert product_code worked')
                store_code_to_varchar(connection, 'orders_table', 'store_code')

                # # Cleaning then converting the product_quantity
                # clean_numbers(connection, 'orders_table', 'product_quantity')
                # convert_to_smallint(connection, 'orders_table', 'product_quantity')
                bigint_to_smallint(connection, 'orders_table', 'product_quantity')

                # DIM USERS TABLE 
                # # Cleaning then converting first_name
                # clean_text_data(connection, 'dim_users', 'first_name')
                # convert_to_varchar(connection, 'dim_users', 'first_name', 255)
                text_to_varchar_255(connection, 'dim_users', 'first_name')

                # # Cleaning then converting first_name
                # clean_text_data(connection, 'dim_users', 'last_name')
                # convert_to_varchar(connection, 'dim_users', 'last_name', 255)
                text_to_varchar_255(connection, 'dim_users', 'last_name')

                # # Cleaning then converting date_of_birth
                # clean_date_data(connection, 'dim_users', 'date_of_birth')
                # convert_to_date(connection, 'dim_users', 'date_of_birth')
                text_date_to_date(connection, 'dim_users', 'date_of_birth')

                # # Cleaning then converting country_code 
                # clean_text_data(connection, 'dim_users', 'country_code')
                # max_length = get_max_length(connection, 'dim_users', 'country_code')
                # convert_to_varchar(connection, 'dim_users', 'country_code', max_length)
                text_to_varchar_any(connection, 'dim_users', 'country_code')

                # # Cleaning then converting the user_uuid
                # clean_uuid(connection, 'dim_users', 'user_uuid')
                # convert_to_uuid(connection, 'dim_users', 'user_uuid')
                text_uuid_to_uuid(connection, 'dim_users', 'user_uuid')

                # # Cleaning then converting date_of_birth
                # clean_date_data(connection, 'dim_users', 'join_date')
                # convert_to_date(connection, 'dim_users', 'join_date')
                text_date_to_date(connection, 'dim_users', 'join_date')

                # DIM_STORE_DETAILS

                # # Cleaning then converting longitude
                # clean_numbers(connection, 'dim_store_details', 'longitude')
                # convert_to_float(connection, 'dim_store_details', 'longitude')
                text_to_float(connection, 'dim_store_details', 'longitude')

                # # Cleaning then converting locality 
                # clean_text_data(connection, 'dim_store_details', 'locality')
                # convert_to_varchar(connection, 'dim_store_details', 'locality', 255)
                text_to_varchar_255(connection, 'dim_store_details', 'locality')            

                # Cleaning then converting store_code 
                # clean_store_or_product_codes(connection, 'dim_store_details', 'store_code')
                # max_length = get_max_length(connection, 'dim_store_details', 'store_code')
                # convert_to_varchar(connection, 'dim_store_details', 'store_code', max_length)
                store_code_to_varchar(connection, 'dim_store_details', 'store_code')

                # # Cleaning then converting staff_numbers
                # clean_numbers(connection, 'dim_store_details', 'staff_numbers')
                # convert_to_smallint(connection, 'dim_store_details', 'staff_numbers')
                bigint_to_smallint(connection, 'dim_store_details', 'staff_numbers')

                # # Cleaning then converting opening_date
                # clean_date_data(connection, 'dim_store_details', 'opening_date')
                # convert_to_date(connection, 'dim_store_details', 'opening_date')
                text_date_to_date(connection, 'dim_store_details', 'opening_date')

                # # Cleaning then converting locality 
                # clean_text_data(connection, 'dim_store_details', 'store_type')
                # convert_to_varchar(connection, 'dim_store_details', 'store_type', 255)
                text_to_varchar_255(connection, 'dim_store_details', 'store_type') 

                # # Cleaning then converting longitude
                # clean_numbers(connection, 'dim_store_details', 'latitude')
                # convert_to_float(connection, 'dim_store_details', 'latitude')
                text_to_float(connection, 'dim_store_details', 'latitude')

                # # Cleaning then converting country_code 
                # clean_text_data(connection, 'dim_store_details', 'country_code')
                # max_length = get_max_length(connection, 'dim_store_details', 'country_code')
                # convert_to_varchar(connection, 'dim_store_details', 'country_code', max_length)
                text_to_varchar_any(connection, 'dim_store_details', 'country_code')

                # # Cleaning then converting continent 
                # clean_text_data(connection, 'dim_store_details', 'continent')
                # convert_to_varchar(connection, 'dim_store_details', 'continent', 255)
                text_to_varchar_255(connection, 'dim_store_details', 'continent')

                # DIM PRODUCTS 

                # Removing pound from price column
                remove_pound_symbol(connection, 'dim_products', 'product_price')
                
                # adding weight categories 
                add_weight_categories(connection, 'dim_products', 'weight_in_kg', 'weight_category')

                # # Cleaning product_price 
                # clean_numbers(connection, 'dim_products', 'product_price')
                # convert_to_float(connection, 'dim_products', 'product_price')
                text_to_float(connection, 'dim_products', 'product_price')

                # # Cleaning and converting weigth_in_kg
                # clean_numbers(connection, 'dim_products', 'weight_in_kg')
                # convert_to_float(connection, 'dim_products', 'weight_in_kg')
                text_to_float(connection, 'dim_products', 'weight_in_kg')

                # # Cleaning and converting EAN 
                # clean_numbers(connection, 'dim_products', 'EAN')
                
                # convert_to_varchar(connection, 'dim_products', 'EAN', max_length)
                #text_to_varchar_any(connection, 'dim_products', 'EAN')
                # clean_ean(connection, 'dim_products', 'EAN')
                # max_length = get_max_length(connection, 'dim_products', 'EAN')
                # convert_to_varchar(connection, 'dim_products', 'EAN', max_length)
                ean_to_varchar(connection, 'dim_products', 'EAN')

                
                # # Cleaning and converting product_code
                # clean_store_or_product_codes(connection, 'dim_products', 'product_code')
                # max_length = get_max_length(connection, 'dim_products', 'product_code')
                # convert_to_varchar(connection, 'dim_products', 'product_code', max_length)
                #text_to_varchar_any(connection, 'dim_products', 'product_code')
                # clean_product_code(connection, 'dim_products', 'product_code')
                # max_length = get_max_length(connection, 'dim_products', 'product_code')
                # convert_to_varchar(connection, 'dim_products', 'product_code', max_length)
                product_to_varchar(connection, 'dim_products', 'product_code')

                # # Cleaning then converting date_added 
                # clean_date_data(connection, 'dim_products', 'date_added')
                # convert_to_date(connection, 'dim_products', 'date_added')
                text_date_to_date(connection, 'dim_products', 'date_added')
          
                # # Cleaning then converting the uuid
                # clean_uuid(connection, 'dim_products', 'uuid')
                # convert_to_uuid(connection, 'dim_products', 'uuid')
                text_uuid_to_uuid(connection, 'dim_products', 'uuid')

                # # Cleaning then converting the removed column
                # clean_text_data(connection, 'dim_products', 'removed')
                # convert_to_boolean(connection, 'dim_products', 'removed', 'is_removed', 'Still_avaliable', 'Removed')
                text_to_boolean(connection, 'dim_products', 'removed', 'is_removed', 'Still_avaliable', 'Removed')

                # DIM DATE TIMES 

                # # Cleaning then converting 'dim_date_times', 'day' 
                num_to_varchar_any(connection, 'dim_date_times', 'day')

                # # Cleaning then converting 'dim_date_times', 'year'
                num_to_varchar_any(connection, 'dim_date_times', 'year')

                # # Cleaning then converting 'dim_date_times', 'month' 
                num_to_varchar_any(connection, 'dim_date_times', 'month')

                # # Cleaning then converting 'dim_date_times', 'time_period' 
                text_to_varchar_any(connection, 'dim_date_times', 'time_period')

                # # Cleaning then converting 'dim_date_times', 'date_uuid' 
                text_uuid_to_uuid(connection, 'dim_date_times', 'date_uuid')

                # # Cleaning then converting 'dim_card_details', 'expiry_date'
                exp_to_varchar_any(connection, 'dim_card_details', 'expiry_date')

                # Cleaning then converting ''dim_card_details', 'card_number'
                #num_to_varchar_any(connection, 'dim_card_details', 'card_number')
                #clean_card_number(connection,  'dim_card_details', 'card_number')
                #max_length = get_max_length(connection,  'dim_card_details', 'card_number')
                #convert_to_varchar(connection,  'dim_card_details', 'card_number', max_length)
                card_num_to_varchar(connection,'dim_card_details', 'card_number')
                
                # Cleaning then converting 'dim_card_details', 'date_payment_confirmed'
                text_date_to_date(connection,'dim_card_details', 'date_payment_confirmed')

                # adding primary keys dim tables
                add_primary_key(connection, 'dim_card_details', 'card_number')
                add_primary_key(connection, 'dim_date_times', 'date_uuid')
                add_primary_key(connection, 'dim_products', 'product_code')
                add_primary_key(connection, 'dim_store_details', 'store_code')
                add_primary_key(connection, 'dim_users', 'user_uuid')

                #adding foreign keys 
                add_foreign_key(connection, 'orders_table', 'card_number', 'dim_card_details', 'card_number')
                add_foreign_key(connection, 'orders_table', 'date_uuid', 'dim_date_times', 'date_uuid')
                add_foreign_key(connection, 'orders_table', 'product_code', 'dim_products', 'product_code')
                add_foreign_key(connection, 'orders_table', 'store_code', 'dim_store_details', 'store_code')
                add_foreign_key(connection, 'orders_table', 'user_uuid', 'dim_users', 'user_uuid')

                primary_keys = get_primary_keys(connection, 'orders_table')
                print(f"Primary keys for table 'orders_table': {primary_keys}")

                foreign_keys = get_foreign_keys(connection, 'orders_table')
                print(f"Foreign keys for table 'orders_table': {foreign_keys}")


            except SQLAlchemyError as e:
                print(f"An error occurred: {e}")

            # Check and print column type after conversion
            #column_type_after = check_column_type(connection, 'dim_store_details', 'store_type')
            #print(f"Column type after conversion: {column_type_after}")

    print('End of call')

run_all_operations()