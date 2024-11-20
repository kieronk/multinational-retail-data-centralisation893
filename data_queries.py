import pandas as pd
from database_utils import DatabaseConnector
import re 
from sqlalchemy import create_engine, text, insert 
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import SQLAlchemyError

# DEFINING THE QUERIES 

# Function to get total stores per country
def get_total_stores_per_country(connection):
    query = """
    SELECT country_code, COUNT(store_code) AS total_stores
    FROM dim_store_details
    GROUP BY country_code;
    """
    return connection.execute(text(query)).fetchall()

# Function to get locations with the most stores
def get_locations_with_most_stores(connection):
    query = """
    SELECT locality, COUNT(store_code) AS total_stores
    FROM dim_store_details
    GROUP BY locality
    ORDER BY total_stores DESC;
    """
    return connection.execute(text(query)).fetchall()

# Function to get months that produced the largest amount of sales
def get_months_with_largest_sales(connection):
    query = """
    SELECT ddt.month, SUM(ot.product_quantity * dp.product_price) AS total_sales
    FROM Orders_table ot
    JOIN dim_products dp ON ot.product_code = dp.product_code
    JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
    GROUP BY ddt.month
    ORDER BY total_sales DESC;
    """
    return connection.execute(text(query)).fetchall()

# Function to get total sales per month
def get_total_sales_per_month(connection):
    query = """
    SELECT ddt.month, SUM(ot.product_quantity * dp.product_price) AS total_sales
    FROM Orders_table ot
    JOIN dim_products dp ON ot.product_code = dp.product_code
    JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
    GROUP BY ddt.month
    ORDER BY ddt.month;
    """
    return connection.execute(text(query)).fetchall()

# Function to get sales coming from online vs offline
def get_sales_online_vs_offline(connection):
    query = """
    SELECT 
        CASE 
            WHEN dsd.locality = 'online' THEN 'web'
            ELSE 'Offline'
        END AS locality_group,
        SUM(ot.product_quantity) AS product_quantity_count,
        COUNT(ot.date_uuid) AS number_of_sales
    FROM orders_table ot
    JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
    GROUP BY locality_group;
    """
    return connection.execute(text(query)).fetchall()

# Function to get percentage of sales from each store type
def get_sales_percentage_by_store_type(connection):
    query = """
    WITH total_sales_by_store_type AS (
        SELECT 
            dsd.store_type,
            SUM(ot.product_quantity * dp.product_price) AS total_sales
        FROM orders_table ot
        JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
        JOIN dim_products dp ON ot.product_code = dp.product_code 
        GROUP BY dsd.store_type
    ),
    total_sales_overall AS (
        SELECT SUM(total_sales) AS overall_sales FROM total_sales_by_store_type
    )
    SELECT 
        tss.store_type,
        tss.total_sales,
        (tss.total_sales / tso.overall_sales) * 100 AS percentage_total
    FROM total_sales_by_store_type tss,
    total_sales_overall tso;
    """
    return connection.execute(text(query)).fetchall()

# Function to get highest total sales by month and year
def get_highest_sales_by_month_and_year(connection):
    query = """
    SELECT 
        ddt.year,
        ddt.month,
        SUM(ot.product_quantity * dp.product_price) AS total_sales
    FROM orders_table ot
    JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
    JOIN dim_products dp ON ot.product_code = dp.product_code 
    GROUP BY ddt.year, ddt.month
    ORDER BY total_sales DESC;
    """
    return connection.execute(text(query)).fetchall()

# Function to get staff numbers per country
def get_staff_numbers_per_country(connection):
    query = """
    SELECT country_code, SUM(staff_numbers) AS total_staff
    FROM dim_store_details
    GROUP BY country_code;
    """
    return connection.execute(text(query)).fetchall()

# Function to get total sales per store type in Germany
def get_sales_per_store_type_in_germany(connection):
    query = """
    SELECT 
        dsd.country_code,
        dsd.store_type,
        SUM(ot.product_quantity * dp.product_price) AS total_sales
    FROM orders_table ot
    JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
    JOIN dim_products dp ON ot.product_code = dp.product_code 
    WHERE dsd.country_code = 'DE'
    GROUP BY dsd.store_type, dsd.country_code;
    """
    return connection.execute(text(query)).fetchall()

# Function to calculate how quickly the company is making sales
def get_sales_speed(connection):
    query = """
    WITH time_differences AS (
        SELECT 
            EXTRACT(YEAR FROM ddt.complete_timestamp) AS year,
            LEAD(ddt.complete_timestamp) OVER (
                PARTITION BY EXTRACT(YEAR FROM ddt.complete_timestamp) 
                ORDER BY ddt.complete_timestamp
            ) - ddt.complete_timestamp AS time_diff
        FROM dim_date_times ddt
    )
    SELECT 
        year,
        TO_CHAR(
            INTERVAL '1 second' * AVG(EXTRACT(EPOCH FROM time_diff)),
            'HH24 "hours", MI "minutes", SS "seconds"'
        ) AS actual_time_taken
    FROM time_differences
    GROUP BY year
    ORDER BY AVG(EXTRACT(EPOCH FROM time_diff)) DESC;
    """
    return connection.execute(text(query)).fetchall()



# RUNNING THE QUERIES 

# Create instance of DatabaseConnector and engine
instance = DatabaseConnector() 
engine = instance.init_db_engine(prefix="DB") 

# Use the connection context to run the queries
with engine.connect() as connection:
    # Run each query and get the results
    total_stores_per_country = get_total_stores_per_country(connection)
    locations_with_most_stores = get_locations_with_most_stores(connection)
    months_with_largest_sales = get_months_with_largest_sales(connection)
    total_sales_per_month = get_total_sales_per_month(connection)
    sales_online_vs_offline = get_sales_online_vs_offline(connection)
    sales_percentage_by_store_type = get_sales_percentage_by_store_type(connection)
    highest_sales_by_month_and_year = get_highest_sales_by_month_and_year(connection)
    staff_numbers_per_country = get_staff_numbers_per_country(connection)
    sales_per_store_type_in_germany = get_sales_per_store_type_in_germany(connection)
    sales_speed = get_sales_speed(connection)

    # Print the results of each query
    print("\nTotal stores per country:")
    for row in total_stores_per_country:
        print(row)

    print("\nLocations with the most stores:")
    for row in locations_with_most_stores:
        print(row)

    print("\nMonths that produced the largest amount of sales:")
    for row in months_with_largest_sales:
        print(row)

    print("\nTotal sales per month:")
    for row in total_sales_per_month:
        print(row)

    print("\nSales coming from online vs offline:")
    for row in sales_online_vs_offline:
        print(row)

    print("\nPercentage of sales from each store type:")
    for row in sales_percentage_by_store_type:
        print(row)

    print("\nHighest total sales by month and year:")
    for row in highest_sales_by_month_and_year:
        print(row)

    print("\nStaff numbers per country:")
    for row in staff_numbers_per_country:
        print(row)

    print("\nTotal sales per store type in Germany:")
    for row in sales_per_store_type_in_germany:
        print(row)

    print("\nHow quickly the company is making sales (average time taken):")
    for row in sales_speed:
        print(row)