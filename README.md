# Centralisation of data for a multinational retail organisation 

## Project background 

This project simulates a data pipeline project for a multinational retail company that sells various goods across the globe.

### Problem
The company's sales data was spread across many different data sources making it not easily accessible or analysable by current members of the team. This leads to poor insight into performance and an inability to deliver data-driven strategy. 

### Solution 
Enable the company to become more data-driven by making the organisation's sales data accessible from one centralised location and creating one 'single source of truth'. Then create and run queries on the database to get up-to-date insights for the business.

### How 
To achieve this, I wrote code that brought together various data from an RDS database and different S3 buckets. I combined these data sources together into pandas dataframes, performed data transformations, stored them in a local postgres database, and ran a series of queries to deliver sales insights. 

## Installation instructions

The project requires the following packages to run:  

* boto3
* dateutil
* dotenv
* io
* IPython
* logging
* numpy
* os
* pandas
* re
* requests
* sqlalchemy
* tabula
* time
* unidecode
* urllib.parse

The project also relies on the creation of an .env file (not included in this repo). This contains the following details: 

Credentials for both the RDS and local postgres databases: 
- Host
- Password
- User name
- Database name
- Port

API endpoints for 
- Retrieving the number of retail stores
- Retrieiving the store details for each of the stores

S3 URLs for:  
- Sales date data
- Sales product data
- Credit card data 

## Usage instructions
* Ensure all packages are downloaded 
* Ensure the creation of an appropriate .env file
* Run data_cleaning.py to extract and place the data in the postgres database
* Run data_casting.py to perform the data transformations
* Run data_queries.py to get the results of the queries 

## File structure 

This is the database schema for the project:  

![sales_data_STAR](https://github.com/user-attachments/assets/de458dc9-46e8-4689-96c8-b08914f86637)

To enable this to be created, there are 5 code files: 
1. database_utils.py: this contains a class 'DatabaseConnector' and it's methods. The methods enable connection to the AWS RDS database using boto3, and also uploading the cleaned data to a postgres database.
2. data_extraction.py: this has a class 'DataExtractor' and it's methods. The methods enable the extraction of data from AWS RDS database and S3 buckets. The methods extract the data and turns them into pandas dataframes.
3. data_cleaning.py: this contains the class 'DataCleaning' and its methods. It uses methods from the DataExtractor class to obtain the data, then performs the required cleaning steps.
4. data_casting.py: this takes the data from the postgres database and casts it to the datatypes that are needed for data queries. It also adds primary and foreign keys to the database to create a STAR format (as seen in the schema diagram).     
5. data_queries.py: this runs a series of queries on the data via SQL alchemy. 

## Note
This project was created for personal educational purposes, based on coursework from [AI-Core](https://www.theaicore.com/). It is not intended for production use.

