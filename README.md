#Centralisation of data for retail organisation 
  
Overview 
The project brings together a series of data from different locations and puts them into a postgres database in order that they can be analysed further. 

There are 3 key code files: 
1. data_extraction: this has a class 'DataExtractor' and it's methods. The methods enable the extraction of data from AWS RDS and S3 bucket. The methods extract the data and turns them into dataframes.
2. data_cleaning: this contains the class 'DataCleaning' and its methods. The methods clean each of the data sources take via the DataExtractor class.
3. database_utils: this contains a class 'DatabaseConnector' and it's methods. The methods enable connection to the AWS RDS database using boto3, and also uploading the cleaned data to a postgres database

Note the connection relies on database credentials which are not in this repositry.  

The code requires the following packages to run:  
- python3
- boto3
- pandas
- numpy
- tabula 
- sqlalchemy

