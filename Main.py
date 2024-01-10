# main.py
"""
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

# Initialize the database engine
connector = DatabaseConnector()
engine = connector.init_db_engine("db_creds.yaml")

# Create an instance of DataExtractor with the engine
extractor = DataExtractor(engine)

# List the tables in the database
tables = connector.list_db_tables(engine)
print(f"Tables in the database: {tables}")

# Assume you want to read data from a table named 'sales'
table_name = 'legacy_store_details'
data = extractor.read_table_data(table_name)
print(f"Data from table '{table_name}':\n{data}")
"""

# main.py

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Initialize the database engine
connector = DatabaseConnector()
connector.engine = connector.init_db_engine("db_creds.yaml")  # Make sure this line correctly initializes the engine

# Create an instance of DataExtractor with the DatabaseConnector instance
extractor = DataExtractor(connector)

# List the tables in the database
tables = connector.list_db_tables(connector.engine)
print(f"Tables in the database: {tables}")

# Define the table name that contains user data (adjust the table name as needed)
user_data_table_name = "legacy_users"  # Update this to the actual user data table name

# Use the read_rds_table method to read the user data table
try:
    user_data_df = extractor.read_rds_table(user_data_table_name)
    print(f"Data from table '{user_data_table_name}':\n{user_data_df}")
except ValueError as e:
    print(e)


data_cleaner = DataCleaning()
cleaned_user_data = data_cleaner.clean_user_data(user_data_df)


# Print the cleaned data to verify it's ready for upload
print(f"Cleaned user data:\n{cleaned_user_data.head()}")

# Define the name of the target table to upload the cleaned data
target_table_name = "dim_users"

# Upload the cleaned data to the 'dim_users' table in the 'sales_data' database
connector.upload_to_db(cleaned_user_data, target_table_name)

print(f"Cleaned user data has been uploaded to the '{target_table_name}' table.")

# PDF URL containing the card details
pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

# Retrieve data from the PDF and store it in a DataFrame
pdf_data_df = extractor.retrieve_pdf_data(pdf_url)
print(f"Data extracted from the PDF:\n{pdf_data_df}")


