"""
import pandas as pd



class DataExtractor:
    def __init__(self, engine):
        self.engine = engine

    def read_table_data(self, table_name):
        """
       # Reads data from a specified table in the database.

        #:param table_name: The name of the table to read data from.
       # :return: DataFrame containing the data from the table.
        #"""
        #with self.engine.connect() as connection:
            #return pd.read_sql_table(table_name, connection)


import pandas as pd
import tabula

class DataExtractor:
    def __init__(self, db_connector):
        """
        Initializes the DataExtractor with an instance of DatabaseConnector.
        
        :param db_connector: An instance of the DatabaseConnector class.
        """
        self.db_connector = db_connector
        self.engine = db_connector.engine  # Assume the DatabaseConnector instance has an 'engine' attribute

    def read_table_data(self, table_name):
        """
        Reads data from a specified table in the database.

        :param table_name: The name of the table to read data from.
        :return: DataFrame containing the data from the table.
        """
        with self.engine.connect() as connection:
            return pd.read_sql_table(table_name, connection)

    def read_rds_table(self, user_data_table_name):
        """
        Extracts the table containing user data from the RDS database into a pandas DataFrame.
        
        :param user_data_table_name: The name of the table to extract user data from.
        :return: A pandas DataFrame containing the user data.
        """
        # Use the list_db_tables method to check if the specified table exists
        tables = self.db_connector.list_db_tables(self.engine)
        if user_data_table_name in tables:
            return self.read_table_data(user_data_table_name)
        else:
            raise ValueError(f"Table {user_data_table_name} does not exist in the database.")

    def retrieve_pdf_data(self, pdf_url):
        """
        Extracts table data from a PDF document and returns it as a pandas DataFrame.

        :param pdf_url: The URL to the PDF file.
        :return: A pandas DataFrame containing the extracted table data.
        """
        # tabula.read_pdf returns a list of DataFrames, hence use try-except to handle potential errors
        try:
            # Read the PDF from the provided URL
            dfs = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True, stream=True)
            # Concatenate all the DataFrames into one if multiple tables are found
            return pd.concat(dfs, ignore_index=True)
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of an error
