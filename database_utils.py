import yaml
from sqlalchemy import create_engine , MetaData


class DatabaseConnector:
    # Other methods and initializations

    def read_db_creds(self, file_path):
        """
        Reads the database credentials from a YAML file.

        :param file_path: The path to the YAML file containing the credentials.
        :return: A dictionary containing the database credentials.
        """
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self, file_path):
        """
        Initializes and returns an SQLAlchemy database engine using credentials from the YAML file.

        :param file_path: The path to the YAML file containing the database credentials.
        :return: An SQLAlchemy engine instance.
        """
        creds = self.read_db_creds(file_path)

        # Updated keys according to the provided YAML file format
        db_url = (
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}"
            f"@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )

        # Create and return the SQLAlchemy engine
        engine = create_engine(db_url)
        return engine
    

    def list_db_tables(self, engine):
        """
        Lists all tables in the database.

        :param engine: SQLAlchemy engine object connected to the database.
        :return: A list of table names.
        """
        meta = MetaData()
        meta.reflect(bind=engine)
        return [table.name for table in meta.tables.values()]

    def upload_to_db(self, df, table_name, if_exists='append', index=False):
        """
        Uploads a pandas DataFrame to the specified table in the database.

        :param df: pandas DataFrame to upload.
        :param table_name: The name of the target table in the database to upload data to.
        :param if_exists: How to behave if the table already exists.
                          {'fail', 'replace', 'append'}, default 'append'.
        :param index: Whether to write DataFrame index as a column. Default: False.
        """
        try:
            # Assuming self.engine is the SQLAlchemy engine that is already connected to your database
            df.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=index)
            print(f"Data uploaded successfully to {table_name}")
        except Exception as e:
            print(f"An error occurred: {e}")

# Usage example
connector = DatabaseConnector()
credentials = connector.read_db_creds("db_creds.yaml")
engine = connector.init_db_engine("db_creds.yaml")
tables = connector.list_db_tables(engine)
print(tables)
print(credentials)
