# create a class 
from code_base.db_connector import DBConnector
import psycopg

class DDL:
    def __init__(self, db_connector: DBConnector):
        self.db_connector = db_connector

    def create_vocab_tables(self, path: str):
        try:
            self._execute_sql_file(path)
            print("Vocab tables created successfully.")
        except (Exception, psycopg.Error) as e:
            # rollback in case of error
            self.db_connector.connect.rollback()
            print(f"Error creating Vocab tables: {e} - rolled back.")   

    def create_cdm_tables(self, path: str):
        try:
            self._execute_sql_file(path)
            print("CDM tables created successfully.")
        except (Exception, psycopg.Error) as e:
            # rollback in case of error
            self.db_connector.connect.rollback()
            print(f"Error creating CDM tables: {e} - rolled back.")

    def add_primary_keys(self, path: str):
        try:
            self._execute_sql_file(path)
            print("Primary keys added successfully.")
        except (Exception, psycopg.Error) as e:
            # rollback in case of error
            self.db_connector.connect.rollback()
            print(f"Error adding primary keys: {e} - rolled back.")

    def add_indices(self, path: str):
        try:
            self._execute_sql_file(path)
            print("Indices added successfully.")
        except (Exception, psycopg.Error) as e:
            # rollback in case of error
            self.db_connector.connect.rollback()
            print(f"Error adding indices: {e} - rolled back.")

    def add_constraints(self, path: str):
        try:
            self._execute_sql_file(path)
            print("Constraints added successfully.")
        except (Exception, psycopg.Error) as e:
            # rollback in case of error
            self.db_connector.connect.rollback()
            print(f"Error adding constraints: {e} - rolled back.")

    def run_achilles_script(self, path: str):
        try:
            print("analysis started")
            self._execute_sql_file(path)
            print("Achilles script executed successfully.")
        except (Exception, psycopg.Error) as e:
            # rollback in case of error
            self.db_connector.connect.rollback()
            print(f"Error executing Achilles script: {e} - rolled back.")

    def _execute_sql_file(self, path: str):
        with self.db_connector.connect.cursor() as cursor:
            with open(path, 'r') as file:
                sql_commands = file.read()
                # replace schema placeholder with actual schema name, it has @vocabDatabaseSchema or @cdmDatabaseSchema
                sql_commands = sql_commands.replace("@cdmDatabaseSchema", self.db_connector.cdmDatabaseSchema)
                sql_commands = sql_commands.replace("@vocabDatabaseSchema", self.db_connector.vocabDatabaseSchema)
                sql_commands = sql_commands.replace("@resultSchema", self.db_connector.resultsDatabaseSchema)
                sql_commands = sql_commands.replace("@tempSchema", self.db_connector.tempSchema)      
                cursor.execute(sql_commands)
            self.db_connector.connect.commit()
            
    def _execute_view_cdm(self, cdm_schemas, vocab_tables, vocab_schema: str):
        with self.db_connector.connect.cursor() as cursor:
            for cdm in cdm_schemas:
                for table in vocab_tables:
                    sql_command = f"""
                    CREATE OR REPLACE VIEW {cdm}.{table} AS
                    SELECT *
                    FROM {vocab_schema}.{table};
                    """
                    cursor.execute(sql_command)
            self.db_connector.connect.commit()