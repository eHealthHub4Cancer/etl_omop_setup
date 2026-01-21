import os
import psycopg
from psycopg import sql
from tqdm import tqdm
import pandas as pd
from code_base.db_connector import DBConnector

class CSVLoader:
    def __init__(self, db_connector: DBConnector):
        self.db_connector = db_connector
        # This map allows you to specify a non-standard delimiter for certain files.
        # The filename (without extension) is matched against the keys.
        self.delimiter_map = {
            'tab_separated_table': '\t',
            'pipe_separated_table': '|',
            'caret_separated_table': '^',
            'comma_separated_table': ',',
        }

    def load_file(self, schema: str, table: str, file_path: str, delimiter: str = '\t'):
        copy_query = sql.SQL(
            "COPY {}.{} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER {}, QUOTE E'\\b')"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Literal(delimiter)
        )

        try:
            file_size = os.path.getsize(file_path)
            
            with self.db_connector.connect.cursor() as cursor:
                with open(file_path, 'rb') as f:
                    # Initialize tqdm with the file size
                    with tqdm(
                        total=file_size, 
                        desc=f"Streaming {table}", 
                        unit='B', 
                        unit_scale=True,
                        leave=False # Cleans up the bar after the file is done
                    ) as pbar:
                        
                        with cursor.copy(copy_query) as copy:
                            while data := f.read(1024 * 1024): # 1MB chunks
                                copy.write(data)
                                pbar.update(len(data)) # Update progress by the number of bytes read
                    
                self.db_connector.connect.commit()
                print(f" -> OK: '{table}' loaded.")

        except Exception as error:
            self.db_connector.connect.rollback()
            print(f"\n -> FAILED: {table}: {error}")
    
    def process_folder(self, schema: str, folder_path: str, delimiter: str = ','):
        """
        Processes all .csv files in a folder, streaming each into a
        correspondingly named table.
        """
        print(f"Starting to process files in: {folder_path}")
        try:
            csv_files = sorted([f for f in os.listdir(folder_path) if f.lower().endswith('.csv')])
        except FileNotFoundError:
            print(f"Error: The specified folder does not exist: {folder_path}")
            return
        
        if not csv_files:
            print("No .csv files found in the specified folder.")
            return
            
        for file_name in tqdm(csv_files, desc="Overall Progress", unit="file"):
            file_path = os.path.join(folder_path, file_name)
            table_name = os.path.splitext(file_name)[0].lower()
            # loading cpt4 concepts into concept table
            if 'concept_cpt4' in table_name:
                self.fill_nulls_with_default("concept_cpt4.csv", folder_path, delimiter='\t', column_name='concept_name', 
                                             default_value='Unknown CPT4 Concept')
                table_name = 'concept'  # special case handling

            if not self.check_table_exists(schema, table_name):
                print(f"\nTable '{table_name}' does not exist in schema '{schema}'. Skipping file '{file_name}'.")
                continue
            
            try:
                self.load_file(schema, table_name, file_path, delimiter)
            except Exception:
                # If load_file fails, it prints the error. We can stop the whole process.
                print(f"Stopping folder processing due to a critical error.")
                break # Exit the loop
        
        print("\nFolder processing complete.")

    def check_table_exists(self, schema: str, table: str) -> bool:
        """
        Checks if a table exists in the given schema.
        """
        query = sql.SQL(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = {schema} AND table_name = {table}"
            ")"
        ).format(
            schema=sql.Literal(schema),
            table=sql.Literal(table)
        )
        try:
            with self.db_connector.connect.cursor() as cursor:
                cursor.execute(query)
                exists = cursor.fetchone()[0]
                return exists
        except (Exception, psycopg.Error) as error:
            print(f"Error checking if table exists: {error}")
            return False
        
    def fill_nulls_with_default(self, file_name: str, folder_path: str, delimiter: str = ',', column_name: str = '', default_value: str = '') -> bool:
        try:
            csv_to_read = os.path.join(folder_path, file_name)
            df = pd.read_csv(csv_to_read, delimiter=delimiter)
            if column_name in df.columns:
                df.fillna({column_name: default_value}, inplace=True)
                df.to_csv(csv_to_read, index=False, sep=delimiter)
                print(f"Nulls in column '{column_name}' of file '{file_name}' filled with default value '{default_value}'.")
                return True
            else:
                print(f"Column '{column_name}' does not exist in file '{file_name}'.")
                return False
        
        except Exception as error:
            print(f"Error filling nulls with default values: {error}")
            return False