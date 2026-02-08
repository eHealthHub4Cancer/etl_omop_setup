# import the necessary libraries.
from code_base.config import Config
from code_base.db_connector import DBConnector
from code_base.ddl import DDL
from code_base.csv_loader import CSVLoader

def initialize_db_connector():
    """Loads all configurations and returns a DBConnector instance."""
    config = Config()
    db_connector = DBConnector(
        **config.load_db_config(),
        **config.load_schema_config(),
        **config.load_webapi_user(),
        **config.load_etl_user()
    )
    
    if not db_connector.connect:
        raise ConnectionError("Failed to establish database connection.")    
    print("Database connection established.")
    return db_connector, config

def setup_security_and_roles(db_conn: DBConnector, config: Config):
    """Handles role creation, schema permissions, and user assignments."""
    role_configs = config.load_role_configs()
    etl_role = role_configs['etlrole']
    webapi_role = role_configs['webapirole']

    # 1. Base Setup
    db_conn.set_user_as_superuser(db_conn.dbUser)
    db_conn.create_schemas()
    db_conn.create_role(etl_role)
    db_conn.create_role(webapi_role)

    # 2. ETL Role Permissions
    db_conn.grant_database_role_access(etl_role, db_conn.dbName)
    db_conn.create_read_only_role(etl_role, db_conn.vocabDatabaseSchema)
    for schema in [db_conn.cdmDatabaseSchema, db_conn.resultsDatabaseSchema, 
                   db_conn.tempSchema, db_conn.scratchDatabaseSchema]:
        db_conn.create_read_write_role(etl_role, schema)

    # 3. WebAPI Role Permissions
    db_conn.grant_database_role_access(webapi_role, db_conn.dbName)
    db_conn.create_read_only_role(webapi_role, db_conn.vocabDatabaseSchema)
    db_conn.create_read_only_role(webapi_role, db_conn.cdmDatabaseSchema)
    for schema in [db_conn.resultsDatabaseSchema, db_conn.tempSchema, 
                   db_conn.webApiSchema, db_conn.scratchDatabaseSchema]:
        db_conn.create_read_write_role(webapi_role, schema)

    # 4. Finalize Users
    db_conn.set_schema_owner(db_conn.webApiSchema, webapi_role)
    db_conn.create_new_user(db_conn.webApiUser, db_conn.webApiPassword)
    db_conn.create_new_user(db_conn.etlUser, db_conn.etlPassword)
    db_conn.assign_role_to_user(db_conn.etlUser, etl_role)
    db_conn.assign_role_to_user(db_conn.webApiUser, webapi_role)
    print("Security and roles configured.")

def run_database_ddl(db_conn: DBConnector, config: Config, create: bool = True):
    """Creates tables, primary keys, and indices."""
    sql_paths = config.load_sql_configs()
    ddl = DDL(db_conn)
    
    # Create Tables
    ddl.create_cdm_tables(sql_paths['cdm_sql_path'])
    if create:
        ddl.create_vocab_tables(sql_paths['vocab_sql_path'])
    
    # Add Keys and Indices
    ddl.add_primary_keys(sql_paths['cdm_primary_keys_sql_path'])
    ddl.add_primary_keys(sql_paths['vocab_primary_keys_sql_path'])
    ddl.add_indices(sql_paths['cdm_indices_sql_path'])
    ddl.add_indices(sql_paths['vocab_indices_sql_path'])
    return ddl, sql_paths

def load_initial_data(db_conn: DBConnector, config: Config, ddl: DDL, sql_paths: dict):
    """Loads CSV data and applies final constraints."""
    csv_paths = config.load_csv_paths()
    csv_loader = CSVLoader(db_connector=db_conn)
    
    # Load Vocabulary
    # csv_loader.process_folder(
    #     schema=db_conn.vocabDatabaseSchema, 
    #     folder_path=csv_paths['vocab_csv_folder'], 
    #     delimiter='\t'
    # )

    # load cdm data
    csv_loader.process_folder(
        schema=db_conn.cdmDatabaseSchema, 
        folder_path=csv_paths['cdm_csv_folder'], 
        delimiter=','
    )
    
    # Constraints (applied after data load for performance/integrity)
    ddl.add_constraints(sql_paths['constraints_sql_path'])
    print("Data loading and constraints completed.")

def run_achilles_analysis(db_conn: DBConnector, config: Config):
    """Runs Achilles analysis scripts."""
    achilles_configs = config.load_achilles_configs()
    ddl = DDL(db_conn)
    
    ddl.run_achilles_script(achilles_configs['achilles_result_sql'])
    ddl.run_achilles_script(achilles_configs['achilles_count_sql'])
    print("Achilles analysis completed.")

# This is necessary to avoid error from packages like feature extraction. We encountered issues
# when running cohort characterization with feature extraction, which is used in ATLAS. The error was related to the fact that the CDM and Vocabulary views were not created before running the feature extraction. By creating the views at the end of the setup process, we ensure that they are available for any subsequent analysis or feature extraction tasks, thus preventing such errors and ensuring a smoother workflow for users who rely on these views for their analyses.
# Reason being that the cdm and vocab tables were in seperate schemas.
def run_cdm_vocab_view(db_conn: DBConnector, config: Config):
    cdm_schemas = config.load_cdm_schemas()
    vocab_schema = config.load_schema_config()['vocabDatabaseSchema']
    vocab_tables = config.load_vocab_tables()
    ddl = DDL(db_conn)
    ddl._execute_view_cdm(cdm_schemas['cdm_schemas'], vocab_tables['vocab_tables'], vocab_schema)

def main():
    db_conn = None
    try:
        # Step 1: Initialize
        db_conn, config = initialize_db_connector()
        # Step 2: Permissions
        # setup_security_and_roles(db_conn, config)
        # Step 3: DDL
        # ddl, sql_paths = run_database_ddl(db_conn, config, create=False)
        # Step 4: Data
        # load_initial_data(db_conn, config, ddl, sql_paths)
        # Step 5: Achilles Analysis
        # Please check the readme before running Achilles analysis.
        # This should be run only after confirming the database is fully set up.
        # In most cases, you are expected to have already run Achilles outside this script, before using this.
        # uncomment the next line to run. 
        # Please ensure you comment step 2 - 4 if you have already run them once.
        
        # run_achilles_analysis(db_conn, config)
        
        # Step 6: Create CDM and Vocabulary Views
        run_cdm_vocab_view(db_conn, config)
        
        print("Full OMOP Database setup completed successfully.")

    except Exception as e:
        print(f"An error occurred during setup: {e}")
    finally:
        if db_conn:
            db_conn.close_connection()
            print("Database connection closed.")

if __name__ == "__main__":
    main()