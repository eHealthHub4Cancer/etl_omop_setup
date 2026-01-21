from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    def __init__(self):
        self._db_config = None
        self._schema_config = None
        self._webapi_user = None
    
    def load_db_config(self):
        if self._db_config is None:
            self._db_config = {
                "dbUser": os.getenv("dbUser"),
                "dbPassword": os.getenv("dbPassword"),
                "dbHost": os.getenv("dbHost"),
                "dbPort": os.getenv("dbPort"),
                "dbName": os.getenv("dbName"),
            }
        return self._db_config
    
    def load_schema_config(self):
        if self._schema_config is None:
            self._schema_config = {
                "cdmDatabaseSchema": os.getenv("cdmDatabaseSchema"),
                "vocabDatabaseSchema": os.getenv("vocabDatabaseSchema"),
                "resultsDatabaseSchema": os.getenv("resultSchema"),
                "scratchDatabaseSchema": os.getenv("scratchSchema"),
                "webApiSchema": os.getenv("webApiSchema"),
                "tempSchema": os.getenv("tempSchema"),
            }
        return self._schema_config
    
    def load_webapi_user(self):
        if self._webapi_user is None:
            self._webapi_user = {
                "webApiUser": os.getenv("dbUser2"),
                "webApiPassword": os.getenv("dbPassword2"),
            }
        return self._webapi_user
    
    def load_etl_user(self):
        return {
            "dbUser1": os.getenv("dbUser1"),
            "dbPassword1": os.getenv("dbPassword1"),
        }
    
    def load_role_configs(self):
        return {
            "etlrole": os.getenv("role1"),
            "webapirole": os.getenv("role2"),
        }
    
    def load_sql_configs(self):
        return {
            "vocab_sql_path": os.getenv("vocabDdl"),
            "cdm_sql_path": os.getenv("cdmDdl"),
            "cdm_primary_keys_sql_path": os.getenv("cdmPrimaryKeys"),
            "vocab_primary_keys_sql_path": os.getenv("vocabPrimaryKeys"),
            "vocab_indices_sql_path": os.getenv("vocabIndices"),
            "cdm_indices_sql_path": os.getenv("cdmIndices"),
            "constraints_sql_path": os.getenv("constraints"),
        }
    
    def load_csv_paths(self):
        return{
            "vocab_csv_folder": os.getenv("vocabCsvFolder"),
            "cdm_csv_folder": os.getenv("cdmCsvFolder"),
        }
    
    def load_achilles_configs(self):
        return {
            "achilles_result_sql": os.getenv("achillesResult"),
            "achilles_count_sql": os.getenv("achillesCount"),
        }