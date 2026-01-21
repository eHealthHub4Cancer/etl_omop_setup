import psycopg
from psycopg import sql
from urllib.parse import quote_plus

class DBConnector:
    def __init__(self, **kwargs):
        # Extract database config
        self.dbUser = kwargs.get('dbUser')
        self.dbPassword = kwargs.get('dbPassword')
        self.dbHost = kwargs.get('dbHost')
        self.dbPort = kwargs.get('dbPort')
        self.dbName = kwargs.get('dbName')
        # Store schema configs
        self.cdmDatabaseSchema = kwargs.get('cdmDatabaseSchema')
        self.vocabDatabaseSchema = kwargs.get('vocabDatabaseSchema')
        self.resultsDatabaseSchema = kwargs.get('resultsDatabaseSchema')
        self.scratchDatabaseSchema = kwargs.get('scratchDatabaseSchema')
        self.webApiSchema = kwargs.get('webApiSchema')
        self.tempSchema = kwargs.get('tempSchema')
        
        # Store webapi user configs
        self.webApiUser = kwargs.get('webApiUser')
        self.webApiPassword = kwargs.get('webApiPassword')

        # Store ETL user configs
        self.etlUser = kwargs.get('dbUser1')
        self.etlPassword = kwargs.get('dbPassword1')

        # Create connection
        self.connect = self.create_connection()

    def create_connection(self):
        try:
            connect_string = f"postgresql://{quote_plus(self.dbUser)}:{quote_plus(self.dbPassword)}@{self.dbHost}:{self.dbPort}/{self.dbName}"
            connection = psycopg.connect(connect_string)
            connection.autocommit = False
            return connection
        except (Exception, psycopg.Error) as error:
            print(f"Error connecting to PostgreSQL: {error}")
            return None
    
    def create_schemas(self):
        try:
            with self.connect.cursor() as cursor:
                schemas = [
                    self.cdmDatabaseSchema,
                    self.vocabDatabaseSchema,
                    self.resultsDatabaseSchema,
                    self.scratchDatabaseSchema,
                    self.webApiSchema,
                    self.tempSchema
                ]
                for schema in schemas:
                    cursor.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(schema)
                        )
                    )
                self.connect.commit()
                print("Schemas created successfully")
        except Exception as error:
            print(f"Error creating schemas: {error}")
            self.connect.rollback()

    # lolx, let's create the role.
    def create_role(self, role_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE ROLE {} NOLOGIN").format(
                        sql.Identifier(role_name)
                    )
                )
                self.connect.commit()
                print(f"Role {role_name} created successfully")
        except psycopg.errors.DuplicateObject:
            print(f"Role {role_name} already exists. Skipping creation.")
            self.connect.rollback()
        except Exception as error:
            print(f"Error creating role {role_name}: {error}")
            self.connect.rollback()

    # ahah! creating user. - always revisiting this part. 
    # looks weird why create a user when i can just use roles.
    # but whatever, let's do it.
    # a user belongs to a role., a role is more like a super set of permissions that can be 
    # assigned to multiple users.? Come back later!
    def create_new_user(self, username, password):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE USER {} WITH PASSWORD {}").format(
                        sql.Identifier(username),
                        sql.Literal(password)
                    )
                )
                self.connect.commit()
                print(f"User {username} created successfully")
        except psycopg.errors.DuplicateObject:
            print(f"User {username} already exists. Skipping creation.")
            self.connect.rollback()
        except Exception as error:
            print(f"Error creating user {username}: {error}")
            self.connect.rollback()

    # grant access to database to role.
    def grant_database_role_access(self, role_name, database_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                        sql.Identifier(database_name),
                        sql.Identifier(role_name)
                    )
                )
                self.connect.commit()
                print(f"Granted CONNECT on database {database_name} to role {role_name}")
        except Exception as error:
            print(f"Error granting database access to role {role_name}: {error}")
            self.connect.rollback()

    # create read_only_role_privileges.
    def create_read_only_role(self, role_name, schema_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                # allow being able to select from all tables in vocab schema.
                cursor.execute(
                    sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                # grant usage on future tables.  
                cursor.execute(
                    sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                self.connect.commit()
                print(f"Read-only privileges granted to role {role_name} on schema {schema_name}")
        except Exception as error:
            print(f"Error granting read-only privileges to role {role_name}: {error}")
            self.connect.rollback()

    # read write permissions to role.
    def create_read_write_role(self, role_name, schema_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                cursor.execute(
                    sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {} TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                cursor.execute(
                    sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                # Grant USAGE on all existing SEQUENCES
                cursor.execute(
                    sql.SQL("GRANT USAGE ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )

                # Alter default privileges for FUTURE SEQUENCES
                cursor.execute(
                    sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT USAGE ON SEQUENCES TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                self.connect.commit()
                print(f"Read-write privileges granted to role {role_name} on schema {schema_name}")
        except Exception as error:
            print(f"Error granting read-write privileges to role {role_name}: {error}")
            self.connect.rollback()

    # grant user access to role.
    def assign_role_to_user(self, username, role_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(role_name),
                        sql.Identifier(username)
                    )
                )
                self.connect.commit()
                print(f"Granted role {role_name} to user {username}")
        except Exception as error:
            print(f"Error granting role {role_name} to user {username}: {error}")
            self.connect.rollback()

    # revoke role from user.
    def revoke_user_role(self, username, role_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(role_name),
                        sql.Identifier(username)
                    )
                )
                self.connect.commit()
                print(f"Revoked role {role_name} from user {username}")
        except Exception as error:
            print(f"Error revoking role {role_name} from user {username}: {error}")
            self.connect.rollback()

    # close connection
    def close_connection(self):
        if self.connect:
            self.connect.close()
            print("Database connection closed.")   
    
    # set the user as super user.
    def set_user_as_superuser(self, username):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("ALTER USER {} WITH SUPERUSER").format(
                        sql.Identifier(username)
                    )
                )
                self.connect.commit()
                print(f"User {username} set as SUPERUSER")
        except Exception as error:
            print(f"Error setting user {username} as SUPERUSER: {error}")
            self.connect.rollback()

    # create schema owner to roles.
    def set_schema_owner(self, schema_name, role_name):
        try:
            with self.connect.cursor() as cursor:
                cursor.execute(
                    sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(role_name)
                    )
                )
                self.connect.commit()
                print(f"Schema {schema_name} ownership transferred to role {role_name}")
        except Exception as error:
            print(f"Error transferring ownership of schema {schema_name} to role {role_name}: {error}")
            self.connect.rollback()