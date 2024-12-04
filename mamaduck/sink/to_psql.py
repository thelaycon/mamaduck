import duckdb
import os
import getpass  # For securely handling password input
from colorama import Fore, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToPostgreSQL:
    def __init__(self, duckdb_db_path, psql_conn_string):
        self.duckdb_db_path = duckdb_db_path
        self.psql_conn_string = psql_conn_string
        self.duckdb_connection = None

    def connect_to_duckdb(self):
        """Connect to the DuckDB database."""
        try:
            self.duckdb_connection = duckdb.connect(self.duckdb_db_path)
            print(f"{Fore.GREEN}Connected to DuckDB database at '{self.duckdb_db_path}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to connect to DuckDB: {e}")
            raise

    def attach_psql_to_duckdb(self):
        """Attach PostgreSQL to DuckDB."""
        try:
            # ATTACH PostgreSQL to DuckDB using provided connection string
            attach_query = f"ATTACH '{self.psql_conn_string}' AS postgres_db (TYPE POSTGRES);"
            self.duckdb_connection.execute(attach_query)
            print(f"{Fore.GREEN}Attached PostgreSQL database.")
        except Exception as e:
            print(f"{Fore.RED}Failed to attach PostgreSQL database: {e}")
            raise

    def get_table_columns(self, table_name):
        """Get the columns of a table in DuckDB."""
        try:
            columns = self.duckdb_connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_definitions = []
            for column in columns:
                col_name = column[1]
                col_type = column[2]
                column_definitions.append(f"{col_name} {col_type}")
            return column_definitions
        except Exception as e:
            print(f"{Fore.RED}Failed to retrieve table columns: {e}")
            raise

    def create_table_in_psql(self, table_name, column_definitions):
        """Dynamically create a table in PostgreSQL."""
        try:
            create_table_query = f"CREATE TABLE IF NOT EXISTS postgres_db.{table_name} ({', '.join(column_definitions)});"
            self.duckdb_connection.execute(create_table_query)
            print(f"{Fore.GREEN}Created table '{table_name}' in PostgreSQL.")
        except Exception as e:
            print(f"{Fore.RED}Failed to create table in PostgreSQL: {e}")
            raise

    def transfer_data_to_psql(self, source_table_name, psql_table_name):
        """Transfer data from DuckDB to PostgreSQL."""
        try:
            # Fetch data from DuckDB
            data = self.duckdb_connection.execute(f"SELECT * FROM {source_table_name}").fetchall()

            # Insert data into PostgreSQL table
            insert_query = f"INSERT INTO postgres_db.{psql_table_name} VALUES ({', '.join(['?' for _ in data[0]])})"
            self.duckdb_connection.executemany(insert_query, data)
            print(f"{Fore.GREEN}Data successfully transferred from '{source_table_name}' to PostgreSQL table '{psql_table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to transfer data: {e}")
            raise

    def preview_psql_data(self, psql_table_name, num_records=10):
        """Preview data from the PostgreSQL table."""
        try:
            preview_query = f"SELECT * FROM postgres_db.{psql_table_name} LIMIT {num_records};"
            result = self.duckdb_connection.execute(preview_query).fetchall()
            print(f"{Fore.CYAN}Previewing {num_records} records from PostgreSQL table '{psql_table_name}':")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}Failed to preview data in PostgreSQL: {e}")
            raise

    def close_connections(self):
        """Close the DuckDB connection."""
        if self.duckdb_connection:
            self.duckdb_connection.close()
            print(f"{Fore.GREEN}Connection to DuckDB closed.")

def get_postgresql_connection_string():
    """Get individual PostgreSQL connection parameters and assemble the connection string."""
    print(f"{Fore.CYAN}Please provide the following PostgreSQL connection details:")

    # Get each part of the connection string
    dbname = input(f"{Fore.YELLOW}Enter the PostgreSQL database name (e.g., 'postgres'): ").strip()
    user = input(f"{Fore.YELLOW}Enter the PostgreSQL user (e.g., 'postgres'): ").strip()
    host = input(f"{Fore.YELLOW}Enter the PostgreSQL host (e.g., '127.0.0.1'): ").strip()
    port = input(f"{Fore.YELLOW}Enter the PostgreSQL port (default is 5432): ").strip() or "5432"

    # Securely get the password
    password = getpass.getpass(f"{Fore.YELLOW}Enter the PostgreSQL password: ").strip()

    # Assemble the PostgreSQL connection string
    psql_conn_string = f"dbname={dbname} user={user} host={host} port={port} password={password}"
    
    return psql_conn_string

def main():
    # Get the DuckDB database path from the user
    duckdb_db_name = input(f"{Fore.CYAN}Enter the name of the DuckDB database (in 'databases' folder): ").strip()
    duckdb_db_path = os.path.join("databases", duckdb_db_name)

    if not os.path.exists(duckdb_db_path):
        print(f"{Fore.RED}DuckDB database file does not exist at '{duckdb_db_path}'.")
        return

    # Get the PostgreSQL connection string
    psql_conn_string = get_postgresql_connection_string()

    # Initialize the class and connect to DuckDB
    db_tool = DuckDBToPostgreSQL(duckdb_db_path, psql_conn_string)
    db_tool.connect_to_duckdb()

    # Attach PostgreSQL to DuckDB
    db_tool.attach_psql_to_duckdb()

    # Define the source table from DuckDB and the target table in PostgreSQL
    source_table_name = input(f"{Fore.CYAN}Enter the name of the DuckDB table to transfer: ").strip()
    psql_table_name = input(f"{Fore.CYAN}Enter the name of the PostgreSQL table to create: ").strip()

    # Get column definitions for the source DuckDB table
    column_definitions = db_tool.get_table_columns(source_table_name)

    # Create the target table in PostgreSQL dynamically
    db_tool.create_table_in_psql(psql_table_name, column_definitions)

    # Transfer data from DuckDB to PostgreSQL
    db_tool.transfer_data_to_psql(source_table_name, psql_table_name)

    # Ask the user if they want to preview the data in PostgreSQL
    preview_action = input(f"{Fore.CYAN}Would you like to preview the data in the PostgreSQL table? (yes/no): ").strip().lower()
    if preview_action == 'yes':
        db_tool.preview_psql_data(psql_table_name)

    # Close connections
    db_tool.close_connections()

if __name__ == "__main__":
    main()
