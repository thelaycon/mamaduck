import duckdb
import os
import getpass  # For securely handling password input
from colorama import Fore, init
import argparse

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToPostgreSQL:
    DATABASE_FOLDER = "databases"

    def __init__(self, db_path=None, psql_conn_string=None):
        self.duckdb_conn = None
        self.db_path = db_path
        self.psql_conn_string = psql_conn_string
        self.ensure_database_folder()

    @staticmethod
    def ensure_database_folder():
        """Ensure the 'databases' folder exists."""
        if not os.path.exists(DuckDBToPostgreSQL.DATABASE_FOLDER):
            os.makedirs(DuckDBToPostgreSQL.DATABASE_FOLDER)
            print(f"{Fore.GREEN}Created folder: '{DuckDBToPostgreSQL.DATABASE_FOLDER}'")

    def connect_to_duckdb(self):
        """Connect to either an in-memory or file-based DuckDB database."""
        try:
            if self.db_path:
                full_path = os.path.join(DuckDBToPostgreSQL.DATABASE_FOLDER, self.db_path)
                print(f"{Fore.CYAN}Connecting to DuckDB database: {full_path}")
                self.duckdb_conn = duckdb.connect(database=full_path)
                print(f"{Fore.GREEN}Connected to DuckDB database file '{full_path}'.")
            else:
                self.duckdb_conn = duckdb.connect(database=':memory:')
                print(f"{Fore.GREEN}Connected to an in-memory DuckDB database.")
        except Exception as e:
            print(f"{Fore.RED}Failed to connect to DuckDB database: {e}")
            raise

    def attach_postgresql(self):
        """Attach a PostgreSQL database to DuckDB using the provided connection string."""
        try:
            attach_query = f"ATTACH '{self.psql_conn_string}' AS postgres_db (TYPE POSTGRES);"
            self.duckdb_conn.execute(attach_query)
            print(f"{Fore.GREEN}Attached PostgreSQL database to DuckDB.")
        except Exception as e:
            print(f"{Fore.RED}Failed to attach PostgreSQL database: {e}")
            raise

    def get_table_columns(self, table_name):
        """Get the columns of a table in DuckDB."""
        try:
            columns = self.duckdb_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_definitions = [f"{column[1]} {column[2]}" for column in columns]
            return column_definitions
        except Exception as e:
            print(f"{Fore.RED}Failed to retrieve table columns: {e}")
            raise

    def create_table_in_psql(self, table_name, column_definitions):
        """Dynamically create a table in PostgreSQL."""
        try:
            create_table_query = f"CREATE TABLE IF NOT EXISTS postgres_db.{table_name} ({', '.join(column_definitions)});"
            self.duckdb_conn.execute(create_table_query)
            print(f"{Fore.GREEN}Created table '{table_name}' in PostgreSQL.")
        except Exception as e:
            print(f"{Fore.RED}Failed to create table in PostgreSQL: {e}")
            raise

    def transfer_data_to_psql(self, source_table_name, psql_table_name):
        """Transfer data from DuckDB to PostgreSQL."""
        try:
            data = self.duckdb_conn.execute(f"SELECT * FROM {source_table_name}").fetchall()
            insert_query = f"INSERT INTO postgres_db.{psql_table_name} VALUES ({', '.join(['?' for _ in data[0]])})"
            self.duckdb_conn.executemany(insert_query, data)
            print(f"{Fore.GREEN}Data successfully transferred from '{source_table_name}' to PostgreSQL table '{psql_table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to transfer data: {e}")
            raise

    def preview_psql_data(self, psql_table_name, num_records=10):
        """Preview data from the PostgreSQL table."""
        try:
            preview_query = f"SELECT * FROM postgres_db.{psql_table_name} LIMIT {num_records};"
            result = self.duckdb_conn.execute(preview_query).fetchall()
            print(f"{Fore.CYAN}Previewing {num_records} records from PostgreSQL table '{psql_table_name}':")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}Failed to preview data in PostgreSQL: {e}")
            raise

    def close_connections(self):
        """Close the DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            print(f"{Fore.GREEN}Connection to DuckDB closed.")

def get_postgresql_connection_string():
    """Get individual PostgreSQL connection parameters and assemble the connection string."""
    print(f"{Fore.CYAN}Please provide the following PostgreSQL connection details:")
    dbname = input(f"{Fore.YELLOW}Enter the PostgreSQL database name (e.g., 'postgres'): ").strip()
    user = input(f"{Fore.YELLOW}Enter the PostgreSQL user (e.g., 'postgres'): ").strip()
    host = input(f"{Fore.YELLOW}Enter the PostgreSQL host (e.g., '127.0.0.1'): ").strip()
    port = input(f"{Fore.YELLOW}Enter the PostgreSQL port (default is 5432): ").strip() or "5432"
    password = getpass.getpass(f"{Fore.YELLOW}Enter the PostgreSQL password: ").strip()
    return f"dbname={dbname} user={user} host={host} port={port} password={password}"

def main():
    parser = argparse.ArgumentParser(description="DuckDB to PostgreSQL Transfer Tool")
    parser.add_argument("--cli", action="store_true", help="Trigger interactive mode")
    args = parser.parse_args()

    if args.cli:
        print(f"{Fore.CYAN}Running in interactive mode...")

        # Interactive mode: Get user input for all details
        duckdb_db_name = input(f"{Fore.CYAN}Enter the DuckDB database name (located in 'databases' folder): ").strip()
        psql_conn_string = get_postgresql_connection_string()
        db_tool = DuckDBToPostgreSQL(duckdb_db_name, psql_conn_string)
        
        db_tool.connect_to_duckdb()
        db_tool.attach_postgresql()

        source_table_name = input(f"{Fore.CYAN}Enter the DuckDB table to transfer: ").strip()
        psql_table_name = input(f"{Fore.CYAN}Enter the PostgreSQL table to create: ").strip()
        column_definitions = db_tool.get_table_columns(source_table_name)
        db_tool.create_table_in_psql(psql_table_name, column_definitions)
        db_tool.transfer_data_to_psql(source_table_name, psql_table_name)

        if input(f"{Fore.CYAN}Preview PostgreSQL table data? (yes/no): ").strip().lower() == "yes":
            db_tool.preview_psql_data(psql_table_name)
        
        db_tool.close_connections()

    else:
        # Non-interactive mode: Provide details via arguments
        print(f"{Fore.CYAN}Running in non-interactive mode...")

        # Get the necessary information here, for example, using hardcoded parameters or passing arguments directly.
        duckdb_db_name = "example.duckdb"  # Example: You can pass this as an argument instead of interactively
        psql_conn_string = "dbname=your_db user=your_user host=localhost port=5432 password=your_password"  # Pass this via args
        db_tool = DuckDBToPostgreSQL(duckdb_db_name, psql_conn_string)
        
        db_tool.connect_to_duckdb()
        db_tool.attach_postgresql()

        # Assuming some source and target tables are provided
        source_table_name = "your_duckdb_table"
        psql_table_name = "your_psql_table"
        column_definitions = db_tool.get_table_columns(source_table_name)
        db_tool.create_table_in_psql(psql_table_name, column_definitions)
        db_tool.transfer_data_to_psql(source_table_name, psql_table_name)

        db_tool.preview_psql_data(psql_table_name)
        
        db_tool.close_connections()

if __name__ == "__main__":
    main()
