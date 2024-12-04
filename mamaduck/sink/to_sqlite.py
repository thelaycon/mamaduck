import duckdb
import os
from colorama import Fore, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToSQLite:
    DATABASE_FOLDER = "databases"

    def __init__(self, duckdb_db_path, sqlite_db_path):
        self.duckdb_db_path = duckdb_db_path
        self.sqlite_db_path = sqlite_db_path
        self.connection = None

    def connect_to_duckdb(self):
        """Connect to the DuckDB database."""
        try:
            self.connection = duckdb.connect(self.duckdb_db_path)
            print(f"{Fore.GREEN}Connected to DuckDB database at '{self.duckdb_db_path}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to connect to DuckDB: {e}")
            raise

    def attach_sqlite_database(self):
        """Attach the SQLite database to DuckDB."""
        try:
            self.connection.execute(f"ATTACH '{self.sqlite_db_path}' AS sqlite_db (TYPE SQLITE);")
            print(f"{Fore.GREEN}Attached SQLite database '{self.sqlite_db_path}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to attach SQLite database: {e}")
            raise

    def get_table_columns(self, table_name):
        """Get the columns of a table in DuckDB."""
        try:
            columns = self.connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_definitions = []
            for column in columns:
                col_name = column[1]
                col_type = column[2]
                column_definitions.append(f"{col_name} {col_type}")
            return column_definitions
        except Exception as e:
            print(f"{Fore.RED}Failed to retrieve table columns: {e}")
            raise

    def create_table_in_sqlite(self, table_name, column_definitions):
        """Dynamically create a table in the SQLite database."""
        try:
            create_table_query = f"CREATE TABLE IF NOT EXISTS sqlite_db.{table_name} ({', '.join(column_definitions)});"
            self.connection.execute(create_table_query)
            print(f"{Fore.GREEN}Created table '{table_name}' in SQLite.")
        except Exception as e:
            print(f"{Fore.RED}Failed to create table in SQLite: {e}")
            raise

    def transfer_data_to_sqlite(self, source_table_name, sqlite_table_name):
        """Transfer data from DuckDB to SQLite."""
        try:
            # Fetch data from DuckDB
            data = self.connection.execute(f"SELECT * FROM {source_table_name}").fetchall()

            # Insert data into the SQLite table
            insert_query = f"INSERT INTO sqlite_db.{sqlite_table_name} VALUES ({', '.join(['?' for _ in data[0]])})"
            self.connection.executemany(insert_query, data)
            self.connection.commit()
            print(f"{Fore.GREEN}Data successfully transferred from '{source_table_name}' to SQLite table '{sqlite_table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to transfer data: {e}")
            raise

    def preview_sqlite_data(self, sqlite_table_name, num_records=10):
        """Preview data from the SQLite table using DuckDB."""
        try:
            result = self.connection.execute(f"SELECT * FROM sqlite_db.{sqlite_table_name} LIMIT {num_records};").fetchall()
            print(f"{Fore.CYAN}Previewing {num_records} records from '{sqlite_table_name}':")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}Failed to preview data in SQLite: {e}")
            raise

    def close_connection(self):
        """Close the DuckDB connection."""
        if self.connection:
            self.connection.close()
            print(f"{Fore.GREEN}Connection to DuckDB closed.")

def main():
    # Get the DuckDB database path from the user
    duckdb_db_name = input(f"{Fore.CYAN}Enter the name of the DuckDB database (in 'databases' folder): ").strip()
    duckdb_db_path = os.path.join(DuckDBToSQLite.DATABASE_FOLDER, duckdb_db_name)
    
    if not os.path.exists(duckdb_db_path):
        print(f"{Fore.RED}DuckDB database file does not exist at '{duckdb_db_path}'.")
        return

    # Get the SQLite database path from the user
    sqlite_db_path = input(f"{Fore.CYAN}Enter the path to the SQLite database: ").strip()

    # Initialize the class and connect to DuckDB
    db_tool = DuckDBToSQLite(duckdb_db_path, sqlite_db_path)
    db_tool.connect_to_duckdb()

    # Attach SQLite database
    db_tool.attach_sqlite_database()

    # Define the source table from DuckDB and the target table in SQLite
    source_table_name = input(f"{Fore.CYAN}Enter the name of the DuckDB table to transfer: ").strip()
    sqlite_table_name = input(f"{Fore.CYAN}Enter the name of the SQLite table to create: ").strip()

    # Get column definitions for the source DuckDB table
    column_definitions = db_tool.get_table_columns(source_table_name)

    # Create the target table in SQLite dynamically
    db_tool.create_table_in_sqlite(sqlite_table_name, column_definitions)

    # Transfer data from DuckDB to SQLite
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    # Ask the user if they want to preview the data in SQLite
    preview_action = input(f"{Fore.CYAN}Would you like to preview the data in the SQLite table? (yes/no): ").strip().lower()
    if preview_action == 'yes':
        db_tool.preview_sqlite_data(sqlite_table_name)

    # Close connection
    db_tool.close_connection()

if __name__ == "__main__":
    main()
