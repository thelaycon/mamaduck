import argparse
import duckdb
import os
from colorama import Fore, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToSQLite:
    DATABASE_FOLDER = "databases"

    def __init__(self, db_path, sqlite_db_path):
        self.db_path = db_path
        self.sqlite_db_path = sqlite_db_path
        self.connection = None

    def connect_to_duckdb(self):
        """Connect to the DuckDB database."""
        try:
            full_path = os.path.join('databases', self.db_path)
            self.connection = duckdb.connect(full_path)
            print(f"{Fore.GREEN}Connected to DuckDB database at '{self.db_path}'.")
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
    parser = argparse.ArgumentParser(
        description="Transfer data from a DuckDB database to an SQLite database."
    )
    parser.add_argument(
        "-d", "--duckdb",
        help="Path to the DuckDB database file (relative to the 'databases' folder)."
    )
    parser.add_argument(
        "-s", "--sqlite",
        help="Path to the SQLite database file."
    )
    parser.add_argument(
        "-t", "--table",
        help="Name of the source table in DuckDB."
    )
    parser.add_argument(
        "-n", "--newtable",
        help="Name of the new table to create in SQLite."
    )
    parser.add_argument(
        "-p", "--preview", action="store_true",
        help="Preview the data in the SQLite table after transfer."
    )
    parser.add_argument(
        "-r", "--records", type=int, default=10,
        help="Number of records to preview if --preview is enabled (default: 10)."
    )
    args = parser.parse_args()

    # Interactive fallback if arguments are not provided
    db_path = os.path.join(DuckDBToSQLite.DATABASE_FOLDER, args.duckdb) if args.duckdb else input(
        f"{Fore.CYAN}Enter the name of the DuckDB database (in 'databases' folder): ").strip()
    sqlite_db_path = args.sqlite or input(f"{Fore.CYAN}Enter the path to the SQLite database: ").strip()
    source_table_name = args.table or input(f"{Fore.CYAN}Enter the name of the DuckDB table to transfer: ").strip()
    sqlite_table_name = args.newtable or input(f"{Fore.CYAN}Enter the name of the SQLite table to create: ").strip()
    preview = args.preview or input(f"{Fore.CYAN}Would you like to preview the data in SQLite? (yes/no): ").strip().lower() == 'yes'
    records = args.records if args.preview else int(input(f"{Fore.CYAN}Enter the number of records to preview: "))

    db_tool = DuckDBToSQLite(db_path, sqlite_db_path)
    db_tool.connect_to_duckdb()
    db_tool.attach_sqlite_database()

    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_sqlite(sqlite_table_name, column_definitions)
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    if preview:
        db_tool.preview_sqlite_data(sqlite_table_name, records)

    db_tool.close_connection()

if __name__ == "__main__":
    main()
