import duckdb
import os
import getpass  # For securely handling password input
from colorama import Fore, init
import argparse

from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToPostgreSQL(DuckDBManager):
    DATABASE_FOLDER = "databases"

    def __init__(self, db_path=None, psql_conn_string=None):
        super().__init__(db_path)
        self.psql_conn_string = psql_conn_string

    def attach_postgresql(self):
        """Attach a PostgreSQL database to DuckDB using the provided connection string."""
        try:
            attach_query = f"ATTACH '{self.psql_conn_string}' AS postgres_db (TYPE POSTGRES);"
            self.duckdb_conn.execute(attach_query)
            print(f"{Fore.GREEN}✅ Attached PostgreSQL database to DuckDB.")
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to attach PostgreSQL database: {e}")
            raise

    def get_table_columns(self, table_name):
        """Get the columns of a table in DuckDB."""
        try:
            columns = self.duckdb_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            column_definitions = [f"{column[1]} {column[2]}" for column in columns]
            return column_definitions
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to retrieve table columns: {e}")
            raise

    def create_table_in_psql(self, table_name, column_definitions):
        """Dynamically create a table in PostgreSQL."""
        try:
            create_table_query = f"CREATE TABLE IF NOT EXISTS postgres_db.{table_name} ({', '.join(column_definitions)});"
            self.duckdb_conn.execute(create_table_query)
            print(f"{Fore.GREEN}✅ Created table '{table_name}' in PostgreSQL.")
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to create table in PostgreSQL: {e}")
            raise

    def transfer_data_to_psql(self, source_table_name, psql_table_name):
        """Transfer data from DuckDB to PostgreSQL."""
        try:
            data = self.duckdb_conn.execute(f"SELECT * FROM {source_table_name}").fetchall()
            insert_query = f"INSERT INTO postgres_db.{psql_table_name} VALUES ({', '.join(['?' for _ in data[0]])})"
            self.duckdb_conn.executemany(insert_query, data)
            print(f"{Fore.GREEN}✅ Data successfully transferred from '{source_table_name}' to PostgreSQL table '{psql_table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to transfer data: {e}")
            raise

    def preview_psql_data(self, psql_table_name, num_records=10):
        """Preview data from the PostgreSQL table."""
        try:
            preview_query = f"SELECT * FROM postgres_db.{psql_table_name} LIMIT {num_records};"
            result = self.duckdb_conn.execute(preview_query).fetchall()
            print(f"{Fore.CYAN}👀 Previewing {num_records} records from PostgreSQL table '{psql_table_name}':")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to preview data in PostgreSQL: {e}")
            raise

    def close_connections(self):
        """Close the DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            print(f"{Fore.GREEN}✅ Connection to DuckDB closed.")

def get_postgresql_connection_string():
    """Get individual PostgreSQL connection parameters and assemble the connection string."""
    print(f"{Fore.CYAN}🔐 Please provide the following PostgreSQL connection details:")
    dbname = input(f"{Fore.YELLOW}Enter the PostgreSQL database name (e.g., 'postgres'): ").strip()
    user = input(f"{Fore.YELLOW}Enter the PostgreSQL user (e.g., 'postgres'): ").strip()
    host = input(f"{Fore.YELLOW}Enter the PostgreSQL host (e.g., '127.0.0.1'): ").strip()
    port = input(f"{Fore.YELLOW}Enter the PostgreSQL port (default is 5432): ").strip() or "5432"
    password = getpass.getpass(f"{Fore.YELLOW}Enter the PostgreSQL password: ").strip()
    return f"dbname={dbname} user={user} host={host} port={port} password={password}"

def interactive_mode():
    print(f"{Fore.CYAN}🎮 Running in interactive mode...")

    # Interactive mode: Get user input for database type
    db_choice = input(f"{Fore.CYAN}📂 Do you want to use a file-based DuckDB or an in-memory database? (file/memory): ").strip().lower()

    # Validate the choice
    if db_choice not in ['file', 'memory']:
        print(f"{Fore.RED}❌ Invalid choice. Please enter 'file' or 'memory'.")
        return

    if db_choice == 'file':
        duckdb_db_name = input(f"{Fore.CYAN}📂 Enter the DuckDB database name (located in 'databases' folder): ").strip()
    else:
        duckdb_db_name = ":memory:"

    psql_conn_string = get_postgresql_connection_string()
    db_tool = DuckDBToPostgreSQL(duckdb_db_name, psql_conn_string)
    
    db_tool.connect_to_duckdb()
    db_tool.attach_postgresql()

    source_table_name = input(f"{Fore.CYAN}🗃 Enter the DuckDB table to transfer: ").strip()
    psql_table_name = input(f"{Fore.CYAN}🗂 Enter the PostgreSQL table to create: ").strip()
    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_psql(psql_table_name, column_definitions)
    db_tool.transfer_data_to_psql(source_table_name, psql_table_name)

    if input(f"{Fore.CYAN}👀 Preview PostgreSQL table data? (yes/no): ").strip().lower() == "yes":
        db_tool.preview_psql_data(psql_table_name)
    
    db_tool.close_connections()

def main():
    parser = argparse.ArgumentParser(description="DuckDB to PostgreSQL Transfer Tool")
    parser.add_argument("--cli", action="store_true", help="Trigger interactive mode")
    parser.add_argument("--db", help="Path to the DuckDB database file (use ':memory:' for an in-memory database)")
    parser.add_argument("--psql", help="PostgreSQL connection string")
    parser.add_argument("--table", help="Name of the source table in DuckDB")
    parser.add_argument("--output", help="Name of the target table in PostgreSQL")
    parser.add_argument("--preview", action="store_true", help="Preview data in PostgreSQL after transfer")
    parser.add_argument("--records", type=int, default=10, help="Number of records to preview if --preview is set (default: 10)")
    args = parser.parse_args()

    if args.cli:
        interactive_mode()
        return

    # Validate required arguments for non-interactive mode
    if not (args.db and args.psql and args.table and args.output):
        print(f"{Fore.RED}❌ Error: Missing required arguments in non-interactive mode. "
              f"Please provide --db, --psql, --table, and --output.")
        return

    # Non-interactive mode
    print(f"{Fore.CYAN}🚀 Running in non-interactive mode...")

    # Handle in-memory DuckDB
    duckdb_db_path = ":memory:" if args.db == ":memory:" else os.path.join(DuckDBToPostgreSQL.DATABASE_FOLDER, args.db)

    # Initialize the database tool
    db_tool = DuckDBToPostgreSQL(duckdb_db_path, args.psql)

    try:
        db_tool.connect_to_duckdb()
        db_tool.attach_postgresql()

        # Transfer data
        column_definitions = db_tool.get_table_columns(args.table)
        db_tool.create_table_in_psql(args.output, column_definitions)
        db_tool.transfer_data_to_psql(args.table, args.output)

        if args.preview:
            db_tool.preview_psql_data(args.output, args.records)
    except Exception as e:
        print(f"{Fore.RED}❌ An error occurred: {e}")
    finally:
        db_tool.close_connections()

if __name__ == "__main__":
    main()
