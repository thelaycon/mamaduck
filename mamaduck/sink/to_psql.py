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
    print(f"{Fore.CYAN}🦆 MamaDuck")

    # Choose database type (in-memory or file)
    db_choice = input(f"{Fore.CYAN}💡 Use in-memory or persistent file DB? (memory/file): ").strip().lower()
    if db_choice == 'file':
        db_path = input(f"{Fore.CYAN}🔑 Enter DuckDB file name (existing/new): ").strip()
    elif db_choice == 'memory':
        db_path = None
    else:
        print(f"{Fore.RED}❌ Invalid choice. Choose 'memory' or 'file'.")
        return

    psql_conn_string = get_postgresql_connection_string()
    db_tool = DuckDBToPostgreSQL(db_path, psql_conn_string)
    
    db_tool.connect_to_duckdb()
    db_tool.attach_postgresql()

    source_table_name = input(f"{Fore.CYAN}🗃 Enter the DuckDB table to transfer: ").strip()
    psql_table_name = input(f"{Fore.CYAN}🗂 Enter the PostgreSQL table to create: ").strip()
    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_psql(psql_table_name, column_definitions)
    db_tool.transfer_data_to_psql(source_table_name, psql_table_name)
    
    db_tool.close_duckdb_conn()
    print(f"{Fore.GREEN}✅ Export completed.")

def main():
    parser = argparse.ArgumentParser(description="DuckDB to PostgreSQL Transfer Tool")
    parser.add_argument("--cli", action="store_true", help="Trigger interactive mode")
    parser.add_argument("--db", help="Path to DuckDB DB file (leave blank for in-memory).")
    parser.add_argument("--psql", help="PostgreSQL connection string")
    parser.add_argument("--table", help="Name of the source table in DuckDB")
    parser.add_argument("--output", help="Name of the target table in PostgreSQL")

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
    # Handle in-memory DuckDB
    db_path = args.db

    # Initialize the database tool
    db_tool = DuckDBToPostgreSQL(db_path, args.psql)

    try:
        db_tool.connect_to_duckdb()
        db_tool.attach_postgresql()

        # Transfer data
        column_definitions = db_tool.get_table_columns(args.table)
        db_tool.create_table_in_psql(args.output, column_definitions)
        db_tool.transfer_data_to_psql(args.table, args.output)

    except Exception as e:
        print(f"{Fore.RED}❌ An error occurred: {e}")
    finally:
        db_tool.close_duckdb_conn()
    
    print(f"{Fore.GREEN}✅ Export completed.")

if __name__ == "__main__":
    main()
