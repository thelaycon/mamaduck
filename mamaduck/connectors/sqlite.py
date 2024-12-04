import duckdb
from colorama import Fore, init
import argparse
import os

from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class SQLiteToDuckDB(DuckDBManager):
    def load_sqlite_extension(self):
        """Install and load the SQLite extension for DuckDB."""
        try:
            self.duckdb_conn.execute("INSTALL sqlite;")
            self.duckdb_conn.execute("LOAD sqlite;")
            print(f"{Fore.GREEN}SQLite extension successfully loaded into DuckDB.")
        except Exception as e:
            print(f"{Fore.RED}Failed to load SQLite extension: {e}")
            raise

    def list_sqlite_tables(self, sqlite_path):
        """List all tables in the SQLite database."""
        try:
            query = f"""
            SELECT name 
            FROM sqlite_scan('{sqlite_path}', 'sqlite_master') 
            WHERE type = 'table';
            """
            tables = self.duckdb_conn.execute(query).fetchall()
            return [row[0] for row in tables]
        except Exception as e:
            print(f"{Fore.RED}Failed to list tables in SQLite: {e}")
            raise

    def migrate_table(self, sqlite_path, sqlite_table, duckdb_table, schema=None):
        """Migrate a table from SQLite to DuckDB."""
        try:
            table_name = f"{schema}.{duckdb_table}" if schema else duckdb_table
            self.duckdb_conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM sqlite_scan('{sqlite_path}', '{sqlite_table}');
            """)
            print(f"{Fore.GREEN}Table '{sqlite_table}' successfully migrated to DuckDB as '{duckdb_table}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to migrate table: {e}")
            raise

    def export_table_to_csv(self, table_name, output_file):
        """Export the contents of a DuckDB table to a CSV file."""
        try:
            self.duckdb_conn.execute(f"COPY {table_name} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Table '{table_name}' successfully exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to export table to CSV: {e}")
            raise

def start_interactive_mode():
    """Function to handle interactive shell mode."""
    print(f"{Fore.CYAN}Welcome to the SQLite to DuckDB Migration Tool in interactive mode!")

    # Connect to DuckDB
    duckdb_path = input(f"{Fore.CYAN}Enter DuckDB file name (leave blank for in-memory): ").strip() or None
    db_tool = SQLiteToDuckDB(duckdb_path)
    db_tool.connect_to_duckdb()

    # Load SQLite extension
    try:
        db_tool.load_sqlite_extension()
    except Exception:
        return

    # SQLite database path
    sqlite_path = input(f"{Fore.CYAN}Enter SQLite database file path: ").strip()

    # List SQLite tables
    try:
        tables = db_tool.list_sqlite_tables(sqlite_path)
        print(f"{Fore.GREEN}Tables in SQLite database: {', '.join(tables) if tables else 'No tables found.'}")
    except Exception:
        return

    # Schema setup
    schema = input(f"{Fore.CYAN}Enter schema name (leave blank for no schema): ").strip() or None
    if schema:
        db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Migrate tables
    migrate_choice = input(f"{Fore.CYAN}Migrate all tables or single table? (all/single): ").strip().lower()
    if migrate_choice == 'single':
        sqlite_table = input(f"{Fore.CYAN}Enter the SQLite table to migrate: ").strip()
        duckdb_table = input(f"{Fore.CYAN}Enter the DuckDB table name: ").strip()
        db_tool.migrate_table(sqlite_path, sqlite_table, duckdb_table, schema)
    elif migrate_choice == 'all':
        for table in tables:
            db_tool.migrate_table(sqlite_path, table, table, schema)
    else:
        print(f"{Fore.RED}Invalid option.")
        return

    # Export tables to CSV
    export_choice = input(f"{Fore.CYAN}Export tables to CSV? (yes/no): ").strip().lower()
    if export_choice == 'yes':
        csv_dir = input(f"{Fore.CYAN}Enter directory to store exported CSV files: ").strip()
        if not csv_dir:
            print(f"{Fore.RED}Error: CSV directory is required for exporting tables.")
            return

        os.makedirs(csv_dir, exist_ok=True)

        for table in tables if migrate_choice == 'all' else [duckdb_table]:
            output_file = os.path.join(csv_dir, f"{table}.csv")
            db_tool.export_table_to_csv(table if not schema else f"{schema}.{table}", output_file)

    print(f"{Fore.GREEN}Process completed successfully. Goodbye!")

def process_cli_arguments():
    """Function to process non-interactive CLI arguments."""
    parser = argparse.ArgumentParser(description="SQLite to DuckDB Migration Tool")
    parser.add_argument('--db', type=str, help="Path to the DuckDB database file (leave blank for in-memory).")
    parser.add_argument('--sqlite', type=str, help="Path to the SQLite database file.")
    parser.add_argument('--schema', type=str, help="Schema name to use for migration.")
    parser.add_argument('--tables', type=str, nargs='*', help="Comma-separated list of table names to migrate (default: all tables).")
    parser.add_argument('--export', action='store_true', help="Export tables to CSV.")
    parser.add_argument('--csv_dir', type=str, help="Directory to store exported CSV files.")
    parser.add_argument('--cli', action='store_true', help="Trigger the interactive shell mode.")
    
    args = parser.parse_args()

    if args.cli:
        start_interactive_mode()
    else:
        if not args.sqlite:
            print(f"{Fore.RED}Error: SQLite database file path is required.")
            return
        
        sqlite_path = args.sqlite
        db_tool = SQLiteToDuckDB(args.db)
        db_tool.connect_to_duckdb()

        # Load SQLite extension
        try:
            db_tool.load_sqlite_extension()
        except Exception:
            return

        # List SQLite tables
        try:
            tables = db_tool.list_sqlite_tables(sqlite_path)
            print(f"{Fore.GREEN}Tables in SQLite database: {', '.join(tables) if tables else 'No tables found.'}")
        except Exception:
            return

        # Schema setup
        schema = args.schema
        if schema:
            db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Migrate specified tables
        if args.tables:
            for table in args.tables:
                if table in tables:
                    db_tool.migrate_table(sqlite_path, table, table, schema)
                else:
                    print(f"{Fore.RED}Table '{table}' not found in SQLite database.")
        else:
            for table in tables:
                db_tool.migrate_table(sqlite_path, table, table, schema)

        # Export tables to CSV
        if args.export:
            if not args.csv_dir:
                print(f"{Fore.RED}Error: CSV directory is required for exporting tables.")
                return

            os.makedirs(args.csv_dir, exist_ok=True)

            for table in tables if args.tables is None else args.tables:
                output_file = os.path.join(args.csv_dir, f"{table}.csv")
                db_tool.export_table_to_csv(table if not schema else f"{schema}.{table}", output_file)

        print(f"{Fore.GREEN}Process completed successfully. Goodbye!")

def main():
    process_cli_arguments()

if __name__ == "__main__":
    main()
