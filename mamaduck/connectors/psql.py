import duckdb
from colorama import Fore, Style, init
import argparse
import os

from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class PostgreSQLToDuckDB(DuckDBManager):
    def __init__(self, db_path=None, psql_conn_string=None):
        super().__init__(db_path)
        self.psql_conn_string = psql_conn_string

    def attach_postgresql(self):
        try:
            attach_query = f"ATTACH '{self.psql_conn_string}' AS postgres_db (TYPE POSTGRES);"
            self.duckdb_conn.execute(attach_query)
            print(f"{Fore.GREEN}Attached PostgreSQL database to DuckDB.")
        except Exception as e:
            print(f"{Fore.RED}Failed to attach PostgreSQL database: {e}")
            raise

    def list_postgresql_tables(self):
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            tables = self.duckdb_conn.execute(query).fetchall()
            return [row[0] for row in tables]
        except Exception as e:
            print(f"{Fore.RED}Failed to list tables in PostgreSQL: {e}")
            raise

    def migrate_table(self, psql_table, duckdb_table, schema=None):
        try:
            table_name = f"{schema}.{duckdb_table}" if schema else duckdb_table
            self.duckdb_conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM postgres_db.{psql_table};
            """)
            print(f"{Fore.GREEN}Table '{psql_table}' successfully migrated to DuckDB as '{duckdb_table}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to migrate table: {e}")
            raise

    def export_table_to_csv(self, table_name, output_file):
        try:
            self.duckdb_conn.execute(f"COPY {table_name} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Table '{table_name}' successfully exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to export table to CSV: {e}")
            raise

def get_postgresql_connection_string():
    """Get individual PostgreSQL connection parameters and assemble the connection string."""
    print(f"{Fore.CYAN}Please provide the following PostgreSQL connection details:")
    dbname = input(f"{Fore.YELLOW}Enter the PostgreSQL database name (e.g., 'postgres'): ").strip()
    user = input(f"{Fore.YELLOW}Enter the PostgreSQL user (e.g., 'postgres'): ").strip()
    host = input(f"{Fore.YELLOW}Enter the PostgreSQL host (e.g., '127.0.0.1'): ").strip()
    port = input(f"{Fore.YELLOW}Enter the PostgreSQL port (default is 5432): ").strip() or "5432"
    password = input(f"{Fore.YELLOW}Enter the PostgreSQL password: ").strip()

    psql_conn_string = f"dbname={dbname} user={user} host={host} port={port} password={password}"
    return psql_conn_string

def initialize_duckdb_and_attach_postgresql(psql_conn_string):
    """Initialize DuckDB and attach the PostgreSQL database."""
    db_path = input(f"{Fore.CYAN}Enter DuckDB file name (leave blank for in-memory): ").strip() or None
    db_tool = PostgreSQLToDuckDB(db_path, psql_conn_string)
    db_tool.connect_to_duckdb()

    try:
        db_tool.attach_postgresql()
    except Exception:
        return db_tool
    return db_tool

def list_and_migrate_tables(db_tool, migrate_choice, schema):
    """List PostgreSQL tables and migrate them."""
    try:
        tables = db_tool.list_postgresql_tables()
        print(f"{Fore.GREEN}Tables in PostgreSQL database: {', '.join(tables) if tables else 'No tables found.'}")
    except Exception:
        return

    if schema:
        db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    if migrate_choice == 'single':
        psql_table = input(f"{Fore.CYAN}Enter the PostgreSQL table to migrate: ").strip()
        duckdb_table = input(f"{Fore.CYAN}Enter the DuckDB table name: ").strip()
        db_tool.migrate_table(psql_table, duckdb_table, schema)
    elif migrate_choice == 'all':
        for table in tables:
            db_tool.migrate_table(table, table, schema)
    else:
        print(f"{Fore.RED}Invalid option.")

def export_tables_to_csv(db_tool, tables, schema, csv_dir):
    """Export migrated tables to CSV."""
    if not csv_dir:
        print(f"{Fore.RED}Error: CSV directory is required for exporting tables.")
        return

    os.makedirs(csv_dir, exist_ok=True)
    for table in tables:
        output_file = os.path.join(csv_dir, f"{table}.csv")
        db_tool.export_table_to_csv(table if not schema else f"{schema}.{table}", output_file)

def start_interactive_mode():
    """Interactive mode for PostgreSQL to DuckDB migration."""
    print(f"{Fore.CYAN}Welcome to the PostgreSQL to DuckDB Migration Tool in interactive mode!")

    psql_conn_string = get_postgresql_connection_string()
    db_tool = initialize_duckdb_and_attach_postgresql(psql_conn_string)
    if not db_tool:
        return

    tables = db_tool.list_postgresql_tables()
    schema = input(f"{Fore.CYAN}Enter schema name (leave blank for no schema): ").strip() or None

    migrate_choice = input(f"{Fore.CYAN}Migrate all tables or single table? (all/single): ").strip().lower()
    list_and_migrate_tables(db_tool, migrate_choice, schema)

    csv_dir = input(f"{Fore.CYAN}Enter directory to store exported CSV files (leave blank to skip): ").strip()

    # If the user provides a directory, proceed with exporting tables to CSV
    if csv_dir:
        export_tables_to_csv(db_tool, tables, schema, csv_dir)
    else:
        print(f"{Fore.YELLOW}Skipping CSV export...")

    print(f"{Fore.GREEN}Process completed successfully. Goodbye!")

def process_cli_arguments():
    """Process command-line arguments."""
    parser = argparse.ArgumentParser(description="PostgreSQL to DuckDB Migration Tool")

    parser.add_argument('--db', type=str, help="Path to the DuckDB database file (leave blank for in-memory).")
    parser.add_argument('--psql_conn_string', type=str, help="PostgreSQL connection string.")
    parser.add_argument('--schema', type=str, help="Schema name to use for migration.")
    parser.add_argument('--tables', type=str, nargs='*', help="Comma-separated list of table names to migrate (default: all tables).")
    parser.add_argument('--export', action='store_true', help="Export tables to CSV.")
    parser.add_argument('--csv_dir', type=str, help="Directory to store exported CSV files.")
    parser.add_argument('--cli', action='store_true', help="Trigger the interactive shell mode.")
    
    return parser.parse_args()

def main():
    args = process_cli_arguments()

    if args.cli:
        start_interactive_mode()
        return

    # Validate required arguments for non-interactive mode
    if not args.db or not args.psql_conn_string or not args.tables:
        print(f"{Fore.RED}Error: '--db', '--psql_conn_string', and '--table' arguments are required in non-interactive mode.")
        return

    if not args.psql_conn_string:
        print(f"{Fore.RED}Error: PostgreSQL connection string is required.")
        return

    psql_conn_string = args.psql_conn_string
    db_tool = PostgreSQLToDuckDB(args.db, psql_conn_string)
    db_tool.connect_to_duckdb()

    try:
        db_tool.attach_postgresql()
    except Exception:
        return

    try:
        tables = db_tool.list_postgresql_tables()
        print(f"{Fore.GREEN}Tables in PostgreSQL database: {', '.join(tables) if tables else 'No tables found.'}")
    except Exception:
        return

    schema = args.schema
    if schema:
        db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    if args.tables:
        for table in args.tables:
            if table in tables:
                db_tool.migrate_table(table, table, schema)
            else:
                print(f"{Fore.RED}Table '{table}' not found in PostgreSQL database.")
    else:
        for table in tables:
            db_tool.migrate_table(table, table, schema)

    if args.export:
        if not args.csv_dir:
            print(f"{Fore.RED}Error: CSV directory is required for exporting tables.")
            return

        os.makedirs(args.csv_dir, exist_ok=True)
        for table in tables if args.tables is None else args.tables:
            output_file = os.path.join(args.csv_dir, f"{table}.csv")
            db_tool.export_table_to_csv(table if not schema else f"{schema}.{table}", output_file)

    print(f"{Fore.GREEN}Process completed successfully. Goodbye!")

if __name__ == "__main__":
    main()
