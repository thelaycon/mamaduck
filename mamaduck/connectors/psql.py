import duckdb
from colorama import Fore, Style, init
import argparse
import os

# Initialize colorama for colored CLI output
init(autoreset=True)

class PostgreSQLToDuckDB:
    DATABASE_FOLDER = "databases"

    def __init__(self, db_path=None, psql_conn_string=None):
        self.duckdb_conn = None
        self.db_path = db_path
        self.psql_conn_string = psql_conn_string
        self.ensure_database_folder()


    @staticmethod
    def ensure_database_folder():
        """Ensure the 'databases' folder exists."""
        if not os.path.exists(PostgreSQLToDuckDB.DATABASE_FOLDER):
            os.makedirs(PostgreSQLToDuckDB.DATABASE_FOLDER)
            print(f"{Fore.GREEN}Created folder: '{PostgreSQLToDuckDB.DATABASE_FOLDER}'")


    def connect_to_duckdb(self):
        """Connect to either an in-memory or file-based DuckDB database."""
        try:
            if self.db_path:
                full_path = os.path.join(PostgreSQLToDuckDB.DATABASE_FOLDER, self.db_path)
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

    def list_postgresql_tables(self):
        """List all tables in the PostgreSQL database attached to DuckDB."""
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            tables = self.duckdb_conn.execute(query).fetchall()
            return [row[0] for row in tables]
        except Exception as e:
            print(f"{Fore.RED}Failed to list tables in PostgreSQL: {e}")
            raise

    def migrate_table(self, psql_table, duckdb_table, schema=None):
        """Migrate a table from PostgreSQL to DuckDB."""
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
        """Export the contents of a DuckDB table to a CSV file."""
        try:
            self.duckdb_conn.execute(f"COPY {table_name} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Table '{table_name}' successfully exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to export table to CSV: {e}")
            raise

def get_postgresql_connection_string():
    """Get individual PostgreSQL connection parameters and assemble the connection string."""
    print(f"{Fore.CYAN}Please provide the following PostgreSQL connection details:")

    # Get each part of the connection string
    dbname = input(f"{Fore.YELLOW}Enter the PostgreSQL database name (e.g., 'postgres'): ").strip()
    user = input(f"{Fore.YELLOW}Enter the PostgreSQL user (e.g., 'postgres'): ").strip()
    host = input(f"{Fore.YELLOW}Enter the PostgreSQL host (e.g., '127.0.0.1'): ").strip()
    port = input(f"{Fore.YELLOW}Enter the PostgreSQL port (default is 5432): ").strip() or "5432"
    password = input(f"{Fore.YELLOW}Enter the PostgreSQL password: ").strip()

    # Assemble the PostgreSQL connection string
    psql_conn_string = f"dbname={dbname} user={user} host={host} port={port} password={password}"
    
    return psql_conn_string

def main():
    parser = argparse.ArgumentParser(description="PostgreSQL to DuckDB Migration Tool")

    # Command-line arguments
    parser.add_argument('--db', type=str, help="Path to the DuckDB database file (leave blank for in-memory).")
    parser.add_argument('--psql_conn_string', type=str, help="PostgreSQL connection string.")
    parser.add_argument('--schema', type=str, help="Schema name to use for migration.")
    parser.add_argument('--tables', type=str, nargs='*', help="Comma-separated list of table names to migrate (default: all tables).")
    parser.add_argument('--export', action='store_true', help="Export tables to CSV.")
    parser.add_argument('--csv_dir', type=str, help="Directory to store exported CSV files.")
    parser.add_argument('-cli', action='store_true', help="Trigger the interactive shell mode.")
    
    args = parser.parse_args()

    # If -cli is passed, trigger the interactive mode
    if args.cli:
        print(f"{Fore.CYAN}Welcome to the PostgreSQL to DuckDB Migration Tool in interactive mode!")

        # Get PostgreSQL connection string
        psql_conn_string = get_postgresql_connection_string()

        # Get the DuckDB database path from the user
        db_path = input(f"{Fore.CYAN}Enter DuckDB file name (leave blank for in-memory): ").strip() or None
        db_tool = PostgreSQLToDuckDB(db_path, psql_conn_string)
        db_tool.connect_to_duckdb()

        # Attach PostgreSQL to DuckDB
        try:
            db_tool.attach_postgresql()
        except Exception:
            return

        # List PostgreSQL tables
        try:
            tables = db_tool.list_postgresql_tables()
            print(f"{Fore.GREEN}Tables in PostgreSQL database: {', '.join(tables) if tables else 'No tables found.'}")
        except Exception:
            return

        # Schema setup
        schema = input(f"{Fore.CYAN}Enter schema name (leave blank for no schema): ").strip() or None
        if schema:
            db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Migrate tables
        migrate_choice = input(f"{Fore.CYAN}Migrate all tables or single table? (all/single): ").strip().lower()
        if migrate_choice == 'single':
            psql_table = input(f"{Fore.CYAN}Enter the PostgreSQL table to migrate: ").strip()
            duckdb_table = input(f"{Fore.CYAN}Enter the DuckDB table name: ").strip()
            db_tool.migrate_table(psql_table, duckdb_table, schema)
        elif migrate_choice == 'all':
            for table in tables:
                db_tool.migrate_table(table, table, schema)
        else:
            print(f"{Fore.RED}Invalid option.")
            return

        # Export tables to CSV
        if args.export:
            if not args.csv_dir:
                print(f"{Fore.RED}Error: CSV directory is required for exporting tables.")
                return

            os.makedirs(args.csv_dir, exist_ok=True)

            for table in tables if migrate_choice == 'all' else [duckdb_table]:
                output_file = os.path.join(args.csv_dir, f"{table}.csv")
                db_tool.export_table_to_csv(table if not schema else f"{schema}.{table}", output_file)

        print(f"{Fore.GREEN}Process completed successfully. Goodbye!")

    else:
        # If no -cli argument is passed, process other command-line arguments
        if not args.psql_conn_string:
            print(f"{Fore.RED}Error: PostgreSQL connection string is required.")
            return

        psql_conn_string = args.psql_conn_string
        db_tool = PostgreSQLToDuckDB(args.db, psql_conn_string)
        db_tool.connect_to_duckdb()

        # Attach PostgreSQL to DuckDB
        try:
            db_tool.attach_postgresql()
        except Exception:
            return

        # List PostgreSQL tables
        try:
            tables = db_tool.list_postgresql_tables()
            print(f"{Fore.GREEN}Tables in PostgreSQL database: {', '.join(tables) if tables else 'No tables found.'}")
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
                    db_tool.migrate_table(table, table, schema)
                else:
                    print(f"{Fore.RED}Table '{table}' not found in PostgreSQL database.")
        else:
            for table in tables:
                db_tool.migrate_table(table, table, schema)

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
    

if __name__ == "__main__":
    main()
