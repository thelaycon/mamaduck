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

    # Choose database type (in-memory or file)
    db_choice = input(f"{Fore.CYAN}💡 Use in-memory or persistent file DB? (memory/file): ").strip().lower()
    if db_choice == 'file':
        db_path = input(f"{Fore.CYAN}🔑 Enter DuckDB file name (existing/new): ").strip()
    elif db_choice == 'memory':
        db_path = None
    else:
        print(f"{Fore.RED}❌ Invalid choice. Choose 'memory' or 'file'.")
        return
    
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
        print(f"{Fore.RED}❌ Invalid option.")


def start_interactive_mode():
    """Interactive mode for PostgreSQL to DuckDB migration."""
    print(f"{Fore.CYAN}👋 MamaDuck")

    psql_conn_string = get_postgresql_connection_string()
    db_tool = initialize_duckdb_and_attach_postgresql(psql_conn_string)
    if not db_tool:
        return

    print(f"{Fore.GREEN}✅ Connected to DuckDB and PostgreSQL successfully.")

    tables = db_tool.list_postgresql_tables()

    # Schema handling
    schema_action = input(f"{Fore.CYAN}🔨 Create new schema or choose an existing? (create/choose/none): ").strip().lower()
    schema = None
    if schema_action == 'create':
        schema = input(f"{Fore.CYAN}📝 Enter new schema name: ").strip()
    elif schema_action == 'choose':
        print(f"{Fore.CYAN}📋 Existing schemas:")
        schemas = db_tool.duckdb_conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
        schemas = [s[0] for s in schemas]
        print(f"{Fore.CYAN}{schemas}")
        schema = input(f"{Fore.CYAN}Enter schema name: ").strip()
    elif schema_action != 'none':
        print(f"{Fore.RED}❌ Invalid choice. Choose 'create', 'choose', or 'none'.")
        return

    migrate_choice = input(f"{Fore.CYAN}🚀 Migrate all tables or a single table? (all/single): ").strip().lower()
    list_and_migrate_tables(db_tool, migrate_choice, schema)

    print(f"{Fore.GREEN}✅ Migration completed successfully.")

def process_cli_arguments():
    """Process command-line arguments."""
    parser = argparse.ArgumentParser(description="PostgreSQL to DuckDB Migration Tool")

    parser.add_argument('--db', type=str, help="Path to the DuckDB database file (leave blank for in-memory).")
    parser.add_argument('--psql_conn_string', type=str, help="PostgreSQL connection string.")
    parser.add_argument('--schema', type=str, help="Schema name to use for migration.")
    parser.add_argument('--tables', type=str, nargs='*', help="Comma-separated list of table names to migrate (default: all tables).")
    parser.add_argument('--cli', action='store_true', help="Trigger the interactive shell mode.")
    
    return parser.parse_args()

def main():
    args = process_cli_arguments()

    if args.cli:
        start_interactive_mode()
        return

    # Validate required arguments for non-interactive mode
    if not args.db or not args.psql_conn_string or not args.tables:
        print(f"{Fore.RED}❌ Error: '--db', '--psql_conn_string', and '--tables' arguments are required.")
        return

    if not args.psql_conn_string:
        print(f"{Fore.RED}❌ Error: PostgreSQL connection string is required.")
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
        print(f"{Fore.GREEN}✅ Tables in PostgreSQL database: {', '.join(tables) if tables else 'No tables found.'}")
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
                print(f"{Fore.RED}❌ Table '{table}' not found in PostgreSQL.")
    else:
        for table in tables:
            db_tool.migrate_table(table, table, schema)

    print(f"{Fore.GREEN}✅ Migration successfully! 👋")

if __name__ == "__main__":
    main()
