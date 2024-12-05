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

def start_interactive_mode():
    """Function to handle interactive shell mode."""
    print(f"{Fore.CYAN}👋 MamaDuck")

    # Choose database type (in-memory or file)
    db_choice = input(f"{Fore.CYAN}💡 Use in-memory or persistent file DB? (memory/file): ").strip().lower()
    if db_choice == 'file':
        db_path = input(f"{Fore.CYAN}🔑 Enter DuckDB file name (existing/new): ").strip()
    elif db_choice == 'memory':
        db_path = None
    else:
        print(f"{Fore.RED}❌ Invalid choice. Choose 'memory' or 'file'.")
        return

    db_tool = SQLiteToDuckDB(db_path)
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
        print(f"{Fore.GREEN}✅ Tables in SQLite database: {', '.join(tables) if tables else 'No tables found.'}")
    except Exception:
        return

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

    if schema:
        db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Migrate tables
    migrate_choice = input(f"{Fore.CYAN}🚀 Migrate all tables or a single table? (all/single): ").strip().lower()
    if migrate_choice == 'single':
        sqlite_table = input(f"{Fore.CYAN}Enter the SQLite table to migrate: ").strip()
        duckdb_table = input(f"{Fore.CYAN}Enter the DuckDB table name: ").strip()
        db_tool.migrate_table(sqlite_path, sqlite_table, duckdb_table, schema)
    elif migrate_choice == 'all':
        for table in tables:
            db_tool.migrate_table(sqlite_path, table, table, schema)
    else:
        print(f"{Fore.RED}❌ Invalid option.")
        return

    print(f"{Fore.GREEN}✅ Migration completed successfully.")

def process_cli_arguments(args):       
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
                print(f"{Fore.RED}❌ Table '{table}' not found in SQLite database.")
    else:
        for table in tables:
            db_tool.migrate_table(sqlite_path, table, table, schema)

    print(f"{Fore.GREEN}✅ Migration completed successfully.")

def main():
    """Function to process non-interactive CLI arguments."""
    parser = argparse.ArgumentParser(description="SQLite to DuckDB Migration Tool")
    parser.add_argument('--db', type=str, help="Path to the DuckDB database file (leave blank for in-memory).")
    parser.add_argument('--sqlite', type=str, help="Path to the SQLite database file.")
    parser.add_argument('--schema', type=str, help="Schema name to use for migration.")
    parser.add_argument('--tables', type=str, nargs='*', help="Comma-separated list of table names to migrate (default: all tables).")
    parser.add_argument('--cli', action='store_true', help="Trigger the interactive shell mode.")
    
    args = parser.parse_args()

    if args.cli:
        start_interactive_mode()
        return

    # Validate required arguments for non-interactive mode
    if not args.db or not args.sqlite or not args.tables:
        print(f"{Fore.RED}❌ Error: --db, --sqlite, and --tables are required.")
        return

    process_cli_arguments(args)

if __name__ == "__main__":
    main()
