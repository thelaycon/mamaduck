import duckdb
from colorama import Fore, Style, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class SQLiteToDuckDB:
    def __init__(self, duckdb_path=None):
        self.duckdb_conn = None
        self.duckdb_path = duckdb_path

    def connect_to_duckdb(self):
        """Connect to either an in-memory or file-based DuckDB database."""
        if self.duckdb_path:
            self.duckdb_conn = duckdb.connect(database=self.duckdb_path)
            print(f"{Fore.GREEN}Connected to DuckDB database file '{self.duckdb_path}'.")
        else:
            self.duckdb_conn = duckdb.connect(database=':memory:')
            print(f"{Fore.GREEN}Connected to an in-memory DuckDB database.")

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
            tables = [row[0] for row in tables]
            print(f"{Fore.GREEN}Found the following tables in SQLite database '{sqlite_path}':")
            for table in tables:
                print(f"{Fore.YELLOW}- {table}")
            return tables
        except Exception as e:
            print(f"{Fore.RED}Failed to list tables in SQLite: {e}")
            raise

    def migrate_table(self, sqlite_path, sqlite_table, duckdb_table, schema):
        """Migrate a table from SQLite to DuckDB."""
        try:
            if schema:
                query = f"""
                CREATE TABLE {schema}.{duckdb_table} AS 
                SELECT * FROM sqlite_scan('{sqlite_path}', '{sqlite_table}');
                """
            else:
                query = f"""
                CREATE TABLE {duckdb_table} AS 
                SELECT * FROM sqlite_scan('{sqlite_path}', '{sqlite_table}');
                """
            self.duckdb_conn.execute(query)
            print(f"{Fore.GREEN}Table '{sqlite_table}' successfully migrated to DuckDB as '{duckdb_table}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to migrate table: {e}")
            raise

    def export_table_to_csv(self, duckdb_table, output_file):
        """Export the contents of a DuckDB table to a CSV file."""
        try:
            print(f"{Fore.BLUE}Exporting table '{duckdb_table}' to file '{output_file}'...")
            self.duckdb_conn.execute(f"COPY {duckdb_table} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Table '{duckdb_table}' successfully exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to export table to CSV: {e}")
            raise

def main():
    print(f"{Fore.CYAN}Welcome to the SQLite to DuckDB Migration Tool!")

    # DuckDB: Choose memory or file-based
    print(f"{Fore.CYAN}Would you like to use an in-memory DuckDB database or a persistent file? (memory/file): ", end="")
    duckdb_choice = input().strip().lower()
    if duckdb_choice == 'file':
        print(f"{Fore.CYAN}Enter the DuckDB file name (existing or new): ", end="")
        duckdb_path = input().strip()
    elif duckdb_choice == 'memory':
        duckdb_path = None
    else:
        print(f"{Fore.RED}Invalid choice. Please choose 'memory' or 'file'.")
        return

    # Initialize DuckDB connection
    db_tool = SQLiteToDuckDB(duckdb_path)
    db_tool.connect_to_duckdb()

    # Load SQLite extension
    try:
        db_tool.load_sqlite_extension()
    except Exception:
        return

    # SQLite: Specify file path
    print(f"{Fore.CYAN}Enter the SQLite database file name (with path if needed): ", end="")
    sqlite_path = input().strip()

    # List tables in SQLite database
    try:
        tables = db_tool.list_sqlite_tables(sqlite_path)
    except Exception:
        return

    # Prompt user to choose or create schema
    print(f"{Fore.CYAN}Would you like to use an existing schema or create a new one? (existing/new): ", end="")
    schema_choice = input().strip().lower()

    if schema_choice == 'new':
        print(f"{Fore.CYAN}Enter the name of the new schema: ", end="")
        schema_name = input().strip()
        try:
            db_tool.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            print(f"{Fore.GREEN}Schema '{schema_name}' created successfully.")
        except Exception as e:
            print(f"{Fore.RED}Failed to create schema: {e}")
            return
    elif schema_choice == 'existing':
        # List schemas in DuckDB
        schemas = db_tool.duckdb_conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
        schemas = [schema[0] for schema in schemas]
        print(f"{Fore.GREEN}Existing schemas in DuckDB:")
        for schema in schemas:
            print(f"{Fore.YELLOW}- {schema}")
        print(f"{Fore.CYAN}Enter the name of the schema to use: ", end="")
        schema_name = input().strip()
        if schema_name not in schemas:
            print(f"{Fore.RED}Schema '{schema_name}' does not exist.")
            return
    else:
        print(f"{Fore.RED}Invalid choice. Please choose 'existing' or 'new'.")
        return

    # Prompt user for action
    print(f"{Fore.CYAN}Would you like to migrate a specific table or all tables? (single/all): ", end="")
    action = input().strip().lower()

    if action == 'single':
        # Single table migration
        print(f"{Fore.CYAN}Enter the name of the table to migrate from SQLite to DuckDB: ", end="")
        sqlite_table = input().strip()
        if sqlite_table not in tables:
            print(f"{Fore.RED}Table '{sqlite_table}' does not exist in the SQLite database.")
            return

        print(f"{Fore.CYAN}Enter the name of the table to create in DuckDB: ", end="")
        duckdb_table = input().strip()
        try:
            db_tool.migrate_table(sqlite_path, sqlite_table, duckdb_table, schema_name)
        except Exception:
            return

        # Export to CSV (optional)
        print(f"{Fore.CYAN}Would you like to export the DuckDB table to a CSV file? (yes/no): ", end="")
        export_action = input().strip().lower()
        if export_action == 'yes':
            print(f"{Fore.CYAN}Enter the output file name (with path if needed): ", end="")
            output_file = input().strip()
            try:
                db_tool.export_table_to_csv(duckdb_table, output_file)
            except Exception:
                return

    elif action == 'all':
        # Migrate all tables
        for sqlite_table in tables:
            duckdb_table = sqlite_table  # Use the same name for simplicity
            try:
                db_tool.migrate_table(sqlite_path, sqlite_table, duckdb_table, schema_name)
            except Exception:
                continue

        # Export all tables to CSV (optional)
        print(f"{Fore.CYAN}Would you like to export all DuckDB tables to CSV files? (yes/no): ", end="")
        export_action = input().strip().lower()
        if export_action == 'yes':
            for duckdb_table in tables:
                output_file = f"{duckdb_table}.csv"
                try:
                    db_tool.export_table_to_csv(duckdb_table, output_file)
                except Exception:
                    continue

    else:
        print(f"{Fore.RED}Invalid choice. Please choose 'single' or 'all'.")
        return

    print(f"{Fore.GREEN}Process completed. Goodbye!")

if __name__ == "__main__":
    main()
