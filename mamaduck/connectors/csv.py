import duckdb
import os
import argparse
from colorama import Fore, Style, init

from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class CSVToDuckDB(DuckDBManager):
    
    def load_csv_to_table(self, file_name, table_name, schema=None):
        """Load CSV into DuckDB table."""
        try:
            if schema:
                print(f"{Fore.CYAN}📥 Loading CSV '{file_name}' into '{schema}.{table_name}'...")
                self.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                self.duckdb_conn.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_csv_auto('{file_name}');")
            else:
                print(f"{Fore.CYAN}📥 Loading CSV '{file_name}' into '{table_name}'...")
                self.duckdb_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_name}');")
            print(f"{Fore.GREEN}✅ CSV successfully loaded into '{table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}❌ Error: {e}")
            raise

    def query_table(self, table_name, schema=None):
        """Query DuckDB table."""
        try:
            table = f"{schema}.{table_name}" if schema else table_name
            print(f"{Fore.CYAN}🔍 Fetching data from '{table}'...")
            result = self.duckdb_conn.execute(f"SELECT * FROM {table} LIMIT 10;").fetchall()
            columns = [desc[0] for desc in self.duckdb_conn.execute(f"PRAGMA table_info('{table}');").fetchall()]
            print(f"{Fore.GREEN}✅ Showing 10 records from '{table}':")
            print(f"{Fore.CYAN}{columns}")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}❌ Error: {e}")
            raise

    def export_table_to_csv(self, table_name, output_file, schema=None):
        """Export DuckDB table to CSV."""
        try:
            table = f"{schema}.{table_name}" if schema else table_name
            print(f"{Fore.CYAN}📤 Exporting '{table}' to '{output_file}'...")
            self.duckdb_conn.execute(f"COPY {table} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}✅ Table '{table}' exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}❌ Error: {e}")
            raise

def start_interactive_mode():
    """Interactive CSV to DuckDB tool."""
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

    # Initialize and connect to DuckDB
    db_tool = CSVToDuckDB(db_path)
    db_tool.connect_to_duckdb()

    print(f"{Fore.GREEN}✅ Connected to DuckDB successfully.")

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

    # CSV file name
    file_name = input(f"{Fore.CYAN}📄 Enter CSV file name (with path if needed): ").strip()
    if not os.path.exists(file_name):
        print(f"{Fore.RED}❌ File '{file_name}' does not exist.")
        return

    # Table name
    table_name = input(f"{Fore.CYAN}🔑 Enter DuckDB table name: ").strip()

    # Load CSV data into DuckDB
    try:
        db_tool.load_csv_to_table(file_name, table_name, schema)
    except Exception:
        return

    # Query the table
    query_action = input(f"{Fore.CYAN}📊 View table data? (yes/no): ").strip().lower()
    if query_action == 'yes':
        try:
            db_tool.query_table(table_name, schema)
        except Exception:
            return


    print(f"{Fore.GREEN}✅ Migration completed successfully.")

def process_cli_arguments(args):
    """Handle CLI arguments."""

    # Validate required arguments for non-interactive mode
    if not args.db or not args.csv or not args.table:
        print(f"{Fore.RED}❌ Error: '--db', '--csv', and '--table' are required for non-interactive mode.")
        return
    

    db_tool = CSVToDuckDB(args.db)
    db_tool.connect_to_duckdb()

    print(f"{Fore.GREEN}✅ Connected to DuckDB successfully.")

    # Load CSV into DuckDB table
    if args.csv and args.table:
        try:
            db_tool.load_csv_to_table(args.csv, args.table, args.schema)
        except Exception:
            return

    # Query the table if --query is set
    if args.query:
        try:
            db_tool.query_table(args.table, args.schema)
        except Exception:
            return

    print(f"{Fore.GREEN}✅ Migration completed successfully.")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="CSV to DuckDB Tool")
    
    # Command-line arguments
    parser.add_argument('--db', type=str, help="Path to DuckDB DB file (leave blank for in-memory).")
    parser.add_argument('--csv', type=str, help="CSV file path to load into DuckDB.")
    parser.add_argument('--table', type=str, help="DuckDB table name to create.")
    parser.add_argument('--schema', type=str, help="Schema name (optional).")
    parser.add_argument('--query', action='store_true', help="Query table after loading (limit 10 rows).")
    parser.add_argument('--cli', action='store_true', help="Trigger interactive shell mode.")
    
    args = parser.parse_args()

    # Trigger interactive mode if -cli is passed
    if args.cli:
        start_interactive_mode()
        return

    # Process CLI arguments
    process_cli_arguments(args)

if __name__ == "__main__":
    main()
