import duckdb
import os
from colorama import Fore, Style, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToCSV:
    def __init__(self, db_path=None):
        self.connection = None
        self.db_path = db_path

    def connect_to_duckdb(self):
        """Connect to either an in-memory or file-based DuckDB database."""
        try:
            if self.db_path:
                full_path = os.path.join('databases', self.db_path)
                self.connection = duckdb.connect(database=full_path)
                print(f"{Fore.GREEN}Connected to DuckDB database file '{full_path}'.")
            else:
                self.connection = duckdb.connect(database=':memory:')
                print(f"{Fore.GREEN}Connected to an in-memory DuckDB database.")
        except Exception as e:
            print(f"{Fore.RED}Failed to connect to DuckDB database: {e}")
            raise

    def export_table_to_csv(self, table_name, output_file, schema=None):
        """Export the contents of a DuckDB table to a CSV file."""
        try:
            table = f"{schema}.{table_name}" if schema else table_name
            print(f"{Fore.BLUE}Exporting table '{table}' to file '{output_file}'...")
            self.connection.execute(f"COPY {table} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Table '{table}' successfully exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to export table to CSV: {e}")
            raise

def main():
    print(f"{Fore.CYAN}Welcome to the DuckDB to CSV Export Tool!")

    # Ask user whether to use an in-memory database or a persistent file
    print(f"{Fore.CYAN}Would you like to use an in-memory database or a persistent file? (memory/file): ", end="")
    db_choice = input().strip().lower()
    if db_choice == 'file':
        print(f"{Fore.CYAN}Enter the DuckDB file name (existing or new, without path): ", end="")
        db_path = input().strip()
    elif db_choice == 'memory':
        db_path = None
    else:
        print(f"{Fore.RED}Invalid choice. Please choose 'memory' or 'file'.")
        return

    # Initialize and connect to DuckDB
    db_tool = DuckDBToCSV(db_path)
    db_tool.connect_to_duckdb()

    # Prompt for schema (optional)
    print(f"{Fore.CYAN}Enter the schema name (leave empty for none): ", end="")
    schema = input().strip() or None

    # Prompt for table name
    print(f"{Fore.CYAN}Enter the name of the table to export from DuckDB: ", end="")
    table_name = input().strip()

    # Prompt for output CSV file name
    print(f"{Fore.CYAN}Enter the output CSV file name (with path if needed): ", end="")
    output_file = input().strip()

    # Export the table to CSV
    try:
        db_tool.export_table_to_csv(table_name, output_file, schema)
    except Exception:
        return

    print(f"{Fore.GREEN}Process completed. Goodbye!")

if __name__ == "__main__":
    main()
