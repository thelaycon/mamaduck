import duckdb
import os
from colorama import Fore, Style, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class CSVToDuckDB:
    def __init__(self, db_path=None):
        self.connection = None
        self.db_path = db_path

    def connect_to_duckdb(self):
        """Connect to either an in-memory or file-based DuckDB database."""
        if self.db_path:
            self.connection = duckdb.connect(database=self.db_path)
            print(f"{Fore.GREEN}Connected to DuckDB database file '{self.db_path}'.")
        else:
            self.connection = duckdb.connect(database=':memory:')
            print(f"{Fore.GREEN}Connected to an in-memory DuckDB database.")

    def load_csv_to_table(self, file_name, table_name):
        """Load CSV data into a DuckDB table."""
        try:
            print(f"{Fore.BLUE}Loading CSV file '{file_name}' into table '{table_name}'...")
            self.connection.execute(f"CREATE TABLE {table_name} AS FROM '{file_name}';")
            print(f"{Fore.GREEN}CSV data successfully loaded into table '{table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to load CSV into database: {e}")
            raise

    def query_table(self, table_name):
        """Query data from the specified DuckDB table."""
        try:
            print(f"{Fore.BLUE}Fetching data from table '{table_name}'...")
            result = self.connection.execute(f"SELECT * FROM {table_name} LIMIT 10;").fetchall()
            columns = [desc[0] for desc in self.connection.execute(f"PRAGMA table_info('{table_name}');").fetchall()]
            print(f"{Fore.GREEN}Showing up to 10 records from table '{table_name}':")
            print(f"{Fore.CYAN}{columns}")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}Failed to query table: {e}")
            raise

    def export_table_to_csv(self, table_name, output_file):
        """Export the contents of a DuckDB table to a CSV file."""
        try:
            print(f"{Fore.BLUE}Exporting table '{table_name}' to file '{output_file}'...")
            self.connection.execute(f"COPY {table_name} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Table '{table_name}' successfully exported to '{output_file}'.")
        except Exception as e:
            print(f"{Fore.RED}Failed to export table to CSV: {e}")
            raise

def main():
    print(f"{Fore.CYAN}Welcome to the CSV to DuckDB Tool!")

    # Ask user whether to use an in-memory database or a persistent file
    print(f"{Fore.CYAN}Would you like to use an in-memory database or a persistent file? (memory/file): ", end="")
    db_choice = input().strip().lower()
    if db_choice == 'file':
        print(f"{Fore.CYAN}Enter the DuckDB file name (existing or new): ", end="")
        db_path = input().strip()
    elif db_choice == 'memory':
        db_path = None
    else:
        print(f"{Fore.RED}Invalid choice. Please choose 'memory' or 'file'.")
        return

    # Initialize and connect to DuckDB
    db_tool = CSVToDuckDB(db_path)
    db_tool.connect_to_duckdb()

    # Prompt for CSV file name
    print(f"{Fore.CYAN}Enter the CSV file name (with path if needed): ", end="")
    file_name = input().strip()
    if not os.path.exists(file_name):
        print(f"{Fore.RED}Error: File '{file_name}' does not exist.")
        return

    # Prompt for table name
    print(f"{Fore.CYAN}Enter the name of the table to create in DuckDB: ", end="")
    table_name = input().strip()

    # Load CSV data into DuckDB table
    try:
        db_tool.load_csv_to_table(file_name, table_name)
    except Exception:
        return

    # Query the table to verify contents
    print(f"{Fore.CYAN}Would you like to view the table data? (yes/no): ", end="")
    query_action = input().strip().lower()
    if query_action == 'yes':
        try:
            db_tool.query_table(table_name)
        except Exception:
            return

    # Export the table to a CSV file
    print(f"{Fore.CYAN}Would you like to export the table to a CSV file? (yes/no): ", end="")
    export_action = input().strip().lower()
    if export_action == 'yes':
        print(f"{Fore.CYAN}Enter the output file name (with path if needed): ", end="")
        output_file = input().strip()
        try:
            db_tool.export_table_to_csv(table_name, output_file)
        except Exception:
            return

    print(f"{Fore.GREEN}Process completed. Goodbye!")

if __name__ == "__main__":
    main()
