import os
import duckdb
from colorama import Fore, Style, init

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBManager:
    DATABASE_FOLDER = "databases"

    def __init__(self, duckdb_path=None):
        self.duckdb_conn = None
        self.duckdb_path = duckdb_path
        self.ensure_database_folder()

    @staticmethod
    def ensure_database_folder():
        """Ensure the 'databases' folder exists."""
        if not os.path.exists(DuckDBManager.DATABASE_FOLDER):
            os.makedirs(DuckDBManager.DATABASE_FOLDER)
            print(f"{Fore.GREEN}Created folder: '{DuckDBManager.DATABASE_FOLDER}'")

    def connect_to_duckdb(self):
        """Connect to either an in-memory or file-based DuckDB database."""
        try:
            if self.duckdb_path:
                full_path = os.path.join(self.DATABASE_FOLDER, self.duckdb_path)
                self.duckdb_conn = duckdb.connect(database=full_path)
                print(f"{Fore.GREEN}Connected to DuckDB database file '{full_path}'.")
            else:
                self.duckdb_conn = duckdb.connect(database=':memory:')
                print(f"{Fore.GREEN}Created an in-memory DuckDB database.")
        except Exception as e:
            print(f"{Fore.RED}Failed to create DuckDB database: {e}")
            raise

    def close_duckdb_conn(self):
        """Close DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            print(f"{Fore.GREEN}✅ DuckDB connection closed.")

    @staticmethod
    def list_databases():
        """List DuckDB database files in the 'databases' folder."""
        files = [
            f for f in os.listdir(DuckDBManager.DATABASE_FOLDER) 
            if f.endswith('.duckdb')
        ]
        if files:
            print(f"{Fore.GREEN}Available DuckDB databases in the '{DuckDBManager.DATABASE_FOLDER}' folder:")
            for file in files:
                print(f"{Fore.YELLOW}- {file}")
        else:
            print(f"{Fore.RED}No DuckDB database files found in the '{DuckDBManager.DATABASE_FOLDER}' folder.")


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

    

def main():
    print(f"{Fore.CYAN}Welcome to the DuckDB Manager Tool!")

    # Offer to list existing databases
    print(f"{Fore.CYAN}Would you like to list existing DuckDB files in the '{DuckDBManager.DATABASE_FOLDER}' folder? (yes/no): ", end="")
    list_choice = input().strip().lower()
    if list_choice == 'yes':
        DuckDBManager.list_databases()

    # DuckDB: Choose memory or file-based
    print(f"{Fore.CYAN}Would you like to create an in-memory DuckDB database or a persistent file? (memory/file): ", end="")
    duckdb_choice = input().strip().lower()
    if duckdb_choice == 'file':
        print(f"{Fore.CYAN}Enter the DuckDB file name (existing or new, without path): ", end="")
        duckdb_path = input().strip()
    elif duckdb_choice == 'memory':
        duckdb_path = None
    else:
        print(f"{Fore.RED}Invalid choice. Please choose 'memory' or 'file'.")
        return

    # Initialize DuckDB connection
    db_tool = DuckDBManager(duckdb_path)
    try:
        db_tool.connect_to_duckdb()
    except Exception:
        return

    print(f"{Fore.GREEN}DuckDB database setup complete. Goodbye!")

if __name__ == "__main__":
    main()
