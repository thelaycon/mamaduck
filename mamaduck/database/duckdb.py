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

    def get_schema_list(self):
        schemas = self.duckdb_conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
        schemas = set([s[0] for s in schemas])
        return list(schemas)
    

    def get_table_list(self, schema=None):
        """Get the list of tables in the specified schema or all schemas."""
        try:
            query = "SELECT table_schema, table_name FROM information_schema.tables"
            if schema:
                query += f" WHERE table_schema = '{schema}'"
            query += ";"

            tables = self.duckdb_conn.execute(query).fetchall()
            table_list = [f"{row[0]}.{row[1]}" if schema is None else row[1] for row in tables]
            
            print(f"{Fore.GREEN}✅ Found {len(table_list)} tables in schema '{schema or 'all'}'.")
            return table_list
        except Exception as e:
            print(f"{Fore.RED}❌ Error fetching tables: {e}")
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
