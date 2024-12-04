import argparse
import duckdb
import os
from colorama import Fore, init

from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToSQLite(DuckDBManager):
    DATABASE_FOLDER = "databases"

    def __init__(self, db_path, sqlite_db_path):
        super().__init__()
        self.duckdb_conn = None
        self.db_path = db_path
        self.sqlite_db_path = sqlite_db_path

    def connect_to_duckdb(self):
        """Connect to DuckDB database."""
        try:
            self.duckdb_conn = duckdb.connect(self.db_path)
            print(f"{Fore.GREEN}✅ Connected to DuckDB database '{self.db_path}'.")
        except Exception as e:
            print(f"{Fore.RED}❌ Connection failed: {e}")
            raise

    def attach_sqlite_database(self):
        """Attach SQLite database."""
        try:
            self.duckdb_conn.execute(f"ATTACH '{self.sqlite_db_path}' AS sqlite_db (TYPE SQLITE);")
            print(f"{Fore.GREEN}✅ Attached SQLite database '{self.sqlite_db_path}'.")
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to attach SQLite: {e}")
            raise

    def get_table_columns(self, table_name):
        """Retrieve columns of a table in DuckDB."""
        try:
            columns = self.duckdb_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return [f"{column[1]} {column[2]}" for column in columns]
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to retrieve columns: {e}")
            raise

    def create_table_in_sqlite(self, table_name, column_definitions):
        """Create table in SQLite."""
        try:
            create_query = f"CREATE TABLE IF NOT EXISTS sqlite_db.{table_name} ({', '.join(column_definitions)});"
            self.duckdb_conn.execute(create_query)
            print(f"{Fore.GREEN}✅ Table '{table_name}' created in SQLite.")
        except Exception as e:
            print(f"{Fore.RED}❌ Table creation failed: {e}")
            raise

    def transfer_data_to_sqlite(self, source_table_name, sqlite_table_name):
        """Transfer data from DuckDB to SQLite."""
        try:
            data = self.duckdb_conn.execute(f"SELECT * FROM {source_table_name}").fetchall()
            insert_query = f"INSERT INTO sqlite_db.{sqlite_table_name} VALUES ({', '.join(['?' for _ in data[0]])})"
            self.duckdb_conn.executemany(insert_query, data)
            print(f"{Fore.GREEN}✅ Data transferred from '{source_table_name}' to SQLite '{sqlite_table_name}'.")
        except Exception as e:
            print(f"{Fore.RED}❌ Data transfer failed: {e}")
            raise

    def preview_sqlite_data(self, sqlite_table_name, num_records=10):
        """Preview data from SQLite table."""
        try:
            result = self.duckdb_conn.execute(f"SELECT * FROM sqlite_db.{sqlite_table_name} LIMIT {num_records};").fetchall()
            print(f"{Fore.CYAN}🔍 Previewing {num_records} records from '{sqlite_table_name}':")
            for row in result:
                print(f"{Fore.YELLOW}{row}")
        except Exception as e:
            print(f"{Fore.RED}❌ Failed to preview data: {e}")
            raise

    def close_duckdb_conn(self):
        """Close DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
            print(f"{Fore.GREEN}✅ DuckDB connection closed.")

def interactive_mode():
    """Interactive mode to transfer data from DuckDB to SQLite."""
    print(f"{Fore.CYAN}🔄 Running in interactive mode...")

    use_memory = input(f"{Fore.CYAN}Use in-memory DuckDB? (yes/no): ").strip().lower() == 'yes'
    
    db_path = ":memory:" if use_memory else input(f"{Fore.CYAN}Enter DuckDB database path: ").strip()
    db_path = os.path.join(DuckDBToSQLite.DATABASE_FOLDER, db_path) if db_path else None
    
    sqlite_db_path = input(f"{Fore.CYAN}Enter SQLite database path: ").strip()
    source_table_name = input(f"{Fore.CYAN}Enter DuckDB source table name: ").strip()
    sqlite_table_name = input(f"{Fore.CYAN}Enter new SQLite table name: ").strip()
    preview = input(f"{Fore.CYAN}Preview data in SQLite? (yes/no): ").strip().lower() == 'yes'
    records = int(input(f"{Fore.CYAN}Enter number of records to preview: ")) if preview else 10

    db_tool = DuckDBToSQLite(db_path, sqlite_db_path)
    db_tool.connect_to_duckdb()
    db_tool.attach_sqlite_database()

    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_sqlite(sqlite_table_name, column_definitions)
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    if preview:
        db_tool.preview_sqlite_data(sqlite_table_name, records)

    db_tool.close_duckdb_conn()

def main():
    """Main function to handle CLI arguments."""
    parser = argparse.ArgumentParser(description="Transfer data from DuckDB to SQLite.")
    parser.add_argument("--cli", action="store_true", help="Start interactive mode")
    parser.add_argument("--db", help="DuckDB database path or ':memory:' for in-memory.")
    parser.add_argument("--sqlite", help="SQLite database path.")
    parser.add_argument("--table", help="Source table in DuckDB.")
    parser.add_argument("--newtable", help="New table in SQLite.")
    parser.add_argument("--preview", action="store_true", help="Preview data in SQLite after transfer.")
    parser.add_argument("--records", type=int, default=10, help="Number of records to preview (default: 10).")
    args = parser.parse_args()

    if args.cli:
        interactive_mode()
        return

    if not (args.db and args.sqlite and args.table and args.newtable):
        print(f"{Fore.RED}❌ Missing arguments: --db, --sqlite, --table, and --newtable are required.")
        return
    
    db_path = args.db if args.db != ":memory:" else ":memory:"
    sqlite_db_path = args.sqlite
    source_table_name = args.table
    sqlite_table_name = args.newtable
    preview = args.preview
    records = args.records if args.preview else 10

    db_tool = DuckDBToSQLite(db_path, sqlite_db_path)
    db_tool.connect_to_duckdb()
    db_tool.attach_sqlite_database()

    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_sqlite(sqlite_table_name, column_definitions)
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    if preview:
        db_tool.preview_sqlite_data(sqlite_table_name, records)

    db_tool.close_duckdb_conn()

if __name__ == "__main__":
    main()
