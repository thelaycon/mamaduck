import argparse
import duckdb
import os
from colorama import Fore, init

from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToSQLite(DuckDBManager):

    def __init__(self, db_path, sqlite_db_path):
        super().__init__(db_path)
        self.sqlite_db_path = sqlite_db_path

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

def interactive_mode():
    """Interactive mode to transfer data from DuckDB to SQLite."""
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
    
    sqlite_db_path = input(f"{Fore.CYAN}Enter SQLite database path: ").strip()
    source_table_name = input(f"{Fore.CYAN}Enter DuckDB source table name: ").strip()
    sqlite_table_name = input(f"{Fore.CYAN}Enter new SQLite table name: ").strip()

    db_tool = DuckDBToSQLite(db_path, sqlite_db_path)
    db_tool.connect_to_duckdb()
    db_tool.attach_sqlite_database()

    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_sqlite(sqlite_table_name, column_definitions)
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    db_tool.close_duckdb_conn()
    print(f"{Fore.GREEN}✅ Export completed.")

def main():
    """Main function to handle CLI arguments."""
    parser = argparse.ArgumentParser(description="Transfer data from DuckDB to SQLite.")
    parser.add_argument("--cli", action="store_true", help="Start interactive mode")
    parser.add_argument("--db", help="Path to DuckDB DB file (leave blank for in-memory).")
    parser.add_argument("--sqlite", help="SQLite database path.")
    parser.add_argument("--table", help="Source table in DuckDB.")
    parser.add_argument("--newtable", help="New table in SQLite.")
    args = parser.parse_args()

    if args.cli:
        interactive_mode()
        return

    if not (args.db and args.sqlite and args.table and args.newtable):
        print(f"{Fore.RED}❌ Missing arguments: --db, --sqlite, --table, and --newtable are required.")
        return
    
    db_path = args.db
    sqlite_db_path = args.sqlite
    source_table_name = args.table
    sqlite_table_name = args.newtable

    db_tool = DuckDBToSQLite(db_path, sqlite_db_path)
    db_tool.connect_to_duckdb()
    db_tool.attach_sqlite_database()

    column_definitions = db_tool.get_table_columns(source_table_name)
    db_tool.create_table_in_sqlite(sqlite_table_name, column_definitions)
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    db_tool.close_duckdb_conn()
    print(f"{Fore.GREEN}✅ Export completed.")


if __name__ == "__main__":
    main()
