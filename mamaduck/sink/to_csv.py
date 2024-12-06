import argparse
import duckdb
import os
from colorama import Fore, Style, init
from mamaduck.database.duckdb import DuckDBManager

# Initialize colorama for colored CLI output
init(autoreset=True)

class DuckDBToCSV(DuckDBManager):

    def __init__(self, db_path):
        super().__init__(db_path)

    def export_table_to_csv(self, table_name, output_file, schema=None):
        """Export DuckDB table to CSV."""
        try:
            table = f"{schema}.{table_name}" if schema else table_name
            print(f"{Fore.BLUE}Exporting '{table}' to '{output_file}'... 📊")
            self.duckdb_conn.execute(f"COPY {table} TO '{output_file}' WITH (HEADER, DELIMITER ',');")
            print(f"{Fore.GREEN}Exported successfully to {output_file} ✅")
        except Exception as e:
            print(f"{Fore.RED}Export failed: {e} ❌")
            raise

def interactive_mode():
    """Interactive session for DuckDB to CSV export."""
    print(f"{Fore.CYAN}🦆 MamaDuck")

    # Choose database type (in-memory or file)
    db_choice = input(f"{Fore.CYAN}💡 Use in-memory or persistent file DB? (memory/file): ").strip().lower()
    if db_choice == 'file':
        db_path = input(f"{Fore.CYAN}🔑 Enter DuckDB file name (existing/new): ").strip()
    elif db_choice == 'memory':
        db_path = None
    else:
        print(f"{Fore.RED}❌ Invalid choice. Choose 'memory' or 'file'.")
        return

    # Connect to DuckDB
    db_tool = DuckDBToCSV(db_path)
    db_tool.connect_to_duckdb()

    # Schema and table inputs
    schema = input(f"{Fore.CYAN}Enter schema (optional): ").strip() or None
    table_name = input(f"{Fore.CYAN}🗃 Enter the DuckDB table to transfer: ").strip()
    output_file = input(f"{Fore.CYAN}🗂 Enter output CSV file: ").strip()

    # Export the table to CSV
    try:
        db_tool.export_table_to_csv(table_name, output_file, schema)
    except Exception:
        return
    
    db_tool.close_duckdb_conn()
    print(f"{Fore.GREEN}✅ Export completed.")

def main():
    """Main entry point for DuckDB to CSV export."""
    parser = argparse.ArgumentParser(description="Export DuckDB tables to CSV.")
    parser.add_argument('--db', type=str, help="Path to DuckDB DB file (leave blank for in-memory).")
    parser.add_argument('--table', type=str, help="Table name to export.")
    parser.add_argument('--schema', type=str, help="Optional schema for the table.")
    parser.add_argument('--output', type=str, help="Output CSV file path.")
    parser.add_argument('--cli', action='store_true', help="Run in interactive mode.")

    args = parser.parse_args()

    # Interactive mode
    if args.cli:
        interactive_mode()
        return

    # Non-interactive mode validation
    if not args.table or not args.output:
        print(f"{Fore.RED}Error: '--table' and '--output' are required. ⚠️")
        return

    # Default to in-memory if no database path
    db_path = args.db

    # Connect to DuckDB and export table to CSV
    db_tool = DuckDBToCSV(db_path)
    try:
        db_tool.connect_to_duckdb()
    except Exception:
        return

    try:
        db_tool.export_table_to_csv(args.table, args.output, args.schema)
    except Exception:
        return
    
    db_tool.close_duckdb_conn()
    print(f"{Fore.GREEN}✅ Export completed.")

if __name__ == "__main__":
    main()
