# MamaDuck

**MamaDuck** is a command-line tool designed to facilitate the migration and synchronization of data between DuckDB and other databases like PostgreSQL, SQLite, and CSV. It provides an efficient way to load, export, and transfer data.

---

## Installation

To install **MamaDuck**, use the following `pip` command:

```bash
pip install mamaduck
```

This command will install the latest version of the tool and its dependencies.

---

## Interactive Mode

**MamaDuck** provides an interactive mode to work with migrations directly from the command line. This mode can be triggered by adding the `--cli` flag to any migration tool command.

For example, to run the `load_csv` command in interactive mode, use:

```bash
mamaduck --kwak load_csv --cli
```

In interactive mode, you can interact with the database and execute migration tasks more interactively.

---

## Usage

**MamaDuck** follows this general syntax:

```bash
mamaduck --kwak <tool> [options]
```

Where `<tool>` is one of the following migration tools:

- `load_csv`: Load data from a CSV file into DuckDB.
- `load_psql`: Load data from PostgreSQL into DuckDB.
- `load_sqlite`: Load data from an SQLite database into DuckDB.
- `to_csv`: Export DuckDB tables to CSV.
- `to_psql`: Transfer data from DuckDB to PostgreSQL.
- `to_sqlite`: Transfer data from DuckDB to SQLite.

---

## Available Commands and Arguments

Each command (tool) has its own set of arguments. Below are the arguments for each command.

### 1. `load_csv`: Load Data from CSV into DuckDB

```bash
mamaduck --kwak load_csv --csv <CSV_FILE_PATH> --db <DUCKDB_DB_PATH> --table <TABLE_NAME>
```

Arguments:
- `--db`: Path to DuckDB DB file (leave blank for in-memory).
- `--csv`: Path to the CSV file to load into DuckDB.
- `--table`: DuckDB table name to create.
- `--schema`: Schema name (optional).
- `--cli`: Launch interactive shell mode.

---

### 2. `load_psql`: Load Data from PostgreSQL into DuckDB

```bash
mamaduck --kwak load_psql --psql_conn_string <PSQL_CONNECTION_STRING> --db <DUCKDB_DB_PATH> --schema <SCHEMA_NAME>
```

Arguments:
- `--db`: Path to DuckDB DB file (leave blank for in-memory).
- `--psql_conn_string`: PostgreSQL connection string.
- `--schema`: Schema name to use for migration (optional).
- `--tables`: Comma-separated list of table names to migrate (default: all tables).
- `--cli`: Launch interactive shell mode.

---

### 3. `load_sqlite`: Load Data from SQLite into DuckDB

```bash
mamaduck --kwak load_sqlite --sqlite <SQLITE_DB_PATH> --db <DUCKDB_DB_PATH> --tables <TABLE_NAMES>
```

Arguments:
- `--db`: Path to DuckDB DB file (leave blank for in-memory).
- `--sqlite`: Path to the SQLite database file.
- `--schema`: Schema name to use for migration (optional).
- `--tables`: Comma-separated list of table names to migrate (default: all tables).
- `--cli`: Launch interactive shell mode.

---

### 4. `to_csv`: Export Data from DuckDB to CSV

```bash
mamaduck --kwak to_csv --db <DUCKDB_DB_PATH> --table <TABLE_NAME> --output <CSV_FILE_PATH>
```

Arguments:
- `--db`: Path to DuckDB DB file (leave blank for in-memory).
- `--table`: Table name to export.
- `--schema`: Optional schema for the table.
- `--output`: Output CSV file path.
- `--cli`: Run in interactive mode.

---

### 5. `to_psql`: Transfer Data from DuckDB to PostgreSQL

```bash
mamaduck --kwak to_psql --db <DUCKDB_DB_PATH> --psql <PSQL_CONNECTION_STRING> --table <TABLE_NAME>
```

Arguments:
- `--cli`: Launch interactive mode.
- `--db`: Path to DuckDB DB file (leave blank for in-memory).
- `--psql`: PostgreSQL connection string.
- `--table`: Name of the source table in DuckDB.
- `--output`: Name of the target table in PostgreSQL.

---

### 6. `to_sqlite`: Transfer Data from DuckDB to SQLite

```bash
mamaduck --kwak to_sqlite --db <DUCKDB_DB_PATH> --sqlite <SQLITE_DB_PATH> --table <TABLE_NAME> --newtable <NEW_TABLE_NAME>
```

Arguments:
- `--cli`: Start interactive mode.
- `--db`: Path to DuckDB DB file (leave blank for in-memory).
- `--sqlite`: SQLite database path.
- `--table`: Source table in DuckDB.
- `--newtable`: New table in SQLite.

---

## License

This project is licensed under the MIT License. See the LICENSE file for more information.