# MamaDuck

**MamaDuck** is a Python package that acts as a data warehouse for syncing data to and from DuckDB. It provides tools for migrating and exporting data between various data sources and DuckDB, functioning as an intermediary for storing and transferring large datasets efficiently.

## Features

- **Data Warehouse Functionality**:
  - Sync data from external sources (CSV, PostgreSQL, SQLite) to DuckDB, using DuckDB as a central data warehouse.
  - Export data from DuckDB to external formats (CSV, PostgreSQL, SQLite) for downstream processing.

- **Efficient Data Migration**:
  - Sync and export data quickly and reliably, making DuckDB a powerful central storage solution for diverse data sources.

## Usage

Once integrated into your project, **MamaDuck** allows you to sync data from CSV, PostgreSQL, or SQLite to DuckDB and export it from DuckDB to various formats. The package provides tools for seamless integration with your data workflow.

## Command-Line Interface (CLI)

**MamaDuck** also provides a command-line interface (CLI) to allow easy execution of the syncing and exporting tools.

## Available Tools

- `csv`: Sync data from CSV files to DuckDB warehouse.
- `psql`: Sync data from PostgreSQL databases to DuckDB warehouse.
- `sqlite`: Sync data from SQLite databases to DuckDB warehouse.
- `to_csv`: Export data from DuckDB warehouse to CSV files.
- `to_psql`: Export data from DuckDB warehouse to PostgreSQL databases.
- `to_sqlite`: Export data from DuckDB warehouse to SQLite databases.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

For further assistance or to report an issue, please refer to the [GitHub repository](https://github.com/thelaycon/mamaduck).