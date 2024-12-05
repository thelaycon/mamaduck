import os
import textwrap

import pytest
from unittest.mock import MagicMock, patch, call

from mamaduck.database.duckdb import DuckDBManager
from mamaduck.connectors.psql import PostgreSQLToDuckDB
from mamaduck.connectors.sqlite import SQLiteToDuckDB
from mamaduck.connectors.csv import CSVToDuckDB


def single_space(text):
    """Reduce multiple spaces and newlines to a single space."""
    return " ".join(text.split())


@pytest.fixture
def mock_duckdb_manager():
    """Fixture to create a mock DuckDBManager."""
    manager = DuckDBManager(":memory:")
    manager.duckdb_conn = MagicMock()
    return manager


# CSVToDuckDB Tests
def test_load_csv_to_table(mock_duckdb_manager):
    csv_tool = CSVToDuckDB(":memory:")
    csv_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn

    file_name = "test.csv"
    table_name = "test_table"
    schema = "test_schema"

    # Simulating the existence of the file
    with patch("os.path.exists", return_value=True):
        csv_tool.load_csv_to_table(file_name, table_name, schema)

    # Verify schema and table creation
    mock_duckdb_manager.duckdb_conn.execute.assert_has_calls([
        call(f"CREATE SCHEMA IF NOT EXISTS {schema};"),
        call(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_csv_auto('{file_name}');")
    ])

# PostgreSQLToDuckDB Tests
def test_attach_postgresql(mock_duckdb_manager):
    psql_tool = PostgreSQLToDuckDB(":memory:", "mock_conn_string")
    psql_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn

    psql_tool.attach_postgresql()

    mock_duckdb_manager.duckdb_conn.execute.assert_called_once_with(
        "ATTACH 'mock_conn_string' AS postgres_db (TYPE POSTGRES);"
    )


def test_list_postgresql_tables(mock_duckdb_manager):
    psql_tool = PostgreSQLToDuckDB(":memory:", "mock_conn_string")
    psql_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn
    mock_duckdb_manager.duckdb_conn.execute.return_value.fetchall.return_value = [
        ("table1",), ("table2",)
    ]

    tables = psql_tool.list_postgresql_tables()

    assert tables == ["table1", "table2"]
    mock_duckdb_manager.duckdb_conn.execute.assert_called_once_with(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    )


def test_migrate_postgresql_table(mock_duckdb_manager):
    psql_tool = PostgreSQLToDuckDB(":memory:", "mock_conn_string")
    psql_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn

    psql_tool.migrate_table("psql_table", "duckdb_table")

    expected_sql = textwrap.dedent("""
        CREATE TABLE duckdb_table AS 
        SELECT * FROM postgres_db.psql_table;
    """).strip()

    # Retrieve the actual SQL executed
    actual_sql = mock_duckdb_manager.duckdb_conn.execute.call_args[0][0].strip()

    assert single_space(actual_sql) == single_space(expected_sql)


# SQLiteToDuckDB Tests
def test_load_sqlite_extension(mock_duckdb_manager):
    sqlite_tool = SQLiteToDuckDB(":memory:")
    sqlite_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn

    sqlite_tool.load_sqlite_extension()

    mock_duckdb_manager.duckdb_conn.execute.assert_has_calls([
        call("INSTALL sqlite;"),
        call("LOAD sqlite;")
    ])


def test_list_sqlite_tables(mock_duckdb_manager):
    sqlite_tool = SQLiteToDuckDB(":memory:")
    sqlite_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn
    mock_duckdb_manager.duckdb_conn.execute.return_value.fetchall.return_value = [
        ("table1",), ("table2",)
    ]

    tables = sqlite_tool.list_sqlite_tables("mock_sqlite.db")

    assert tables == ["table1", "table2"]

    expected_sql = "SELECT name FROM sqlite_scan('mock_sqlite.db', 'sqlite_master') WHERE type = 'table';"
    actual_sql = mock_duckdb_manager.duckdb_conn.execute.call_args[0][0].strip()

    assert single_space(actual_sql) == single_space(expected_sql)


def test_migrate_sqlite_table(mock_duckdb_manager):
    sqlite_tool = SQLiteToDuckDB(":memory:")
    sqlite_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn

    sqlite_tool.migrate_table("mock_sqlite.db", "sqlite_table", "duckdb_table")

    expected_sql = textwrap.dedent("""
        CREATE TABLE duckdb_table AS 
        SELECT * FROM sqlite_scan('mock_sqlite.db', 'sqlite_table');
    """).strip()

    # Retrieve the actual SQL executed
    actual_sql = mock_duckdb_manager.duckdb_conn.execute.call_args[0][0].strip()

    assert single_space(actual_sql) == single_space(expected_sql)


def test_migrate_sqlite_table_invalid_table(mock_duckdb_manager):
    sqlite_tool = SQLiteToDuckDB(":memory:")
    sqlite_tool.duckdb_conn = mock_duckdb_manager.duckdb_conn

    mock_duckdb_manager.duckdb_conn.execute.side_effect = ValueError("Table does not exist")

    with pytest.raises(ValueError, match="Table does not exist"):
        sqlite_tool.migrate_table("mock_sqlite.db", "nonexistent_table", "duckdb_table")
