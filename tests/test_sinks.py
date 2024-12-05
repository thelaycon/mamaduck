import pytest
from unittest.mock import MagicMock, patch
from mamaduck.sink.to_csv import DuckDBToCSV
from mamaduck.sink.to_psql import DuckDBToPostgreSQL
from mamaduck.sink.to_sqlite import DuckDBToSQLite


# Mock DuckDBManager and DuckDB connection
@pytest.fixture
def mock_duckdb_connection():
    mock_connection = MagicMock()
    return mock_connection


# Test for DuckDBToCSV
def test_export_table_to_csv_success(mock_duckdb_connection):
    # Arrange
    table_name = "test_table"
    output_file = "output.csv"
    db_tool = DuckDBToCSV(db_path=None)
    db_tool.duckdb_conn = mock_duckdb_connection  # Inject the mock connection

    # Act
    db_tool.export_table_to_csv(table_name, output_file)

    # Assert
    db_tool.duckdb_conn.execute.assert_called_once_with(f"COPY {table_name} TO '{output_file}' WITH (HEADER, DELIMITER ',');")


def test_export_table_to_csv_failure(mock_duckdb_connection):
    # Arrange
    table_name = "test_table"
    output_file = "output.csv"
    db_tool = DuckDBToCSV(db_path=None)
    db_tool.duckdb_conn = mock_duckdb_connection  # Inject the mock connection
    db_tool.duckdb_conn.execute.side_effect = Exception("Database error")

    # Act & Assert
    with pytest.raises(Exception):
        db_tool.export_table_to_csv(table_name, output_file)


# Test for DuckDBToPostgreSQL
def test_attach_postgresql_success(mock_duckdb_connection):
    # Arrange
    psql_conn_string = "dbname=test user=test password=test host=localhost"
    db_tool = DuckDBToPostgreSQL(db_path=None, psql_conn_string=psql_conn_string)
    db_tool.duckdb_conn = mock_duckdb_connection

    # Act
    db_tool.attach_postgresql()

    # Assert
    db_tool.duckdb_conn.execute.assert_called_once_with(f"ATTACH '{psql_conn_string}' AS postgres_db (TYPE POSTGRES);")


def test_attach_postgresql_failure(mock_duckdb_connection):
    # Arrange
    psql_conn_string = "dbname=test user=test password=test host=localhost"
    db_tool = DuckDBToPostgreSQL(db_path=None, psql_conn_string=psql_conn_string)
    db_tool.duckdb_conn = mock_duckdb_connection
    db_tool.duckdb_conn.execute.side_effect = Exception("Failed to attach PostgreSQL")

    # Act & Assert
    with pytest.raises(Exception):
        db_tool.attach_postgresql()


def test_transfer_data_to_psql_success(mock_duckdb_connection):
    # Arrange
    source_table_name = "test_source_table"
    psql_table_name = "test_psql_table"
    db_tool = DuckDBToPostgreSQL(db_path=None, psql_conn_string="fake_psql_conn")
    db_tool.duckdb_conn = mock_duckdb_connection
    db_tool.duckdb_conn.execute.return_value.fetchall.return_value = [(1, 'test')]

    # Act
    db_tool.transfer_data_to_psql(source_table_name, psql_table_name)

    # Assert
    db_tool.duckdb_conn.executemany.assert_called_once()


# Test for DuckDBToSQLite
def test_attach_sqlite_database_success(mock_duckdb_connection):
    # Arrange
    sqlite_db_path = "sqlite_db_path"
    db_tool = DuckDBToSQLite(db_path=None, sqlite_db_path=sqlite_db_path)
    db_tool.duckdb_conn = mock_duckdb_connection

    # Act
    db_tool.attach_sqlite_database()

    # Assert
    db_tool.duckdb_conn.execute.assert_called_once_with(f"ATTACH '{sqlite_db_path}' AS sqlite_db (TYPE SQLITE);")


def test_attach_sqlite_database_failure(mock_duckdb_connection):
    # Arrange
    sqlite_db_path = "sqlite_db_path"
    db_tool = DuckDBToSQLite(db_path=None, sqlite_db_path=sqlite_db_path)
    db_tool.duckdb_conn = mock_duckdb_connection
    db_tool.duckdb_conn.execute.side_effect = Exception("Failed to attach SQLite")

    # Act & Assert
    with pytest.raises(Exception):
        db_tool.attach_sqlite_database()


def test_transfer_data_to_sqlite_success(mock_duckdb_connection):
    # Arrange
    source_table_name = "test_source_table"
    sqlite_table_name = "test_sqlite_table"
    db_tool = DuckDBToSQLite(db_path=None, sqlite_db_path="fake_sqlite_path")
    db_tool.duckdb_conn = mock_duckdb_connection
    db_tool.duckdb_conn.execute.return_value.fetchall.return_value = [(1, 'test')]

    # Act
    db_tool.transfer_data_to_sqlite(source_table_name, sqlite_table_name)

    # Assert
    db_tool.duckdb_conn.executemany.assert_called_once()


# Test for DuckDBToSQLite preview
def test_preview_sqlite_data_success(mock_duckdb_connection):
    # Arrange
    sqlite_table_name = "test_sqlite_table"
    db_tool = DuckDBToSQLite(db_path=None, sqlite_db_path="fake_sqlite_path")
    db_tool.duckdb_conn = mock_duckdb_connection
    db_tool.duckdb_conn.execute.return_value.fetchall.return_value = [(1, 'test')]

    # Act
    db_tool.preview_sqlite_data(sqlite_table_name)

    # Assert
    db_tool.duckdb_conn.execute.assert_called_once_with(f"SELECT * FROM sqlite_db.{sqlite_table_name} LIMIT 10;")


def test_preview_sqlite_data_failure(mock_duckdb_connection):
    # Arrange
    sqlite_table_name = "test_sqlite_table"
    db_tool = DuckDBToSQLite(db_path=None, sqlite_db_path="fake_sqlite_path")
    db_tool.duckdb_conn = mock_duckdb_connection
    db_tool.duckdb_conn.execute.side_effect = Exception("Failed to retrieve data")

    # Act & Assert
    with pytest.raises(Exception):
        db_tool.preview_sqlite_data(sqlite_table_name)

