from unittest.mock import Mock

import pytest

from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.exceptions import CustomSQLErrorException
from datahub_executor.common.source.source import Source


class TestSourceSqlValidator:
    def setup_method(self) -> None:
        # Source is not an abstract class; we can instantiate it directly for validation tests.
        self.connection = Mock(spec=Connection)
        self.source = Source(self.connection)

    def test_validate_custom_sql_valid(self) -> None:
        # Should NOT raise: keywords appear in identifiers, not as statements
        self.source._validate_custom_sql(
            "SELECT * FROM database.insert-is-my-schema-name.delete-is-my-table-name"
        )

    def test_validate_custom_sql_insert_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql(
                "SELECT * FROM my_table; INSERT INTO my_table id='bad data'"
            )

    def test_validate_custom_sql_insert_statement_lowercase(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql(
                "SELECT * FROM my_table; insert into my_table id='bad data'"
            )

    def test_validate_custom_sql_insert_statement_mixed_case(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql(
                "SELECT * FROM my_table; InSeRt InTO my_table id='bad data'"
            )

    def test_validate_custom_sql_update_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql(
                "SELECT * FROM my_table; UPDATE my_table set id='bad data'"
            )

    def test_validate_custom_sql_delete_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql(
                "delete FROM my_table; SELECT * FROM my_table"
            )

    def test_validate_custom_sql_create_table_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql("CREATE TABLE my_new_table (ID int)")

    def test_validate_custom_sql_alter_table_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql(
                "alter TABLE my_new_table add my_new_column varchar(255)"
            )

    def test_validate_custom_sql_drop_table_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql("DROP TABLE my_new_table")

    def test_validate_custom_sql_create_database_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql("create DATABASE my_new_database")

    def test_validate_custom_sql_drop_database_statement(self) -> None:
        with pytest.raises(CustomSQLErrorException):
            self.source._validate_custom_sql("DROP DATABASE my_new_database")
