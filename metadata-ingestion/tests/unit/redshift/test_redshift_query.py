from datetime import datetime
from typing import Type

import pytest
import sqlglot
from sqlglot.dialects.redshift import Redshift

from datahub.ingestion.source.redshift.query import (
    RedshiftCommonQuery,
    RedshiftProvisionedQuery,
    RedshiftServerlessQuery,
)


class TestRedshiftCommonQuery:
    """Test RedshiftCommonQuery methods."""

    @pytest.mark.parametrize(
        "alternative_schema,table_name,expected",
        [
            (None, "svv_datashares", "svv_datashares"),
            (
                "custom_db.custom_schema",
                "svv_datashares",
                "custom_db.custom_schema.svv_datashares",
            ),
            (
                "test.schema",
                "svv_redshift_databases",
                "test.schema.svv_redshift_databases",
            ),
        ],
    )
    def test_get_system_table_name(self, alternative_schema, table_name, expected):
        """Test system table name generation with and without alternative schema."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)
        assert query._get_system_table_name(table_name=table_name) == expected

    @pytest.mark.parametrize("alternative_schema", [None, "custom_db.custom_schema"])
    def test_list_databases_query_compiles(self, alternative_schema):
        """Test that list_databases query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)
        sql = query.list_databases()

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    @pytest.mark.parametrize(
        "alternative_schema,database",
        [
            (None, "test_db"),
            ("custom_db.custom_schema", "test_db"),
        ],
    )
    def test_get_database_details_query_compiles(self, alternative_schema, database):
        """Test that get_database_details query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)
        sql = query.get_database_details(database=database)

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    @pytest.mark.parametrize(
        "alternative_schema,database",
        [
            (None, "test_db"),
            ("custom_db.custom_schema", "test_db"),
        ],
    )
    def test_list_schemas_query_compiles(self, alternative_schema, database):
        """Test that list_schemas query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)
        sql = query.list_schemas(database=database)

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    @pytest.mark.parametrize(
        "alternative_schema,database,skip_external,is_shared",
        [
            (None, "test_db", False, False),
            ("custom_db.custom_schema", "test_db", False, False),
            (None, "test_db", True, False),
            (None, "test_db", False, True),
            ("custom_db.custom_schema", "test_db", False, True),
        ],
    )
    def test_list_tables_query_compiles(
        self, alternative_schema, database, skip_external, is_shared
    ):
        """Test that list_tables query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)
        sql = query.list_tables(
            database=database,
            skip_external_tables=skip_external,
            is_shared_database=is_shared,
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    @pytest.mark.parametrize(
        "alternative_schema,database,schema,is_shared",
        [
            (None, "test_db", "public", False),
            ("custom_db.custom_schema", "test_db", "public", False),
            (None, "test_db", "analytics", True),
            ("custom_db.custom_schema", "test_db", "analytics", True),
        ],
    )
    def test_list_columns_query_compiles(
        self, alternative_schema, database, schema, is_shared
    ):
        """Test that list_columns query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)
        sql = query.list_columns(
            database_name=database, schema_name=schema, is_shared_database=is_shared
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    @pytest.mark.parametrize("alternative_schema", [None, "custom_db.custom_schema"])
    def test_datashare_queries_compile(self, alternative_schema):
        """Test that datashare queries compile correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=alternative_schema)

        # Test outbound datashares
        sql = query.list_outbound_datashares()
        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

        # Test inbound datashares
        sql = query.get_inbound_datashare(database="test_db")
        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_view_lineage_query_compiles(self):
        """Test that view_lineage_query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=None)
        sql = query.view_lineage_query()

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_list_late_view_ddls_query_compiles(self):
        """Test that list_late_view_ddls_query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=None)
        sql = query.list_late_view_ddls_query()

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_alter_table_rename_query_compiles(self):
        """Test that alter_table_rename_query compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=None)
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 2, 12, 0, 0)

        sql = query.alter_table_rename_query(
            db_name="test_db", start_time=start_time, end_time=end_time
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_list_copy_commands_sql_compiles(self):
        """Test that list_copy_commands_sql compiles correctly."""
        query = RedshiftCommonQuery(alternative_system_tables_schema=None)
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 2, 12, 0, 0)

        sql = query.list_copy_commands_sql(
            db_name="test_db", start_time=start_time, end_time=end_time
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None


@pytest.mark.parametrize(
    "query_class", [RedshiftProvisionedQuery, RedshiftServerlessQuery]
)
class TestRedshiftQueryClasses:
    """Test both RedshiftProvisionedQuery and RedshiftServerlessQuery classes."""

    def test_additional_table_metadata_query_compiles(
        self, query_class: Type[RedshiftCommonQuery]
    ) -> None:
        """Test that additional_table_metadata_query compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        sql = query.additional_table_metadata_query()

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_stl_scan_based_lineage_query_compiles(
        self, query_class: Type[RedshiftCommonQuery]
    ) -> None:
        """Test that stl_scan_based_lineage_query compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 2, 12, 0, 0)

        sql = query.stl_scan_based_lineage_query(
            db_name="test_db", start_time=start_time, end_time=end_time
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_list_unload_commands_sql_compiles(
        self, query_class: Type[RedshiftCommonQuery]
    ) -> None:
        """Test that list_unload_commands_sql compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 2, 12, 0, 0)

        sql = query.list_unload_commands_sql(
            db_name="test_db", start_time=start_time, end_time=end_time
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_list_insert_create_queries_sql_compiles(
        self, query_class: Type[RedshiftCommonQuery]
    ) -> None:
        """Test that list_insert_create_queries_sql compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 2, 12, 0, 0)

        sql = query.list_insert_create_queries_sql(
            db_name="test_db", start_time=start_time, end_time=end_time
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_temp_table_ddl_query_compiles(
        self, query_class: Type[RedshiftCommonQuery]
    ) -> None:
        """Test that temp_table_ddl_query compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        start_time = datetime(2023, 1, 1, 12, 0, 0)
        end_time = datetime(2023, 1, 2, 12, 0, 0)

        sql = query.temp_table_ddl_query(start_time, end_time)

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_usage_query_compiles(self, query_class: Type[RedshiftCommonQuery]) -> None:
        """Test that usage_query compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        start_time = "2023-01-01 12:00:00"
        end_time = "2023-01-02 12:00:00"

        sql = query.usage_query(
            start_time=start_time, end_time=end_time, database="test_db"
        )

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None

    def test_operation_aspect_query_compiles(
        self, query_class: Type[RedshiftCommonQuery]
    ) -> None:
        """Test that operation_aspect_query compiles correctly."""
        query = query_class(alternative_system_tables_schema=None)
        start_time = "2023-01-01 12:00:00"
        end_time = "2023-01-02 12:00:00"

        sql = query.operation_aspect_query(start_time=start_time, end_time=end_time)

        assert sqlglot.parse_one(sql, dialect=Redshift) is not None


class TestAlternativeSchemaIntegration:
    """Test that alternative schema is properly integrated into system table queries."""

    def test_alternative_schema_used_in_system_table_queries(self):
        """Test that alternative schema is used in system table queries."""
        alternative_schema = "custom_db.custom_schema"
        query = RedshiftProvisionedQuery(
            alternative_system_tables_schema=alternative_schema
        )

        # Test that system table queries use the alternative schema
        sql = query.get_database_details(database="test_db")
        assert alternative_schema in sql
        assert "custom_db.custom_schema.svv_redshift_databases" in sql

        sql = query.list_schemas(database="test_db")
        assert alternative_schema in sql
        assert "custom_db.custom_schema.svv_redshift_schemas" in sql
        assert "custom_db.custom_schema.SVV_EXTERNAL_SCHEMAS" in sql

        sql = query.list_tables(
            database="test_db", skip_external_tables=False, is_shared_database=True
        )
        assert alternative_schema in sql
        assert "custom_db.custom_schema.svv_redshift_tables" in sql

        sql = query.list_columns(
            database_name="test_db", schema_name="public", is_shared_database=True
        )
        assert alternative_schema in sql
        assert "custom_db.custom_schema.SVV_REDSHIFT_COLUMNS" in sql

        sql = query.list_outbound_datashares()
        assert alternative_schema in sql
        assert "custom_db.custom_schema.svv_datashares" in sql

        sql = query.get_inbound_datashare(database="test_db")
        assert alternative_schema in sql
        assert "custom_db.custom_schema.svv_datashares" in sql

    def test_no_alternative_schema_uses_default(self):
        """Test that queries use default system table names when no alternative schema is provided."""
        query = RedshiftProvisionedQuery(alternative_system_tables_schema=None)

        # Test that system table queries use default names
        sql = query.get_database_details(database="test_db")
        assert "svv_redshift_databases" in sql
        assert "custom_db.custom_schema.svv_redshift_databases" not in sql

        sql = query.list_schemas(database="test_db")
        assert "svv_redshift_schemas" in sql
        assert "SVV_EXTERNAL_SCHEMAS" in sql
        assert "custom_db.custom_schema" not in sql

        sql = query.list_outbound_datashares()
        assert "svv_datashares" in sql
        assert "custom_db.custom_schema.svv_datashares" not in sql
