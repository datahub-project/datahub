"""Unit tests for Postgres Foreign Data Wrapper (FDW) support."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.engine import Inspector

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import (
    ForeignTableMetadata,
    PostgresConfig,
    PostgresSource,
)


@pytest.fixture
def postgres_config():
    """Create a basic Postgres configuration."""
    return PostgresConfig(
        host_port="localhost:5432",
        database="test_db",
        username="test_user",
        password="test_password",
        include_foreign_tables=True,
    )


@pytest.fixture
def postgres_source(postgres_config):
    """Create a Postgres source instance."""
    ctx = PipelineContext(run_id="test_run")
    return PostgresSource(postgres_config, ctx)


class TestGetForeignTableNames:
    """Tests for get_foreign_table_names method."""

    def test_get_foreign_table_names_success(self, postgres_source):
        """Test successful retrieval of foreign table names."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(
            return_value=iter([("fdw_table1",), ("fdw_table2",), ("fdw_table3",)])
        )
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_names(mock_inspector, "public")

        assert result == ["fdw_table1", "fdw_table2", "fdw_table3"]
        mock_conn.execute.assert_called_once()

    def test_get_foreign_table_names_empty(self, postgres_source):
        """Test when no foreign tables exist."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_names(mock_inspector, "public")

        assert result == []

    def test_get_foreign_table_names_exception(self, postgres_source):
        """Test handling of exceptions during foreign table retrieval."""
        mock_inspector = MagicMock()
        mock_inspector.engine.connect.side_effect = Exception("Connection failed")

        result = postgres_source.get_foreign_table_names(mock_inspector, "public")

        assert result == []


class TestGetForeignTableMetadata:
    """Tests for get_foreign_table_metadata method."""

    def test_get_foreign_table_metadata_success(self, postgres_source):
        """Test successful retrieval of FDW metadata."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (
            "external_sales",
            "sqlserver1",
            "tds_fdw",
            "host=sqlserver.example.com, port=1433, dbname=sales",
            "schema_name=dbo, table_name=sales",
        )
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_metadata(
            mock_inspector, "public", "external_sales"
        )

        assert result is not None
        assert isinstance(result, ForeignTableMetadata)
        assert result.table_name == "external_sales"
        assert result.server_name == "sqlserver1"
        assert result.fdw_name == "tds_fdw"
        assert result.server_options is not None
        assert "host=sqlserver.example.com" in result.server_options
        assert result.table_options is not None
        assert "schema_name=dbo" in result.table_options

    def test_get_foreign_table_metadata_not_found(self, postgres_source):
        """Test when foreign table metadata is not found."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_metadata(
            mock_inspector, "public", "nonexistent_table"
        )

        assert result is None

    def test_get_foreign_table_metadata_exception(self, postgres_source):
        """Test handling of exceptions during metadata retrieval."""
        mock_inspector = MagicMock()
        mock_inspector.engine.connect.side_effect = Exception("Connection failed")

        result = postgres_source.get_foreign_table_metadata(
            mock_inspector, "public", "external_sales"
        )

        assert result is None

    def test_get_foreign_table_metadata_partial_data(self, postgres_source):
        """Test metadata with some None values."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (
            "external_table",
            "server1",
            None,  # fdw_name is None
            None,  # server_options is None
            "table_option=value",
        )
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_metadata(
            mock_inspector, "public", "external_table"
        )

        assert result is not None
        assert result.table_name == "external_table"
        assert result.server_name == "server1"
        assert result.fdw_name is None
        assert result.server_options is None
        assert result.table_options == "table_option=value"


class TestConfigurationOptions:
    """Tests for FDW configuration options."""

    def test_include_foreign_tables_default(self):
        """Test that include_foreign_tables defaults to True."""
        config = PostgresConfig(
            host_port="localhost:5432",
            database="test_db",
            username="test_user",
            password="test_password",
        )
        assert config.include_foreign_tables is True

    def test_include_foreign_tables_disabled(self):
        """Test disabling foreign table ingestion."""
        config = PostgresConfig(
            host_port="localhost:5432",
            database="test_db",
            username="test_user",
            password="test_password",
            include_foreign_tables=False,
        )
        assert config.include_foreign_tables is False

    def test_foreign_table_pattern_default(self):
        """Test that foreign_table_pattern defaults to allow all."""
        config = PostgresConfig(
            host_port="localhost:5432",
            database="test_db",
            username="test_user",
            password="test_password",
        )
        assert config.foreign_table_pattern.allowed("test_db.public.any_table")

    def test_foreign_table_pattern_custom(self):
        """Test custom foreign table pattern filtering."""
        from datahub.configuration.common import AllowDenyPattern

        config = PostgresConfig(
            host_port="localhost:5432",
            database="test_db",
            username="test_user",
            password="test_password",
            foreign_table_pattern=AllowDenyPattern(
                allow=[".*\\.public\\.fdw_.*"], deny=[".*_temp$"]
            ),
        )

        assert config.foreign_table_pattern.allowed("test_db.public.fdw_sales")
        assert not config.foreign_table_pattern.allowed("test_db.public.regular_table")
        assert not config.foreign_table_pattern.allowed("test_db.public.fdw_sales_temp")


class TestLoopForeignTables:
    """Tests for loop_foreign_tables method."""

    @patch.object(PostgresSource, "_process_foreign_table")
    @patch.object(PostgresSource, "get_foreign_table_names")
    @patch.object(PostgresSource, "get_identifier")
    def test_loop_foreign_tables_success(
        self,
        mock_get_identifier,
        mock_get_foreign_table_names,
        mock_process_foreign_table,
        postgres_source,
    ):
        """Test successful processing of foreign tables."""
        mock_inspector = MagicMock()
        mock_get_foreign_table_names.return_value = ["fdw_table1", "fdw_table2"]
        mock_get_identifier.side_effect = [
            "test_db.public.fdw_table1",
            "test_db.public.fdw_table2",
        ]
        mock_process_foreign_table.return_value = iter([])

        sql_config = MagicMock()
        sql_config.table_pattern.allowed.return_value = True

        list(postgres_source.loop_foreign_tables(mock_inspector, "public", sql_config))

        assert mock_process_foreign_table.call_count == 2

    @patch.object(PostgresSource, "get_foreign_table_names")
    def test_loop_foreign_tables_disabled(
        self, mock_get_foreign_table_names, postgres_config
    ):
        """Test that foreign tables are not processed when disabled."""
        postgres_config.include_foreign_tables = False
        ctx = PipelineContext(run_id="test_run")
        source = PostgresSource(postgres_config, ctx)

        mock_inspector = MagicMock()
        sql_config = MagicMock()

        result = list(source.loop_foreign_tables(mock_inspector, "public", sql_config))

        assert result == []
        mock_get_foreign_table_names.assert_not_called()

    @patch.object(PostgresSource, "get_foreign_table_names")
    @patch.object(PostgresSource, "get_identifier")
    def test_loop_foreign_tables_pattern_filtering(
        self, mock_get_identifier, mock_get_foreign_table_names, postgres_config
    ):
        """Test foreign table pattern filtering."""
        from datahub.configuration.common import AllowDenyPattern

        postgres_config.foreign_table_pattern = AllowDenyPattern(
            allow=[".*\\.fdw_.*"], deny=[]
        )
        ctx = PipelineContext(run_id="test_run")
        source = PostgresSource(postgres_config, ctx)

        mock_inspector = MagicMock()
        mock_get_foreign_table_names.return_value = [
            "fdw_sales",
            "regular_table",
            "fdw_orders",
        ]
        mock_get_identifier.side_effect = [
            "test_db.public.fdw_sales",
            "test_db.public.regular_table",
            "test_db.public.fdw_orders",
        ]

        sql_config = MagicMock()
        sql_config.table_pattern.allowed.return_value = True

        with patch.object(
            source, "_process_foreign_table", return_value=iter([])
        ) as mock_process:
            list(source.loop_foreign_tables(mock_inspector, "public", sql_config))

        # Should only process tables matching the pattern
        assert mock_process.call_count == 2

    @patch.object(PostgresSource, "get_foreign_table_names")
    @patch.object(PostgresSource, "get_identifier")
    def test_loop_foreign_tables_deduplication(
        self, mock_get_identifier, mock_get_foreign_table_names, postgres_source
    ):
        """Test that duplicate foreign tables are skipped."""
        mock_inspector = MagicMock()
        mock_get_foreign_table_names.return_value = [
            "fdw_table1",
            "fdw_table1",  # Duplicate
            "fdw_table2",
        ]
        mock_get_identifier.side_effect = [
            "test_db.public.fdw_table1",
            "test_db.public.fdw_table1",  # Same identifier
            "test_db.public.fdw_table2",
        ]

        sql_config = MagicMock()
        sql_config.table_pattern.allowed.return_value = True

        with patch.object(
            postgres_source, "_process_foreign_table", return_value=iter([])
        ) as mock_process:
            list(
                postgres_source.loop_foreign_tables(
                    mock_inspector, "public", sql_config
                )
            )

        # Should only process unique tables
        assert mock_process.call_count == 2


class TestProcessForeignTable:
    """Tests for _process_foreign_table method."""

    @patch.object(PostgresSource, "_process_table")
    @patch.object(PostgresSource, "get_foreign_table_metadata")
    def test_process_foreign_table_with_metadata(
        self, mock_get_metadata, mock_process_table, postgres_source
    ):
        """Test processing foreign table with FDW metadata."""
        mock_inspector = MagicMock(spec=Inspector)
        mock_get_metadata.return_value = ForeignTableMetadata(
            table_name="external_sales",
            server_name="sqlserver1",
            fdw_name="tds_fdw",
            server_options="host=sqlserver.example.com",
            table_options="schema_name=dbo",
        )
        mock_process_table.return_value = iter([])

        sql_config = MagicMock()

        workunits = list(
            postgres_source._process_foreign_table(
                "test_db.public.external_sales",
                mock_inspector,
                "public",
                "external_sales",
                sql_config,
            )
        )

        # Should call _process_table
        mock_process_table.assert_called_once()

        # Should emit MCP with custom properties
        assert len(workunits) >= 1

    @patch.object(PostgresSource, "_process_table")
    @patch.object(PostgresSource, "get_foreign_table_metadata")
    def test_process_foreign_table_without_metadata(
        self, mock_get_metadata, mock_process_table, postgres_source
    ):
        """Test processing foreign table without FDW metadata."""
        mock_inspector = MagicMock(spec=Inspector)
        mock_get_metadata.return_value = None
        mock_process_table.return_value = iter([])

        sql_config = MagicMock()

        list(
            postgres_source._process_foreign_table(
                "test_db.public.external_sales",
                mock_inspector,
                "public",
                "external_sales",
                sql_config,
            )
        )

        # Should still call _process_table
        mock_process_table.assert_called_once()


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_foreign_table_with_special_characters(self, postgres_source):
        """Test handling of foreign tables with special characters in names."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(
            return_value=iter([("fdw-table.with-special@chars",)])
        )
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_names(mock_inspector, "public")

        assert result == ["fdw-table.with-special@chars"]

    def test_foreign_table_in_non_public_schema(self, postgres_source):
        """Test foreign tables in non-public schemas."""
        mock_inspector = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([("fdw_table",)]))
        mock_conn.execute.return_value = mock_result
        mock_inspector.engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_inspector.engine.connect.return_value.__exit__ = MagicMock(
            return_value=None
        )

        result = postgres_source.get_foreign_table_names(
            mock_inspector, "custom_schema"
        )

        assert result == ["fdw_table"]

    @patch.object(PostgresSource, "get_foreign_table_names")
    def test_loop_foreign_tables_exception_handling(
        self, mock_get_foreign_table_names, postgres_source
    ):
        """Test exception handling in loop_foreign_tables."""
        mock_inspector = MagicMock()
        mock_get_foreign_table_names.side_effect = Exception("Database error")

        sql_config = MagicMock()

        # Should not raise exception, just log and continue
        result = list(
            postgres_source.loop_foreign_tables(mock_inspector, "public", sql_config)
        )

        assert result == []
