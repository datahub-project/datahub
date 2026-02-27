import unittest.mock
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import sqlalchemy.exc
from pydantic import ValidationError
from sqlalchemy.dialects.oracle.base import ischema_names
from sqlalchemy.engine import Inspector
from sqlalchemy.sql import sqltypes

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.oracle import (
    VSQL_USAGE_QUERY,
    OracleConfig,
    OracleInspectorObjectWrapper,
    OracleObjectType,
    OracleSource,
    ProcedureDependencies,
    VSqlPrerequisiteCheckResult,
    extra_oracle_types,
)
from datahub.ingestion.source.sql.stored_procedures.base import BaseProcedure
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
)


def test_oracle_config():
    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
    }

    config = OracleConfig.model_validate(
        {
            **base_config,
            "service_name": "svc01",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "oracle://user:password@host:1521/?service_name=svc01"
    )

    with pytest.raises(ValueError):
        config = OracleConfig.model_validate(
            {
                **base_config,
                "database": "db",
                "service_name": "svc01",
            }
        )

    with unittest.mock.patch(
        "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_workunits"
    ):
        OracleSource.create(
            {
                **base_config,
                "service_name": "svc01",
            },
            PipelineContext("test-oracle-config"),
        ).get_workunits()


def test_oracle_config_stored_procedures():
    """Test Oracle configuration for stored procedures."""
    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
        "service_name": "svc01",
    }

    # Test default stored procedures configuration
    config = OracleConfig.parse_obj(base_config)
    assert config.include_stored_procedures is True
    assert config.procedure_pattern == AllowDenyPattern.allow_all()
    assert config.include_materialized_views is True
    assert config.include_usage_stats is False
    assert config.include_operational_stats is False

    # Test custom stored procedures configuration
    custom_config = {
        **base_config,
        "include_stored_procedures": False,
        "procedure_pattern": {"allow": ["HR.*"], "deny": ["SYS.*"]},
        "include_materialized_views": False,
        "include_usage_stats": False,
        "include_operational_stats": False,
    }
    config = OracleConfig.parse_obj(custom_config)
    assert config.include_stored_procedures is False
    assert config.include_materialized_views is False
    assert config.include_usage_stats is False
    assert config.include_operational_stats is False
    assert "HR.*" in config.procedure_pattern.allow
    assert "SYS.*" in config.procedure_pattern.deny


def test_oracle_config_data_dictionary_mode():
    """Test Oracle configuration validation for data dictionary mode."""
    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
        "service_name": "svc01",
    }

    # Test valid data dictionary modes
    for mode in ["ALL", "DBA"]:
        config = OracleConfig.parse_obj({**base_config, "data_dictionary_mode": mode})
        assert config.data_dictionary_mode == mode

    # Test invalid data dictionary mode
    with pytest.raises(
        ValidationError, match="Specify one of data dictionary views mode"
    ):
        OracleConfig.parse_obj({**base_config, "data_dictionary_mode": "INVALID"})


def test_oracle_get_db_name_with_service_name():
    """Test getting database name when using service_name with ALL mode."""

    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
        "service_name": "svc01",
        "data_dictionary_mode": "ALL",
    }

    config = OracleConfig.parse_obj(base_config)
    ctx = PipelineContext(run_id="test-oracle-service-name")

    with patch("datahub.ingestion.source.sql.oracle.oracledb"):
        source = OracleSource(config, ctx)

        # Mock inspector with empty database in URL (simulating service_name usage)
        mock_inspector = Mock()
        mock_engine = Mock()
        mock_url = Mock()
        mock_url.database = None  # This is the case when using service_name
        mock_engine.url = mock_url
        mock_inspector.engine = mock_engine

        # Mock bind and the database query result
        mock_bind = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = "TESTDB"  # Oracle returns uppercase
        mock_bind.execute.return_value = mock_result
        mock_inspector.bind = mock_bind

        # Mock dialect for normalization (Oracle normalizes to lowercase)
        mock_dialect = Mock()
        mock_dialect.normalize_name.return_value = "testdb"  # normalized to lowercase
        mock_inspector.dialect = mock_dialect

        # Test that get_db_name queries Oracle and normalizes the database name
        db_name = source.get_db_name(mock_inspector)

        assert db_name == "testdb"  # Should be normalized to lowercase
        mock_bind.execute.assert_called_once()
        call_args = mock_bind.execute.call_args
        # Verify the exact SQL query is used
        query_text = str(call_args[0][0]).upper()
        assert "SYS_CONTEXT('USERENV','DB_NAME')" in query_text
        assert "FROM DUAL" in query_text
        mock_dialect.normalize_name.assert_called_once_with("TESTDB")


def test_oracle_get_db_name_with_service_name_dba_mode():
    """Test getting database name when using service_name with DBA mode."""

    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
        "service_name": "svc01",
        "data_dictionary_mode": "DBA",
    }

    config = OracleConfig.parse_obj(base_config)
    ctx = PipelineContext(run_id="test-oracle-service-name-dba")

    with patch("datahub.ingestion.source.sql.oracle.oracledb"):
        source = OracleSource(config, ctx)

        # Mock OracleInspectorObjectWrapper with empty database in URL
        mock_inspector = Mock(spec=OracleInspectorObjectWrapper)
        mock_engine = Mock()
        mock_url = Mock()
        mock_url.database = None  # This is the case when using service_name
        mock_engine.url = mock_url
        mock_inspector.engine = mock_engine

        # Mock the wrapper's get_db_name method (returns uppercase from Oracle)
        mock_inspector.get_db_name.return_value = "TESTDB_DBA"

        # Mock dialect for normalization
        mock_dialect = Mock()
        mock_dialect.normalize_name.return_value = "testdb_dba"  # normalized
        mock_inspector.dialect = mock_dialect

        # Test that get_db_name uses the wrapper's method and normalizes
        db_name = source.get_db_name(mock_inspector)

        assert db_name == "testdb_dba"  # Should be normalized
        mock_inspector.get_db_name.assert_called_once()
        mock_dialect.normalize_name.assert_called_once_with("TESTDB_DBA")


def test_oracle_get_db_name_database_error():
    """Test error handling when Oracle database query fails."""
    base_config = {
        "username": "user",
        "password": "password",
        "host_port": "host:1521",
        "service_name": "svc01",
        "data_dictionary_mode": "ALL",
    }

    config = OracleConfig.parse_obj(base_config)
    ctx = PipelineContext(run_id="test-oracle-db-error")

    with patch("datahub.ingestion.source.sql.oracle.oracledb"):
        source = OracleSource(config, ctx)

        # Mock inspector with empty database in URL
        mock_inspector = Mock()
        mock_engine = Mock()
        mock_url = Mock()
        mock_url.database = None
        mock_engine.url = mock_url
        mock_inspector.engine = mock_engine

        # Mock bind to raise DatabaseError
        mock_bind = Mock()
        mock_bind.execute.side_effect = sqlalchemy.exc.DatabaseError(
            "Connection failed", None, None
        )
        mock_inspector.bind = mock_bind

        # Mock dialect (even though we won't reach normalization)
        mock_dialect = Mock()
        mock_inspector.dialect = mock_dialect

        # Test that get_db_name handles the error gracefully
        db_name = source.get_db_name(mock_inspector)

        # Should return empty string when query fails
        assert db_name == ""
        mock_bind.execute.assert_called_once()
        # Normalization should not be called since query failed
        mock_dialect.normalize_name.assert_not_called()


class TestOracleInspectorObjectWrapper:
    """Test cases for OracleInspectorObjectWrapper."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_inspector = Mock(spec=Inspector)
        self.mock_inspector.bind = Mock()
        self.mock_inspector.dialect = Mock()
        self.mock_inspector.dialect.normalize_name = Mock(
            side_effect=lambda x: x.lower()
        )
        self.mock_inspector.dialect.denormalize_name = Mock(
            side_effect=lambda x: x.upper()
        )
        self.mock_inspector.dialect.default_schema_name = "TEST_SCHEMA"

        self.wrapper = OracleInspectorObjectWrapper(self.mock_inspector)

    def test_get_materialized_view_names(self):
        """Test getting materialized view names."""
        # Mock the database response
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([("MV1",), ("MV2",)]))
        self.mock_inspector.bind.execute.return_value = mock_cursor

        result = self.wrapper.get_materialized_view_names("test_schema")

        assert result == ["mv1", "mv2"]
        self.mock_inspector.bind.execute.assert_called_once()
        call_args = self.mock_inspector.bind.execute.call_args
        assert "dba_mviews" in str(call_args[0][0]).lower()

    def test_get_materialized_view_definition(self):
        """Test getting materialized view definition."""
        # Reset the mock for this test
        self.mock_inspector.bind.execute.reset_mock()

        mock_definition = "SELECT * FROM test_table"
        self.mock_inspector.bind.execute.return_value.scalar.return_value = (
            mock_definition
        )

        result = self.wrapper.get_materialized_view_definition("test_mv", "test_schema")

        assert result == mock_definition
        self.mock_inspector.bind.execute.assert_called_once()
        call_args = self.mock_inspector.bind.execute.call_args
        assert "dba_mviews" in str(call_args[0][0]).lower()

    def test_get_columns_sdo_geometry(self):
        """SDO_GEOMETRY columns must resolve to a non-null type when ischema_names is patched."""
        self.mock_inspector.dialect.server_version_info = (19,)
        self.mock_inspector.bind.execute.return_value = [
            [
                "GEOM_COLUMN",
                "SDO_GEOMETRY",
                0,
                None,
                None,
                "Y",
                None,
                None,
                "NO",
                None,
                None,
            ]
        ]

        with patch.dict(
            "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
            {klass.__name__: klass for klass in extra_oracle_types},
            clear=False,
        ):
            columns = self.wrapper.get_columns("TEST_TABLE", "TEST_SCHEMA")

        assert len(columns) == 1
        assert columns[0]["name"] == "geom_column"
        assert not isinstance(columns[0]["type"], sqltypes.NullType)

    def test_get_columns_xmltype(self):
        """XMLTYPE columns must resolve to a non-null type when ischema_names is patched."""
        self.mock_inspector.dialect.server_version_info = (19,)
        self.mock_inspector.bind.execute.return_value = [
            ["XML_COLUMN", "XMLTYPE", 0, None, None, "Y", None, None, "NO", None, None]
        ]

        with patch.dict(
            "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
            {klass.__name__: klass for klass in extra_oracle_types},
            clear=False,
        ):
            columns = self.wrapper.get_columns("TEST_TABLE", "TEST_SCHEMA")

        assert len(columns) == 1
        assert columns[0]["name"] == "xml_column"
        assert not isinstance(columns[0]["type"], sqltypes.NullType)


class TestOracleSource:
    """Test cases for OracleSource."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = OracleConfig(
            username="test_user",
            password="test_password",
            host_port="localhost:1521",
            service_name="test_service",
            include_stored_procedures=True,
            include_materialized_views=True,
        )
        self.ctx = PipelineContext(run_id="test-oracle-source")

    @patch("datahub.ingestion.source.sql.oracle.oracledb")
    def test_oracle_source_initialization(self, mock_oracledb):
        """Test Oracle source initialization."""
        source = OracleSource(self.config, self.ctx)
        assert source.config == self.config
        assert source.ctx == self.ctx

    @patch("datahub.ingestion.source.sql.oracle.oracledb")
    def test_oracle_source_sql_aggregator_initialization(self, mock_oracledb):
        """Test Oracle source SQL aggregator initialization with usage and operations."""
        # Test with usage and operations enabled
        config_with_stats = OracleConfig(
            username="test_user",
            password="test_password",
            host_port="localhost:1521",
            service_name="test_service",
            include_usage_stats=True,
            include_operational_stats=True,
        )

        source = OracleSource(config_with_stats, self.ctx)

        # Should have custom aggregator with usage and operations enabled
        assert hasattr(source, "aggregator")
        assert source.aggregator.generate_usage_statistics is True
        assert source.aggregator.generate_operations is True

        # Test with usage and operations disabled
        config_no_stats = OracleConfig(
            username="test_user",
            password="test_password",
            host_port="localhost:1521",
            service_name="test_service",
            include_usage_stats=False,
            include_operational_stats=False,
        )

        source_no_stats = OracleSource(config_no_stats, self.ctx)

        # Should use default aggregator from parent class
        assert source_no_stats.aggregator.generate_usage_statistics is False
        assert source_no_stats.aggregator.generate_operations is False

    def test_get_procedures_for_schema(self):
        """Test getting stored procedures for a schema."""
        source = OracleSource(self.config, self.ctx)

        # Mock inspector and connection
        mock_inspector = Mock()
        mock_connection = Mock()

        # Set up context manager support
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_inspector.engine.connect.return_value = mock_context_manager

        # Mock procedure query results
        mock_proc = Mock()
        mock_proc.name = "TEST_PROC"
        mock_proc.type = "PROCEDURE"
        mock_proc.created = datetime.now()
        mock_proc.last_ddl_time = datetime.now()
        mock_proc.status = "VALID"

        mock_func = Mock()
        mock_func.name = "TEST_FUNC"
        mock_func.type = "FUNCTION"
        mock_func.created = datetime.now()
        mock_func.last_ddl_time = datetime.now()
        mock_func.status = "VALID"

        mock_procedures = [mock_proc, mock_func]
        mock_connection.execute.return_value = mock_procedures

        # Mock the helper methods
        with (
            patch.object(
                source,
                "_get_procedure_source_code",
                return_value="CREATE PROCEDURE test_proc AS BEGIN NULL; END;",
            ),
            patch.object(
                source, "_get_procedure_arguments", return_value="IN param1 VARCHAR2"
            ),
            patch.object(
                source,
                "_get_procedure_dependencies",
                return_value=ProcedureDependencies(upstream=["TEST_TABLE"]),
            ),
        ):
            result = source.get_procedures_for_schema(
                inspector=mock_inspector, schema="TEST_SCHEMA", db_name="TEST_DB"
            )

            assert len(result) == 2
            assert all(isinstance(proc, BaseProcedure) for proc in result)
            assert result[0].name == "TEST_PROC"
            assert result[1].name == "TEST_FUNC"
            assert result[0].language == "SQL"
            assert result[0].extra_properties is not None
            assert "upstream_dependencies" in result[0].extra_properties

    def test_get_procedure_source_code(self):
        """Test getting procedure source code."""
        source = OracleSource(self.config, self.ctx)

        mock_connection = Mock()
        mock_source_data = [
            Mock(text="CREATE PROCEDURE test_proc AS\n"),
            Mock(text="BEGIN\n"),
            Mock(text="  NULL;\n"),
            Mock(text="END;"),
        ]
        mock_connection.execute.return_value = mock_source_data

        result = source._get_procedure_source_code(
            mock_connection, "TEST_SCHEMA", "TEST_PROC", "PROCEDURE", "DBA"
        )

        expected = "CREATE PROCEDURE test_proc AS\nBEGIN\n  NULL;\nEND;"
        assert result == expected

        # Verify the query was called with correct parameters
        mock_connection.execute.assert_called_once()
        call_args = mock_connection.execute.call_args
        assert "dba_source" in str(call_args[0][0]).lower()

    def test_get_procedure_arguments(self):
        """Test getting procedure arguments."""
        source = OracleSource(self.config, self.ctx)

        mock_connection = Mock()
        mock_args_data = [
            Mock(argument_name="PARAM1", data_type="VARCHAR2", in_out="IN", position=1),
            Mock(argument_name="PARAM2", data_type="NUMBER", in_out="OUT", position=2),
        ]
        mock_connection.execute.return_value = mock_args_data

        result = source._get_procedure_arguments(
            mock_connection, "TEST_SCHEMA", "TEST_PROC", "DBA"
        )

        expected = "IN PARAM1 VARCHAR2, OUT PARAM2 NUMBER"
        assert result == expected

        # Verify the query was called
        mock_connection.execute.assert_called_once()
        call_args = mock_connection.execute.call_args
        assert "dba_arguments" in str(call_args[0][0]).lower()

    def test_get_procedure_dependencies(self):
        """Test getting procedure dependencies."""
        source = OracleSource(self.config, self.ctx)

        mock_connection = Mock()

        # Mock upstream dependencies
        mock_upstream_data = [
            Mock(
                referenced_owner="TEST_SCHEMA",
                referenced_name="TEST_TABLE",
                referenced_type="TABLE",
            ),
            Mock(
                referenced_owner="TEST_SCHEMA",
                referenced_name="OTHER_PROC",
                referenced_type="PROCEDURE",
            ),
        ]
        # Set attributes explicitly to avoid Mock object issues
        mock_upstream_data[0].referenced_name = "TEST_TABLE"
        mock_upstream_data[1].referenced_name = "OTHER_PROC"

        # Mock downstream dependencies
        mock_downstream_data = [
            Mock(owner="TEST_SCHEMA", name="DEPENDENT_PROC", type="PROCEDURE"),
        ]
        # Set the name attribute explicitly to avoid Mock object issues
        mock_downstream_data[0].name = "DEPENDENT_PROC"

        mock_connection.execute.side_effect = [mock_upstream_data, mock_downstream_data]

        result = source._get_procedure_dependencies(
            mock_connection, "TEST_SCHEMA", "TEST_PROC", "DBA"
        )

        assert result is not None
        assert result.upstream is not None
        assert result.downstream is not None
        assert len(result.upstream) == 2
        assert len(result.downstream) == 1
        assert "TEST_SCHEMA.TEST_TABLE (TABLE)" in result.upstream
        assert "TEST_SCHEMA.OTHER_PROC (PROCEDURE)" in result.upstream
        assert "TEST_SCHEMA.DEPENDENT_PROC (PROCEDURE)" in result.downstream

        # Verify both queries were called
        assert mock_connection.execute.call_count == 2

    def test_loop_materialized_views(self):
        """Test looping through materialized views."""
        source = OracleSource(self.config, self.ctx)

        # Mock inspector with materialized view support
        mock_inspector = Mock()
        mock_inspector.get_materialized_view_names.return_value = ["MV1", "MV2"]

        # Mock the _process_materialized_view method
        mock_workunit = Mock()
        with (
            patch.object(source, "get_identifier", return_value="test_schema.mv1"),
            patch.object(
                source, "_process_materialized_view", return_value=[mock_workunit]
            ),
        ):
            result = list(
                source.loop_materialized_views(
                    mock_inspector, "TEST_SCHEMA", self.config
                )
            )

            # Should process both materialized views
            assert len(result) == 2
            assert all(wu == mock_workunit for wu in result)

    def test_get_materialized_view_names_fallback(self):
        """Test fallback method for getting materialized view names."""
        source = OracleSource(self.config, self.ctx)

        mock_inspector = Mock()
        mock_inspector.dialect.denormalize_name.return_value = "TEST_SCHEMA"
        mock_inspector.dialect.default_schema_name = "TEST_SCHEMA"
        mock_inspector.dialect.normalize_name.side_effect = lambda x: x.lower()

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([("MV1",), ("MV2",)]))
        mock_connection.execute.return_value = mock_cursor

        # Set up context manager support
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_inspector.engine.connect.return_value = mock_context_manager

        result = source._get_materialized_view_names_fallback(
            inspector=mock_inspector, schema="test_schema"
        )

        assert len(result) == 2
        mock_connection.execute.assert_called_once()

    def test_get_materialized_view_definition_fallback(self):
        """Test fallback method for getting materialized view definition."""
        source = OracleSource(self.config, self.ctx)

        mock_inspector = Mock()
        mock_inspector.dialect.denormalize_name.side_effect = lambda x: x.upper()
        mock_inspector.dialect.default_schema_name = "TEST_SCHEMA"

        mock_definition = "SELECT * FROM test_table"
        mock_inspector.bind.execute.return_value.scalar.return_value = mock_definition

        result = source._get_materialized_view_definition_fallback(
            inspector=mock_inspector, mview_name="test_mv", schema="test_schema"
        )

        assert result == mock_definition
        mock_inspector.bind.execute.assert_called_once()

    def test_process_materialized_view(self):
        """Test processing a single materialized view."""
        source = OracleSource(self.config, self.ctx)

        # Mock inspector
        mock_inspector = Mock()
        mock_inspector.get_materialized_view_definition.return_value = (
            "SELECT * FROM test_table"
        )

        # Mock required methods
        with (
            patch.object(
                source, "get_table_properties", return_value=("Test MV", {}, None)
            ),
            patch.object(source, "_get_columns", return_value=[]),
            patch(
                "datahub.ingestion.source.sql.sql_common.get_schema_metadata",
                return_value=Mock(),
            ),
            patch(
                "datahub.ingestion.source.sql.oracle.make_dataset_urn_with_platform_instance",
                return_value="urn:li:dataset:(urn:li:dataPlatform:oracle,test_schema.test_mv,PROD)",
            ),
            patch.object(source, "add_table_to_schema_container", return_value=[]),
            patch.object(source, "get_db_name", return_value="TEST_DB"),
            patch.object(
                source,
                "get_dataplatform_instance_aspect",
                return_value=None,
            ),
        ):
            result = list(
                source._process_materialized_view(
                    "test_schema.test_mv",
                    mock_inspector,
                    "TEST_SCHEMA",
                    "TEST_MV",
                    self.config,
                )
            )

            # Should generate multiple work units (properties, schema, subtypes, view properties)
            assert len(result) >= 3

            # Look for ViewPropertiesClass work unit in metadata
            view_properties_workunits = [
                wu
                for wu in result
                if hasattr(wu, "metadata")
                and hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "ViewPropertiesClass"
            ]

            # We should have at least one ViewPropertiesClass work unit with materialized=True
            assert len(view_properties_workunits) > 0
            workunit = view_properties_workunits[0]
            assert hasattr(workunit.metadata, "aspect")
            view_properties_aspect = workunit.metadata.aspect
            assert view_properties_aspect is not None
            assert hasattr(view_properties_aspect, "materialized")
            assert view_properties_aspect.materialized is True

    def test_error_handling_in_get_procedures_for_schema(self):
        """Test error handling in get_procedures_for_schema."""
        source = OracleSource(self.config, self.ctx)

        mock_inspector = Mock()
        mock_connection = Mock()

        # Set up context manager support
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_inspector.engine.connect.return_value = mock_context_manager

        # Simulate database error
        mock_connection.execute.side_effect = Exception("Database connection error")

        result = source.get_procedures_for_schema(
            inspector=mock_inspector, schema="TEST_SCHEMA", db_name="TEST_DB"
        )

        # Should return empty list on error
        assert result == []

    def test_error_handling_in_procedure_methods(self):
        """Test error handling in procedure helper methods."""
        source = OracleSource(self.config, self.ctx)

        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Query error")

        # Test source code method
        source_result = source._get_procedure_source_code(
            conn=mock_connection,
            schema="TEST_SCHEMA",
            procedure_name="TEST_PROC",
            object_type="PROCEDURE",
            tables_prefix="DBA",
        )
        assert source_result is None

        # Test arguments method
        args_result = source._get_procedure_arguments(
            conn=mock_connection,
            schema="TEST_SCHEMA",
            procedure_name="TEST_PROC",
            tables_prefix="DBA",
        )
        assert args_result is None

        # Test dependencies method
        deps_result = source._get_procedure_dependencies(
            conn=mock_connection,
            schema="TEST_SCHEMA",
            procedure_name="TEST_PROC",
            tables_prefix="DBA",
        )
        assert deps_result is None


class TestOracleQueryExtraction:
    """Test Oracle query extraction from V$SQL."""

    @pytest.fixture
    def config_with_query_usage(self):
        return OracleConfig.model_validate(
            {
                "username": "test_user",
                "password": "test_pass",
                "host_port": "localhost:1521",
                "service_name": "test_service",
                "include_query_usage": True,
                "max_queries_to_extract": 100,
            }
        )

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test")

    def test_vsql_prerequisites_success(self, config_with_query_usage, ctx):
        """Test V$SQL prerequisites check when access is granted."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_engine = Mock()
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = 1
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_conn

        result = source._check_vsql_prerequisites(mock_engine)

        assert result.is_accessible is True
        assert "accessible" in result.message.lower()

    def test_vsql_prerequisites_no_table(self, config_with_query_usage, ctx):
        """Test V$SQL prerequisites check when view doesn't exist."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_engine = Mock()
        mock_conn = Mock()
        mock_conn.execute.side_effect = sqlalchemy.exc.DatabaseError(
            statement="SELECT COUNT(*) FROM V$SQL",
            params={},
            orig=Exception("ORA-00942: table or view does not exist"),
        )
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_conn

        result = source._check_vsql_prerequisites(mock_engine)

        assert result.is_accessible is False
        assert "not accessible" in result.message

    def test_vsql_prerequisites_insufficient_privileges(
        self, config_with_query_usage, ctx
    ):
        """Test V$SQL prerequisites check when user lacks privileges."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_engine = Mock()
        mock_conn = Mock()
        mock_conn.execute.side_effect = sqlalchemy.exc.DatabaseError(
            statement="SELECT COUNT(*) FROM V$SQL",
            params={},
            orig=Exception("ORA-01031: insufficient privileges"),
        )
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_conn

        result = source._check_vsql_prerequisites(mock_engine)

        assert result.is_accessible is False
        assert "Insufficient privileges" in result.message

    def test_extract_queries_from_vsql_success(self, config_with_query_usage, ctx):
        """Test successful query extraction from V$SQL."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_rows = [
            {
                "sql_id": "abc123",
                "sql_text": "SELECT * FROM HR.EMPLOYEES WHERE dept_id = 10",
                "parsing_schema_name": "HR",
                "executions": 100,
                "elapsed_seconds": 5.5,
                "first_load_time": "2024-01-15 10:30:00",
            },
            {
                "sql_id": "def456",
                "sql_text": "SELECT COUNT(*) FROM SALES.ORDERS",
                "parsing_schema_name": "SALES",
                "executions": 50,
                "elapsed_seconds": 2.3,
                "first_load_time": "2024-01-15 11:00:00",
            },
        ]

        mock_engine = Mock()
        mock_conn = Mock()
        mock_inspector = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(mock_rows))
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_conn

        with (
            patch("sqlalchemy.inspect", return_value=mock_inspector),
            patch.object(source, "get_db_name", return_value="TESTDB"),
        ):
            queries = list(source._extract_queries_from_vsql(mock_engine))

        assert len(queries) == 2
        assert queries[0].query == "SELECT * FROM HR.EMPLOYEES WHERE dept_id = 10"
        assert queries[0].default_schema == "HR"
        assert queries[0].default_db == "TESTDB"
        assert queries[0].session_id == "sql_id:abc123"
        assert queries[1].query == "SELECT COUNT(*) FROM SALES.ORDERS"
        assert queries[1].default_schema == "SALES"

    def test_extract_queries_with_exclude_patterns(self, ctx):
        """Test query extraction with exclude patterns."""
        config = OracleConfig.model_validate(
            {
                "username": "test_user",
                "password": "test_pass",
                "host_port": "localhost:1521",
                "service_name": "test_service",
                "include_query_usage": True,
                "max_queries_to_extract": 100,
                "query_exclude_patterns": ["^SELECT.*FROM SYS\\..*", ".*DBMS_.*"],
            }
        )
        source = OracleSource(config, ctx)

        mock_rows = [
            {
                "sql_id": "abc123",
                "sql_text": "SELECT * FROM HR.EMPLOYEES",
                "parsing_schema_name": "HR",
                "executions": 100,
                "elapsed_seconds": 5.5,
                "first_load_time": "2024-01-15 10:30:00",
            },
            {
                "sql_id": "def456",
                "sql_text": "SELECT * FROM SYS.DBA_TABLES",
                "parsing_schema_name": "SYSTEM",
                "executions": 50,
                "elapsed_seconds": 2.3,
                "first_load_time": "2024-01-15 11:00:00",
            },
            {
                "sql_id": "ghi789",
                "sql_text": "BEGIN DBMS_OUTPUT.PUT_LINE('test'); END;",
                "parsing_schema_name": "HR",
                "executions": 20,
                "elapsed_seconds": 1.0,
                "first_load_time": "2024-01-15 12:00:00",
            },
        ]

        mock_engine = Mock()
        mock_conn = Mock()
        mock_inspector = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(mock_rows))
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_conn

        with (
            patch("sqlalchemy.inspect", return_value=mock_inspector),
            patch.object(source, "get_db_name", return_value="TESTDB"),
        ):
            queries = list(source._extract_queries_from_vsql(mock_engine))

        assert len(queries) == 1
        assert queries[0].query == "SELECT * FROM HR.EMPLOYEES"

    def test_extract_queries_database_error(self, config_with_query_usage, ctx):
        """Test query extraction handles database errors gracefully."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_engine = Mock()
        mock_conn = Mock()
        mock_conn.execute.side_effect = sqlalchemy.exc.DatabaseError(
            statement="SELECT FROM V$SQL",
            params={},
            orig=Exception("Connection lost"),
        )
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_conn

        queries = list(source._extract_queries_from_vsql(mock_engine))

        assert len(queries) == 0

    def test_populate_aggregator_skip_when_disabled(self, ctx):
        """Test that query population is skipped when include_query_usage is False."""
        config = OracleConfig.model_validate(
            {
                "username": "test_user",
                "password": "test_pass",
                "host_port": "localhost:1521",
                "service_name": "test_service",
                "include_query_usage": False,
            }
        )
        source = OracleSource(config, ctx)

        source._populate_aggregator_from_queries()

    def test_populate_aggregator_skip_on_permission_error(
        self, config_with_query_usage, ctx
    ):
        """Test that query population is skipped gracefully when V$SQL is not accessible."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_engine = Mock()
        with (
            patch.object(source, "_get_metadata_engine", return_value=mock_engine),
            patch.object(
                source,
                "_check_vsql_prerequisites",
                return_value=VSqlPrerequisiteCheckResult(
                    is_accessible=False, message="Insufficient privileges"
                ),
            ),
        ):
            source._populate_aggregator_from_queries()

        mock_engine.dispose.assert_called_once()

    def test_populate_aggregator_success(self, config_with_query_usage, ctx):
        """Test successful population of aggregator from queries."""
        source = OracleSource(config_with_query_usage, ctx)

        mock_engine = Mock()
        mock_queries = [
            ObservedQuery(
                query="SELECT * FROM test_table",
                default_schema="HR",
                default_db="TESTDB",
            )
        ]

        with (
            patch.object(source, "_get_metadata_engine", return_value=mock_engine),
            patch.object(
                source,
                "_check_vsql_prerequisites",
                return_value=VSqlPrerequisiteCheckResult(
                    is_accessible=True, message="V$SQL accessible"
                ),
            ),
            patch.object(
                source, "_extract_queries_from_vsql", return_value=iter(mock_queries)
            ),
        ):
            source._populate_aggregator_from_queries()

        assert source.report.num_queries_extracted == 1
        mock_engine.dispose.assert_called_once()

    def test_vsql_query_format(self):
        """Test that VSQL_USAGE_QUERY constant is properly formatted."""
        assert "SELECT" in VSQL_USAGE_QUERY
        assert "sql_id" in VSQL_USAGE_QUERY
        assert "sql_text" in VSQL_USAGE_QUERY
        assert "parsing_schema_name" in VSQL_USAGE_QUERY
        assert "V$SQL" in VSQL_USAGE_QUERY
        assert "command_type IN" in VSQL_USAGE_QUERY
        assert "ROWNUM" in VSQL_USAGE_QUERY
        assert ":max_queries" in VSQL_USAGE_QUERY

    def test_query_usage_integration_flow(self, ctx):
        """Test end-to-end flow of query usage extraction."""
        config = OracleConfig.model_validate(
            {
                "username": "test_user",
                "password": "test_pass",
                "host_port": "localhost:1521",
                "service_name": "test_service",
                "include_query_usage": True,
                "max_queries_to_extract": 10,
            }
        )
        source = OracleSource(config, ctx)

        mock_engine = Mock()
        mock_queries = [
            ObservedQuery(
                query="SELECT * FROM hr_schema.employees",
                default_schema="HR_SCHEMA",
                default_db="TESTDB",
            )
        ]

        with (
            patch.object(source, "_get_metadata_engine", return_value=mock_engine),
            patch.object(
                source,
                "_check_vsql_prerequisites",
                return_value=VSqlPrerequisiteCheckResult(
                    is_accessible=True, message="Accessible"
                ),
            ),
            patch.object(
                source, "_extract_queries_from_vsql", return_value=iter(mock_queries)
            ),
        ):
            source._populate_aggregator_from_queries()

        assert source.report.num_queries_extracted == 1
        mock_engine.dispose.assert_called_once()


class TestOracleProcedureLineage:
    """Tests for stored procedure dependency extraction."""

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test")

    @pytest.fixture
    def config_with_procedures(self):
        """Config with stored procedures enabled."""
        return OracleConfig.model_validate(
            {
                "username": "test_user",
                "password": "test_pass",
                "host_port": "localhost:1521",
                "service_name": "test_service",
                "include_stored_procedures": True,
            }
        )

    def test_get_procedure_dependencies_returns_upstream_tables(
        self, config_with_procedures, ctx
    ):
        """Test that _get_procedure_dependencies returns structured upstream_tables."""
        source = OracleSource(config_with_procedures, ctx)

        mock_conn = Mock()

        mock_upstream_result = Mock()
        mock_upstream_result.__iter__ = Mock(
            return_value=iter(
                [
                    Mock(
                        referenced_owner="HR_SCHEMA",
                        referenced_name="EMPLOYEES",
                        referenced_type="TABLE",
                    ),
                    Mock(
                        referenced_owner="HR_SCHEMA",
                        referenced_name="DEPARTMENTS",
                        referenced_type="VIEW",
                    ),
                    Mock(
                        referenced_owner="HR_SCHEMA",
                        referenced_name="OTHER_PROC",
                        referenced_type="PROCEDURE",
                    ),
                ]
            )
        )

        mock_downstream_result = Mock()
        mock_downstream_result.__iter__ = Mock(return_value=iter([]))

        mock_conn.execute = Mock(
            side_effect=[mock_upstream_result, mock_downstream_result]
        )

        dependencies = source._get_procedure_dependencies(
            conn=mock_conn,
            schema="HR_SCHEMA",
            procedure_name="PROCESS_EMPLOYEES",
            tables_prefix="ALL",
        )

        assert dependencies is not None
        assert dependencies.upstream_tables is not None
        assert len(dependencies.upstream_tables) == 2
        assert dependencies.upstream_tables[0].schema_name == "HR_SCHEMA"
        assert dependencies.upstream_tables[0].table == "EMPLOYEES"
        assert dependencies.upstream_tables[0].type == OracleObjectType.TABLE
        assert dependencies.upstream_tables[1].type == OracleObjectType.VIEW


def test_extra_oracle_types_registered_during_workunits_iteration():
    """Regression: ischema_names patch must stay active while get_workunits is iterated."""
    registered_during_iteration: list[bool] = []

    def mock_parent_workunits():
        for type_cls in extra_oracle_types:
            registered_during_iteration.append(type_cls.__name__ in ischema_names)
        return
        yield  # make it a generator

    config = OracleConfig(
        username="user",
        password="password",
        host_port="host:1521",
        service_name="svc01",
    )

    with patch("datahub.ingestion.source.sql.oracle.oracledb"):
        source = OracleSource(config, PipelineContext("test-extra-types"))
        with patch(
            "datahub.ingestion.source.sql.sql_common.SQLAlchemySource.get_workunits",
            return_value=mock_parent_workunits(),
        ):
            list(source.get_workunits())

    assert all(registered_during_iteration)
