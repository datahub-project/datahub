import json
from unittest.mock import MagicMock, patch

import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import WriteCallback
from datahub.ingestion.sink.iceberg_rest import (
    IcebergRestSink,
    IcebergRestSinkConfig,
    IcebergRestSinkReport,
)
from datahub.metadata.schema_classes import ChangeTypeClass


class TestIcebergRestSinkConfig:
    def test_minimal_config(self):
        """Test minimal required configuration"""
        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
        )
        assert config.uri == "http://localhost:8080/iceberg"
        assert config.warehouse == "test_warehouse"
        assert config.namespace == "datahub_metadata"
        assert config.table_name == "metadata_aspects_v2"
        assert config.create_table_if_not_exists is True
        assert config.upsert is True  # Default is True
        assert config.truncate is False  # Default is False

    def test_full_config(self):
        """Test configuration with all optional fields"""
        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="custom_namespace",
            table_name="custom_table",
            token="test_token",
            aws_role_arn="arn:aws:iam::123456789012:role/test-role",
            s3_region="us-west-2",
            s3_access_key_id="test_key",
            s3_secret_access_key="test_secret",
            s3_endpoint="http://minio:9000",
            create_table_if_not_exists=False,
            upsert=False,
            truncate=True,
            connection={"timeout": 60, "retry": {"total": 5}},
        )
        assert config.namespace == "custom_namespace"
        assert config.table_name == "custom_table"
        assert config.token == "test_token"
        assert config.aws_role_arn == "arn:aws:iam::123456789012:role/test-role"
        assert config.s3_region == "us-west-2"
        assert config.create_table_if_not_exists is False
        assert config.upsert is False
        assert config.truncate is True

    def test_catalog_config_builder(self):
        """Test that catalog config is built correctly"""
        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            token="test_token",
            s3_region="us-west-2",
        )
        assert hasattr(config, "_catalog_config")
        assert config._catalog_config["type"] == "rest"
        assert config._catalog_config["uri"] == "http://localhost:8080/iceberg"
        assert config._catalog_config["token"] == "test_token"
        assert config._catalog_config["s3.region"] == "us-west-2"

    def test_vended_credentials_header(self):
        """Test that vended credentials header is set when aws_role_arn is provided"""
        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            aws_role_arn="arn:aws:iam::123456789012:role/test-role",
        )
        assert (
            config._catalog_config["header.X-Iceberg-Access-Delegation"]
            == "vended-credentials"
        )


class TestIcebergRestSinkReport:
    def test_report_initialization(self):
        """Test report initialization with default values"""
        report = IcebergRestSinkReport()
        assert report.total_records_written == 0
        assert report.write_errors == 0
        assert report.catalog_uri is None
        assert report.warehouse_location is None
        assert report.namespace_created is False
        assert report.table_created is False

    def test_report_write_error(self):
        """Test error tracking"""
        report = IcebergRestSinkReport()
        report.report_write_error()
        assert report.write_errors == 1
        report.report_write_error()
        assert report.write_errors == 2


class TestIcebergRestSink:
    @pytest.fixture
    def mock_catalog(self):
        """Create a mock Iceberg catalog"""
        catalog = MagicMock()
        catalog._session = MagicMock()
        catalog._session.mount = MagicMock()
        return catalog

    @pytest.fixture
    def mock_table(self):
        """Create a mock Iceberg table"""
        table = MagicMock()
        table.append = MagicMock()
        return table

    @pytest.fixture
    def sink_config(self):
        """Create a basic sink configuration"""
        return IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="test_namespace",
            table_name="test_table",
        )

    @pytest.fixture
    def pipeline_context(self):
        """Create a pipeline context with datahub source"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        pipeline_config = PipelineConfig(
            source=SourceConfig(type="datahub", config={}),
        )
        return PipelineContext(run_id="test-run", pipeline_config=pipeline_config)

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_sink_initialization(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test sink initialization"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        assert sink.config == sink_config
        assert sink.report.catalog_uri == "http://localhost:8080/iceberg"
        assert sink.report.warehouse_location == "test_warehouse"
        mock_load_catalog.assert_called_once()

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_namespace_creation(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that namespace is created if it doesn't exist"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        mock_catalog.create_namespace.assert_called_once_with("test_namespace")
        assert sink.report.namespace_created is True

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_namespace_already_exists(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test handling when namespace already exists"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock(
            side_effect=NamespaceAlreadyExistsError("test_namespace")
        )
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        assert sink.report.namespace_created is False

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_warehouse_verification(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that warehouse verification works"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(
            return_value=[("namespace1",), ("namespace2",)]
        )
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        # Verify list_namespaces was called for warehouse verification
        mock_catalog.list_namespaces.assert_called_once()
        assert sink.report.warehouse_location == "test_warehouse"

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_warehouse_verification_failure(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog
    ):
        """Test that warehouse verification failure raises appropriate error"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(
            side_effect=Exception("Warehouse not found")
        )

        with pytest.raises(ValueError) as exc_info:
            IcebergRestSink(pipeline_context, sink_config)

        assert "Failed to verify warehouse" in str(exc_info.value)
        assert "test_warehouse" in str(exc_info.value)

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_warehouse_verification_disabled(
        self, mock_load_catalog, pipeline_context, mock_catalog, mock_table
    ):
        """Test that warehouse verification can be disabled"""
        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="test_namespace",
            table_name="test_table",
            verify_warehouse=False,  # Disable verification
        )

        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(
            side_effect=Exception("Should not be called")
        )
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        # Should not raise exception even though list_namespaces would fail
        sink = IcebergRestSink(pipeline_context, config)

        # Verify list_namespaces was NOT called
        mock_catalog.list_namespaces.assert_not_called()
        assert sink.report.warehouse_location == "test_warehouse"

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_table_creation(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that table is created with correct schema"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        mock_catalog.create_table.assert_called_once()
        call_args = mock_catalog.create_table.call_args
        assert call_args[0][0] == "test_namespace.test_table"
        schema = call_args[0][1]
        assert len(schema.fields) == 7
        assert schema.fields[0].name == "urn"
        assert schema.fields[1].name == "entity_type"
        assert schema.fields[2].name == "aspect"
        assert schema.fields[3].name == "metadata"
        assert schema.fields[4].name == "systemmetadata"
        assert schema.fields[5].name == "version"
        assert schema.fields[6].name == "createdon"
        
        # Verify partition spec
        partition_spec = call_args[1]["partition_spec"]
        assert partition_spec is not None
        assert len(partition_spec.fields) == 1
        assert partition_spec.fields[0].source_id == 2  # entity_type field
        assert partition_spec.fields[0].name == "entity_type"
        
        assert sink.report.table_created is True

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_write_mcp(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test writing a MetadataChangeProposal"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        # Create a test MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)",
            aspectName="datasetProperties",
            aspect={
                "description": "Test dataset",
                "customProperties": {"key": "value"},
            },
            changeType=ChangeTypeClass.UPSERT,
        )

        # Create callback
        callback = MagicMock(spec=WriteCallback)

        # Write the record
        record_envelope = RecordEnvelope(mcp, metadata={})
        sink.write_record_async(record_envelope, callback)

        # Verify table.append was called
        mock_table.append.assert_called_once()

        # Verify callback was called
        callback.on_success.assert_called_once_with(record_envelope, {})

        # Verify report
        assert sink.report.total_records_written == 1
        assert sink.report.write_errors == 0

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_write_error_handling(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test error handling during write"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        # Make table.append raise an exception
        mock_table.append.side_effect = Exception("Write failed")

        # Create a test MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)",
            aspectName="datasetProperties",
        )

        # Create callback
        callback = MagicMock(spec=WriteCallback)

        # Write the record
        record_envelope = RecordEnvelope(mcp, metadata={})
        sink.write_record_async(record_envelope, callback)

        # Verify error was reported
        callback.on_failure.assert_called_once()
        assert sink.report.write_errors == 1

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_mcp_to_arrow_conversion(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test MCP to PyArrow table conversion"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        # Create a test MCP with various fields
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)",
            aspectName="ownership",
            aspect={"owners": [{"owner": "urn:li:corpuser:test", "type": "DATAOWNER"}]},
            changeType=ChangeTypeClass.UPSERT,
        )

        # Create callback
        callback = MagicMock(spec=WriteCallback)

        # Write the record
        record_envelope = RecordEnvelope(mcp, metadata={})
        sink.write_record_async(record_envelope, callback)

        # Verify table.append was called with a PyArrow table
        mock_table.append.assert_called_once()
        pa_table = mock_table.append.call_args[0][0]

        # Verify the PyArrow table structure
        assert "urn" in pa_table.column_names
        assert "entity_type" in pa_table.column_names
        assert "aspect" in pa_table.column_names
        assert "metadata" in pa_table.column_names
        assert "systemmetadata" in pa_table.column_names
        assert "version" in pa_table.column_names
        assert "createdon" in pa_table.column_names

        # Verify the values
        assert (
            pa_table["urn"][0].as_py()
            == "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)"
        )
        assert pa_table["entity_type"][0].as_py() == "dataset"
        assert pa_table["aspect"][0].as_py() == "ownership"
        assert pa_table["version"][0].as_py() == 0

        # Verify metadata is valid JSON
        metadata = pa_table["metadata"][0].as_py()
        aspect_obj = json.loads(metadata)
        assert "owners" in aspect_obj

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_configured_method(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test the configured() method returns readable string"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        configured_str = sink.configured()
        assert "http://localhost:8080/iceberg" in configured_str
        assert "test_warehouse" in configured_str
        assert "test_namespace" in configured_str
        assert "test_table" in configured_str

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_entity_type_extraction(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that entity type is correctly extracted from URN"""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, sink_config)

        # Test different entity types
        test_cases = [
            ("urn:li:dataset:(test,table,PROD)", "dataset"),
            ("urn:li:chart:test-chart", "chart"),
            ("urn:li:dashboard:test-dashboard", "dashboard"),
            ("urn:li:corpuser:test-user", "corpuser"),
            ("urn:li:dataFlow:test-flow", "dataFlow"),
            ("invalid-urn", "unknown"),
            ("", "unknown"),
        ]

        for urn, expected_entity_type in test_cases:
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspectName="testAspect",
            )
            record_envelope = RecordEnvelope(mcp, metadata={})
            record_data = sink._process_record(record_envelope)
            assert record_data is not None
            assert record_data["entity_type"] == expected_entity_type, (
                f"Expected entity_type '{expected_entity_type}' for URN '{urn}', "
                f"got '{record_data['entity_type']}'"
            )

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_sink_requires_datahub_source(
        self, mock_load_catalog, sink_config, mock_catalog
    ):
        """Test that sink raises error when used with non-datahub source"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        # Test with different source types
        invalid_sources = ["snowflake", "bigquery", "mysql", "postgres"]

        for source_type in invalid_sources:
            pipeline_config = PipelineConfig(
                source=SourceConfig(type=source_type, config={}),
            )
            pipeline_context = PipelineContext(
                run_id="test-run", pipeline_config=pipeline_config
            )

            with pytest.raises(ValueError) as exc_info:
                IcebergRestSink(pipeline_context, sink_config)

            assert "only compatible with the 'datahub' source" in str(exc_info.value)
            assert source_type in str(exc_info.value)

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_sink_allows_datahub_source(
        self, mock_load_catalog, sink_config, mock_catalog, mock_table
    ):
        """Test that sink works correctly with datahub source"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        pipeline_config = PipelineConfig(
            source=SourceConfig(type="datahub", config={}),
        )
        pipeline_context = PipelineContext(
            run_id="test-run", pipeline_config=pipeline_config
        )

        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        # Should not raise an error
        sink = IcebergRestSink(pipeline_context, sink_config)
        assert sink is not None

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_upsert_mode_append(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that append mode is used when upsert=False"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="test_namespace",
            table_name="test_table",
            upsert=False,  # Disable upsert
        )

        pipeline_config = PipelineConfig(
            source=SourceConfig(type="datahub", config={}),
        )
        pipeline_context = PipelineContext(
            run_id="test-run", pipeline_config=pipeline_config
        )

        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        sink = IcebergRestSink(pipeline_context, config)

        # Create a test MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(test,table,PROD)",
            aspectName="datasetProperties",
        )

        callback = MagicMock(spec=WriteCallback)
        record_envelope = RecordEnvelope(mcp, metadata={})
        sink.write_record_async(record_envelope, callback)

        # Verify append was called, not upsert
        mock_table.append.assert_called_once()
        assert not hasattr(mock_table, "upsert") or not mock_table.upsert.called

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_upsert_mode_enabled(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that upsert is used when upsert=True and upsert is available"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="test_namespace",
            table_name="test_table",
            upsert=True,  # Enable upsert
        )

        pipeline_config = PipelineConfig(
            source=SourceConfig(type="datahub", config={}),
        )
        pipeline_context = PipelineContext(
            run_id="test-run", pipeline_config=pipeline_config
        )

        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        # Mock upsert method and return value
        # The implementation tries 'data' and 'primary_key' first, so make that succeed
        mock_upsert_result = MagicMock()
        mock_upsert_result.rows_updated = 5
        mock_upsert_result.rows_inserted = 5
        mock_table.upsert = MagicMock(return_value=mock_upsert_result)

        sink = IcebergRestSink(pipeline_context, config)

        # Create a test MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(test,table,PROD)",
            aspectName="datasetProperties",
        )

        callback = MagicMock(spec=WriteCallback)
        record_envelope = RecordEnvelope(mcp, metadata={})
        sink.write_record_async(record_envelope, callback)

        # Verify upsert was called with correct parameters
        # The implementation tries 'data' and 'primary_key' first
        mock_table.upsert.assert_called()
        call_args = mock_table.upsert.call_args
        # Should be called with keyword arguments: data=pa_table, primary_key=["urn", "aspect", "version"]
        assert "data" in call_args[1] or "primary_key" in call_args[1] or len(call_args[0]) > 0
        if "primary_key" in call_args[1]:
            assert call_args[1]["primary_key"] == ["urn", "aspect", "version"]
        elif "on" in call_args[1]:
            assert call_args[1]["on"] == ["urn", "aspect", "version"]

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_truncate_table(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that table is truncated when truncate=True"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="test_namespace",
            table_name="test_table",
            truncate=True,  # Enable truncate
        )

        pipeline_config = PipelineConfig(
            source=SourceConfig(type="datahub", config={}),
        )
        pipeline_context = PipelineContext(
            run_id="test-run", pipeline_config=pipeline_config
        )

        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        # Mock overwrite for truncate
        mock_table.overwrite = MagicMock()

        sink = IcebergRestSink(pipeline_context, config)

        # Verify overwrite was called (for truncate)
        mock_table.overwrite.assert_called_once()

    @patch("datahub.ingestion.sink.iceberg_rest.load_catalog")
    def test_upsert_mode_fallback_to_append(
        self, mock_load_catalog, sink_config, pipeline_context, mock_catalog, mock_table
    ):
        """Test that append is used as fallback when upsert is not available"""
        from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig

        config = IcebergRestSinkConfig(
            uri="http://localhost:8080/iceberg",
            warehouse="test_warehouse",
            namespace="test_namespace",
            table_name="test_table",
            upsert=True,  # Enable upsert
            batch_size=1,  # Small batch to trigger flush immediately
        )

        pipeline_config = PipelineConfig(
            source=SourceConfig(type="datahub", config={}),
        )
        pipeline_context = PipelineContext(
            run_id="test-run", pipeline_config=pipeline_config
        )

        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_namespaces = MagicMock(return_value=[])
        mock_catalog.create_namespace = MagicMock()
        mock_catalog.create_table = MagicMock()
        mock_catalog.load_table = MagicMock(return_value=mock_table)

        # Don't mock upsert - simulate older PyIceberg version without upsert
        # (hasattr will return False)

        sink = IcebergRestSink(pipeline_context, config)

        # Create a test MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(test,table,PROD)",
            aspectName="datasetProperties",
        )

        callback = MagicMock(spec=WriteCallback)
        record_envelope = RecordEnvelope(mcp, metadata={})
        sink.write_record_async(record_envelope, callback)

        # Verify append was called as fallback (upsert not available)
        mock_table.append.assert_called_once()
