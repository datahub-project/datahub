"""Unit tests for Pinecone source."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.pinecone.config import PineconeConfig
from datahub.ingestion.source.pinecone.pinecone_client import (
    IndexInfo,
    NamespaceStats,
    PineconeClient,
    VectorRecord,
)
from datahub.ingestion.source.pinecone.pinecone_source import PineconeSource
from datahub.ingestion.source.pinecone.schema_inference import MetadataSchemaInferrer


class TestPineconeConfig:
    """Test Pinecone configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = PineconeConfig(api_key="test-key")

        assert config.api_key.get_secret_value() == "test-key"
        assert config.environment is None
        assert config.enable_schema_inference is True
        assert config.schema_sampling_size == 100
        assert config.max_metadata_fields == 100
        assert config.max_workers == 5

    def test_config_with_patterns(self):
        """Test configuration with filtering patterns."""
        config = PineconeConfig(
            api_key="test-key",
            index_pattern={"allow": ["prod-.*"], "deny": [".*-test"]},
            namespace_pattern={"allow": ["customer-.*"]},
        )

        assert config.index_pattern.allowed("prod-index")
        assert not config.index_pattern.allowed("dev-test")
        assert config.namespace_pattern.allowed("customer-123")
        assert not config.namespace_pattern.allowed("internal-data")


class TestPineconeClient:
    """Test Pinecone client wrapper."""

    @patch("datahub.ingestion.source.pinecone.pinecone_client.Pinecone")
    def test_client_initialization(self, mock_pinecone):
        """Test client initialization with API key."""
        config = PineconeConfig(api_key="test-key")
        PineconeClient(config)

        mock_pinecone.assert_called_once_with(api_key="test-key")

    @patch("datahub.ingestion.source.pinecone.pinecone_client.Pinecone")
    def test_client_initialization_with_environment(self, mock_pinecone):
        """Test client initialization with environment."""
        config = PineconeConfig(api_key="test-key", environment="us-west1-gcp")
        PineconeClient(config)

        mock_pinecone.assert_called_once_with(
            api_key="test-key", environment="us-west1-gcp"
        )

    @patch("datahub.ingestion.source.pinecone.pinecone_client.Pinecone")
    def test_list_indexes(self, mock_pinecone):
        """Test listing indexes."""
        # Setup mock
        mock_pc_instance = MagicMock()
        mock_pinecone.return_value = mock_pc_instance

        mock_pc_instance.list_indexes.return_value = [
            {"name": "test-index-1"},
            {"name": "test-index-2"},
        ]

        mock_pc_instance.describe_index.side_effect = [
            {
                "name": "test-index-1",
                "dimension": 384,
                "metric": "cosine",
                "host": "test-1.pinecone.io",
                "status": {"state": "Ready"},
                "spec": {"serverless": {"cloud": "aws", "region": "us-east-1"}},
            },
            {
                "name": "test-index-2",
                "dimension": 768,
                "metric": "euclidean",
                "host": "test-2.pinecone.io",
                "status": {"state": "Ready"},
                "spec": {"pod": {"pod_type": "p1.x1", "replicas": 1}},
            },
        ]

        # Test
        config = PineconeConfig(api_key="test-key")
        client = PineconeClient(config)
        indexes = client.list_indexes()

        assert len(indexes) == 2
        assert indexes[0].name == "test-index-1"
        assert indexes[0].dimension == 384
        assert indexes[0].metric == "cosine"
        assert indexes[1].name == "test-index-2"
        assert indexes[1].dimension == 768


class TestPineconeSource:
    """Test Pinecone source."""

    def test_source_initialization(self):
        """Test source initialization."""
        config = PineconeConfig(api_key="test-key")
        ctx = PipelineContext(run_id="test-run")

        source = PineconeSource(config, ctx)

        assert source.config == config
        assert source.report is not None

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_get_workunits_with_no_indexes(self, mock_client_class):
        """Test workunit generation with no indexes."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.list_indexes.return_value = []

        # Test
        config = PineconeConfig(api_key="test-key")
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        workunits = list(source.get_workunits_internal())

        assert len(workunits) == 0
        assert source.report.indexes_scanned == 0

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_get_workunits_with_filtered_index(self, mock_client_class):
        """Test workunit generation with filtered indexes."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_client.list_indexes.return_value = [
            IndexInfo(
                name="test-index",
                dimension=384,
                metric="cosine",
                host="test.pinecone.io",
                status="Ready",
                spec={"serverless": {}},
            )
        ]

        # Test with deny pattern
        config = PineconeConfig(
            api_key="test-key",
            index_pattern={"deny": ["test-.*"]},
        )
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        list(source.get_workunits_internal())

        assert source.report.indexes_filtered == 1
        assert source.report.indexes_scanned == 0

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_get_workunits_basic_flow(self, mock_client_class):
        """Test basic workunit generation flow."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock index
        test_index = IndexInfo(
            name="test-index",
            dimension=384,
            metric="cosine",
            host="test.pinecone.io",
            status="Ready",
            spec={"serverless": {"cloud": "aws", "region": "us-east-1"}},
        )

        mock_client.list_indexes.return_value = [test_index]

        # Mock namespace
        mock_client.list_namespaces.return_value = [
            NamespaceStats(name="default", vector_count=1000)
        ]

        # Test
        config = PineconeConfig(api_key="test-key")
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        workunits = list(source.get_workunits_internal())

        # Should generate workunits for:
        # - Index container
        # - Namespace container
        # - Dataset
        assert len(workunits) > 0
        assert source.report.indexes_scanned == 1
        assert source.report.namespaces_scanned == 1
        assert source.report.datasets_generated == 1


if __name__ == "__main__":
    pytest.main([__file__])


class TestMetadataSchemaInferrer:
    """Test schema inference from vector metadata."""

    def test_infer_field_type(self):
        """Test field type inference."""
        inferrer = MetadataSchemaInferrer()

        assert inferrer._infer_field_type("test") == "string"
        assert inferrer._infer_field_type(123) == "number"
        assert inferrer._infer_field_type(45.67) == "number"
        assert inferrer._infer_field_type(True) == "boolean"
        assert inferrer._infer_field_type(False) == "boolean"
        assert inferrer._infer_field_type([1, 2, 3]) == "array"
        assert inferrer._infer_field_type({"key": "value"}) == "object"
        assert inferrer._infer_field_type(None) == "null"

    def test_select_primary_type(self):
        """Test primary type selection."""
        inferrer = MetadataSchemaInferrer()

        # String has highest priority
        assert inferrer._select_primary_type({"string", "number"}) == "string"
        assert inferrer._select_primary_type({"number", "boolean"}) == "number"
        assert inferrer._select_primary_type({"boolean", "array"}) == "boolean"
        assert inferrer._select_primary_type({"array", "null"}) == "array"

    def test_infer_schema_no_vectors(self):
        """Test schema inference with no vectors."""
        inferrer = MetadataSchemaInferrer()

        schema = inferrer.infer_schema([], "test-dataset")
        assert schema is None

    def test_infer_schema_no_metadata(self):
        """Test schema inference with vectors but no metadata."""
        inferrer = MetadataSchemaInferrer()

        vectors = [
            VectorRecord(id="1", values=[0.1, 0.2], metadata=None),
            VectorRecord(id="2", values=[0.3, 0.4], metadata={}),
        ]

        schema = inferrer.infer_schema(vectors, "test-dataset")
        assert schema is None

    def test_infer_schema_basic(self):
        """Test basic schema inference."""
        inferrer = MetadataSchemaInferrer()

        vectors = [
            VectorRecord(
                id="1",
                values=[0.1, 0.2],
                metadata={"category": "A", "score": 0.95, "active": True},
            ),
            VectorRecord(
                id="2",
                values=[0.3, 0.4],
                metadata={"category": "B", "score": 0.87, "active": False},
            ),
            VectorRecord(
                id="3",
                values=[0.5, 0.6],
                metadata={"category": "A", "score": 0.92},
            ),
        ]

        schema = inferrer.infer_schema(vectors, "test-dataset")

        assert schema is not None
        assert schema.schemaName == "test-dataset"
        assert len(schema.fields) == 3

        # Check field names
        field_names = {f.fieldPath.split(".")[-1] for f in schema.fields}
        assert field_names == {"category", "score", "active"}

    def test_infer_schema_with_arrays(self):
        """Test schema inference with array fields."""
        inferrer = MetadataSchemaInferrer()

        vectors = [
            VectorRecord(
                id="1",
                values=[0.1, 0.2],
                metadata={"tags": ["tag1", "tag2"], "name": "test"},
            ),
            VectorRecord(
                id="2",
                values=[0.3, 0.4],
                metadata={"tags": ["tag3"], "name": "test2"},
            ),
        ]

        schema = inferrer.infer_schema(vectors, "test-dataset")

        assert schema is not None
        assert len(schema.fields) == 2

    def test_infer_schema_max_fields_limit(self):
        """Test that max_fields limit is respected."""
        inferrer = MetadataSchemaInferrer(max_fields=2)

        vectors = [
            VectorRecord(
                id="1",
                values=[0.1, 0.2],
                metadata={
                    "field1": "a",
                    "field2": "b",
                    "field3": "c",
                    "field4": "d",
                },
            ),
        ]

        schema = inferrer.infer_schema(vectors, "test-dataset")

        assert schema is not None
        assert len(schema.fields) <= 2

    def test_infer_schema_mixed_types(self):
        """Test schema inference with mixed types for same field."""
        inferrer = MetadataSchemaInferrer()

        vectors = [
            VectorRecord(
                id="1",
                values=[0.1, 0.2],
                metadata={"value": "string"},
            ),
            VectorRecord(
                id="2",
                values=[0.3, 0.4],
                metadata={"value": 123},
            ),
            VectorRecord(
                id="3",
                values=[0.5, 0.6],
                metadata={"value": True},
            ),
        ]

        schema = inferrer.infer_schema(vectors, "test-dataset")

        assert schema is not None
        assert len(schema.fields) == 1
        # String should be selected as primary type
        field = schema.fields[0]
        assert field.nativeDataType == "string"


class TestPineconeSourceWithSchemaInference:
    """Test Pinecone source with schema inference."""

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_schema_inference_enabled(self, mock_client_class):
        """Test that schema inference is enabled by default."""
        config = PineconeConfig(api_key="test-key")
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        assert source.schema_inferrer is not None
        assert config.enable_schema_inference is True

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_schema_inference_disabled(self, mock_client_class):
        """Test that schema inference can be disabled."""
        config = PineconeConfig(api_key="test-key", enable_schema_inference=False)
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        assert source.schema_inferrer is None

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_get_workunits_with_schema_inference(self, mock_client_class):
        """Test workunit generation with schema inference."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock index
        test_index = IndexInfo(
            name="test-index",
            dimension=384,
            metric="cosine",
            host="test.pinecone.io",
            status="Ready",
            spec={"serverless": {"cloud": "aws", "region": "us-east-1"}},
        )

        mock_client.list_indexes.return_value = [test_index]

        # Mock namespace
        mock_client.list_namespaces.return_value = [
            NamespaceStats(name="default", vector_count=1000)
        ]

        # Mock sample vectors with metadata
        mock_client.sample_vectors.return_value = [
            VectorRecord(
                id="vec1",
                values=[0.1] * 384,
                metadata={"category": "A", "score": 0.95, "active": True},
            ),
            VectorRecord(
                id="vec2",
                values=[0.2] * 384,
                metadata={"category": "B", "score": 0.87, "active": False},
            ),
        ]

        # Test
        config = PineconeConfig(
            api_key="test-key",
            enable_schema_inference=True,
            schema_sampling_size=100,
        )
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        workunits = list(source.get_workunits_internal())

        # Should generate workunits including schema
        assert len(workunits) > 0
        assert source.report.indexes_scanned == 1
        assert source.report.namespaces_scanned == 1
        assert source.report.datasets_generated == 1

        # Verify sample_vectors was called
        mock_client.sample_vectors.assert_called_once_with(
            index_name="test-index",
            namespace="default",
            limit=100,
        )

    @patch("datahub.ingestion.source.pinecone.pinecone_source.PineconeClient")
    def test_schema_inference_no_metadata(self, mock_client_class):
        """Test schema inference when vectors have no metadata."""
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        test_index = IndexInfo(
            name="test-index",
            dimension=384,
            metric="cosine",
            host="test.pinecone.io",
            status="Ready",
            spec={"serverless": {}},
        )

        mock_client.list_indexes.return_value = [test_index]
        mock_client.list_namespaces.return_value = [
            NamespaceStats(name="default", vector_count=100)
        ]

        # Mock vectors without metadata
        mock_client.sample_vectors.return_value = [
            VectorRecord(id="vec1", values=[0.1] * 384, metadata=None),
            VectorRecord(id="vec2", values=[0.2] * 384, metadata={}),
        ]

        # Test
        config = PineconeConfig(api_key="test-key", enable_schema_inference=True)
        ctx = PipelineContext(run_id="test-run")
        source = PineconeSource(config, ctx)

        workunits = list(source.get_workunits_internal())

        # Should still generate workunits, just without schema
        assert len(workunits) > 0
        assert source.report.datasets_generated == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
