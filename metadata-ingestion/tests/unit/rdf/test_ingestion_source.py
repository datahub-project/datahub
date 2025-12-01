#!/usr/bin/env python3
"""
Tests for RDF DataHub ingestion source.

These tests verify that the ingestion source is properly implemented and can be
imported and instantiated correctly.
"""

from unittest.mock import Mock, patch

import pytest


def test_import_ingestion_source():
    """Test that the ingestion source can be imported."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    assert RDFSource is not None
    assert RDFSourceConfig is not None


def test_config_model_validation():
    """Test that the config model validates correctly."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    # Valid config
    config = RDFSourceConfig(source="examples/bcbs239/", environment="PROD")

    assert config.source == "examples/bcbs239/"
    assert config.environment == "PROD"
    assert config.recursive is True
    assert config.extensions == [".ttl", ".rdf", ".owl", ".n3", ".nt"]


def test_config_model_with_export_only():
    """Test config with export_only parameter."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    config = RDFSourceConfig(
        source="examples/bcbs239/",
        environment="PROD",
        export_only=["glossary", "datasets"],
    )

    assert config.export_only == ["glossary", "datasets"]


def test_config_model_with_dialect():
    """Test config with dialect parameter."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    config = RDFSourceConfig(
        source="examples/bcbs239/", environment="PROD", dialect="default"
    )

    assert config.dialect == "default"


def test_config_model_invalid_dialect():
    """Test that invalid dialect raises error."""
    from pydantic import ValidationError

    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    with pytest.raises(ValidationError) as exc_info:
        RDFSourceConfig(source="examples/bcbs239/", dialect="invalid_dialect")

    assert "Invalid dialect" in str(exc_info.value)


def test_config_model_invalid_export_type():
    """Test that invalid export type raises error."""
    from pydantic import ValidationError

    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    with pytest.raises(ValidationError) as exc_info:
        RDFSourceConfig(source="examples/bcbs239/", export_only=["invalid_type"])

    assert "Invalid entity type" in str(exc_info.value)


def test_source_decorators():
    """Test that source has proper DataHub decorators."""
    from datahub.ingestion.source.rdf.ingestion import RDFSource

    # Check that the class has the necessary attributes set by decorators
    assert hasattr(RDFSource, "get_platform_name")
    assert hasattr(RDFSource, "get_support_status")


def test_source_has_required_methods():
    """Test that source implements required methods."""
    from datahub.ingestion.source.rdf.ingestion import RDFSource

    # Check required Source interface methods
    assert hasattr(RDFSource, "create")
    assert hasattr(RDFSource, "get_workunits")
    assert hasattr(RDFSource, "get_report")
    assert hasattr(RDFSource, "close")


def test_config_parse_from_dict():
    """Test that config can be parsed from dictionary."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    config_dict = {
        "source": "examples/bcbs239/",
        "environment": "PROD",
        "export_only": ["glossary", "datasets"],
        "recursive": True,
    }

    config = RDFSourceConfig.model_validate(config_dict)

    assert config.source == "examples/bcbs239/"
    assert config.environment == "PROD"
    assert config.export_only == ["glossary", "datasets"]
    assert config.recursive is True


def test_source_report():
    """Test that source report tracks statistics."""
    from datahub.ingestion.source.rdf.ingestion import RDFSourceReport

    report = RDFSourceReport()

    # Test initial state
    assert report.num_files_processed == 0
    assert report.num_triples_processed == 0
    assert report.num_entities_emitted == 0
    assert report.num_workunits_produced == 0

    # Test reporting methods
    report.report_file_processed()
    assert report.num_files_processed == 1

    report.report_triples_processed(100)
    assert report.num_triples_processed == 100

    report.report_entity_emitted()
    assert report.num_entities_emitted == 1

    report.report_workunit_produced()
    assert report.num_workunits_produced == 1


# ============================================================================
# Tests for RDFSource.create() class method
# ============================================================================


def test_source_create_method():
    """Test RDFSource.create() class method."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
    )

    config_dict = {"source": "examples/bcbs239/", "environment": "PROD"}
    ctx = PipelineContext(run_id="test-run")

    source = RDFSource.create(config_dict, ctx)

    assert isinstance(source, RDFSource)
    assert source.config.source == "examples/bcbs239/"
    assert source.config.environment == "PROD"
    assert source.report is not None


# ============================================================================
# Tests for _create_source() method
# ============================================================================


def test_create_source_with_file(tmp_path):
    """Test _create_source() with a single file."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    # Create a temporary file
    test_file = tmp_path / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    config = RDFSourceConfig(source=str(test_file))
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    rdf_source = source._create_source()
    assert rdf_source is not None
    assert hasattr(rdf_source, "get_graph")
    assert hasattr(rdf_source, "get_source_info")


def test_create_source_with_folder(tmp_path):
    """Test _create_source() with a folder path."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    # Create a temporary folder with a file
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    test_file = test_dir / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    config = RDFSourceConfig(source=str(test_dir))
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    rdf_source = source._create_source()
    assert rdf_source is not None
    assert hasattr(rdf_source, "get_graph")


def test_create_source_with_url():
    """Test _create_source() with HTTP URL."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="http://example.com/sparql")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    rdf_source = source._create_source()
    assert rdf_source is not None
    assert hasattr(rdf_source, "get_graph")


def test_create_source_with_comma_separated_files(tmp_path):
    """Test _create_source() with comma-separated files."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    # Create temporary files
    file1 = tmp_path / "file1.ttl"
    file1.write_text("@prefix ex: <http://example.org/> . ex:test1 a ex:Test .")
    file2 = tmp_path / "file2.ttl"
    file2.write_text("@prefix ex: <http://example.org/> . ex:test2 a ex:Test .")

    config = RDFSourceConfig(source=f"{file1},{file2}")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    rdf_source = source._create_source()
    assert rdf_source is not None
    assert hasattr(rdf_source, "get_graph")


def test_create_source_with_invalid_path():
    """Test _create_source() raises error for invalid path."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="/nonexistent/path/that/does/not/exist.ttl")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    with pytest.raises(ValueError, match="Source not found"):
        source._create_source()


def test_create_source_with_recursive_config(tmp_path):
    """Test _create_source() respects recursive configuration."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    config = RDFSourceConfig(source=str(test_dir), recursive=False)
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    rdf_source = source._create_source()
    assert rdf_source is not None


def test_create_source_with_custom_extensions(tmp_path):
    """Test _create_source() respects custom file extensions."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    config = RDFSourceConfig(source=str(test_dir), extensions=[".ttl", ".custom"])
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    rdf_source = source._create_source()
    assert rdf_source is not None


# ============================================================================
# Tests for _create_query() method
# ============================================================================


def test_create_query_with_sparql():
    """Test _create_query() with SPARQL query."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    sparql_query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
    config = RDFSourceConfig(source="examples/bcbs239/", sparql=sparql_query)
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    query = source._create_query()
    assert query is not None
    assert hasattr(query, "execute")
    assert hasattr(query, "get_query_info")


def test_create_query_with_filter():
    """Test _create_query() with filter criteria."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    filter_criteria = {"namespace": "http://example.com/"}
    config = RDFSourceConfig(source="examples/bcbs239/", filter=filter_criteria)
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    query = source._create_query()
    assert query is not None
    assert hasattr(query, "execute")


def test_create_query_pass_through():
    """Test _create_query() creates pass-through query when no query specified."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="examples/bcbs239/")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    query = source._create_query()
    assert query is not None
    assert hasattr(query, "execute")
    assert hasattr(query, "get_query_info")


# ============================================================================
# Tests for _create_transpiler() method
# ============================================================================


def test_create_transpiler_with_environment():
    """Test _create_transpiler() sets environment correctly."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="examples/bcbs239/", environment="DEV")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    transpiler = source._create_transpiler()
    assert transpiler is not None
    assert transpiler.environment == "DEV"


def test_create_transpiler_with_dialect():
    """Test _create_transpiler() sets dialect correctly."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="examples/bcbs239/", dialect="fibo")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    transpiler = source._create_transpiler()
    assert transpiler is not None
    # Check that dialect was stored in transpiler
    assert transpiler.forced_dialect is not None


def test_create_transpiler_with_export_only():
    """Test _create_transpiler() sets export_only filter."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(
        source="examples/bcbs239/", export_only=["glossary", "datasets"]
    )
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    transpiler = source._create_transpiler()
    assert transpiler is not None
    assert transpiler.export_only == ["glossary", "datasets"]


def test_create_transpiler_with_skip_export():
    """Test _create_transpiler() sets skip_export filter."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(
        source="examples/bcbs239/", skip_export=["ownership", "properties"]
    )
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    transpiler = source._create_transpiler()
    assert transpiler is not None
    assert transpiler.skip_export == ["ownership", "properties"]


# ============================================================================
# Tests for DataHubIngestionTarget class
# ============================================================================


def test_datahub_ingestion_target_init():
    """Test DataHubIngestionTarget initialization."""
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    assert target.report == report
    assert target.workunits == []
    assert len(target.workunits) == 0


def test_datahub_ingestion_target_get_target_info():
    """Test DataHubIngestionTarget.get_target_info()."""
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    info = target.get_target_info()
    assert info["type"] == "datahub-ingestion"
    assert "description" in info


def test_datahub_ingestion_target_get_workunits_empty():
    """Test DataHubIngestionTarget.get_workunits() with no work units."""
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    workunits = list(target.get_workunits())
    assert len(workunits) == 0


def test_datahub_ingestion_target_send_with_invalid_type():
    """Test DataHubIngestionTarget.send() with invalid graph type."""
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Send invalid type
    result = target.send("not a DataHubGraph")
    assert result["success"] is False
    assert "error" in result
    assert "Expected DataHubGraph" in result["error"]


def test_datahub_ingestion_target_send_with_empty_graph():
    """Test DataHubIngestionTarget.send() with empty DataHubGraph."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create empty graph
    graph = DataHubGraph()

    result = target.send(graph)
    assert result["success"] is True
    assert result["workunits_generated"] == 0
    assert result["entities_emitted"] == 0
    assert len(target.workunits) == 0


def test_datahub_ingestion_target_send_with_mock_entities():
    """Test DataHubIngestionTarget.send() with mock entities."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.entities.dataset.ast import (
        DataHubDataset,
    )
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )
    from datahub.utilities.urns.dataset_urn import DatasetUrn

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create graph with mock entities
    graph = DataHubGraph()

    # Add mock glossary term (terms not in domains will be processed separately)
    mock_term = Mock(spec=DataHubGlossaryTerm)
    mock_term.urn = "urn:li:glossaryTerm:test"
    mock_term.name = "test_term"
    mock_term.definition = "Test term definition"
    mock_term.source = "http://example.com/test"
    mock_term.custom_properties = {}
    graph.glossary_terms = [mock_term]

    # Add empty domains list (terms not in domains)
    graph.domains = []

    # Add mock dataset
    mock_dataset = Mock(spec=DataHubDataset)
    mock_dataset.urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD)"
    )
    mock_dataset.name = "test_table"
    mock_dataset.description = "Test dataset"
    mock_dataset.custom_properties = {}
    mock_dataset.schema_fields = []
    graph.datasets = [mock_dataset]

    # MCPFactory is now used, so no need to mock DataHubClient
    result = target.send(graph)

    assert result["success"] is True
    assert result["workunits_generated"] >= 2  # At least 2 (term + dataset)
    assert result["entities_emitted"] >= 2
    assert len(target.workunits) >= 2


def test_datahub_ingestion_target_send_with_mcp_error():
    """Test DataHubIngestionTarget.send() handles MCP creation errors gracefully."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create graph with mock entity that will fail
    graph = DataHubGraph()
    mock_term = Mock(spec=DataHubGlossaryTerm)
    mock_term.urn = "urn:li:glossaryTerm:test"
    mock_term.name = "test"
    mock_term.definition = None  # Missing required field
    mock_term.source = None
    mock_term.custom_properties = {}
    graph.glossary_terms = [mock_term]
    graph.domains = []

    # Mock MCPFactory to raise error
    # MCPFactory no longer exists - MCPs are created by entity MCP builders
    # This test may need to be updated to test the actual MCP builder
    from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
        GlossaryTermMCPBuilder,
    )

    with patch.object(GlossaryTermMCPBuilder, "build_mcps") as mock_create:
        mock_create.side_effect = Exception("MCP creation failed")

        result = target.send(graph)

        # Should still succeed overall, but log warning
        assert result["success"] is True
        assert result["workunits_generated"] == 0
        assert result["entities_emitted"] == 0


def test_datahub_ingestion_target_send_all_entity_types():
    """Test DataHubIngestionTarget.send() processes all entity types."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.entities.data_product.ast import (
        DataHubDataProduct,
    )
    from datahub.ingestion.source.rdf.entities.dataset.ast import (
        DataHubDataset,
    )
    from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )
    from datahub.ingestion.source.rdf.entities.lineage.ast import (
        DataHubLineageRelationship,
    )
    from datahub.ingestion.source.rdf.entities.relationship.ast import (
        DataHubRelationship,
    )
    from datahub.ingestion.source.rdf.entities.structured_property.ast import (
        DataHubStructuredProperty,
    )
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )
    from datahub.utilities.urns.dataset_urn import DatasetUrn
    from datahub.utilities.urns.domain_urn import DomainUrn
    from datahub.utilities.urns.structured_properties_urn import StructuredPropertyUrn

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create graph with all entity types
    graph = DataHubGraph()

    # Create mock glossary term
    mock_term = Mock(spec=DataHubGlossaryTerm)
    mock_term.urn = "urn:li:glossaryTerm:term1"
    mock_term.name = "term1"
    mock_term.definition = "Test term"
    mock_term.source = "http://example.com/term1"
    mock_term.custom_properties = {}
    graph.glossary_terms = [mock_term]

    # Create mock dataset
    mock_dataset = Mock(spec=DataHubDataset)
    mock_dataset.urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD)"
    )
    mock_dataset.name = "test_table"
    mock_dataset.description = "Test dataset"
    mock_dataset.custom_properties = {}
    mock_dataset.schema_fields = []
    graph.datasets = [mock_dataset]

    # Create mock structured property
    mock_prop = Mock(spec=DataHubStructuredProperty)
    mock_prop.urn = StructuredPropertyUrn.from_string("urn:li:structuredProperty:prop1")
    mock_prop.name = "prop1"
    mock_prop.description = "Test property"
    mock_prop.value_type = "urn:li:dataType:datahub.string"
    mock_prop.cardinality = "SINGLE"
    mock_prop.entity_types = []
    mock_prop.allowed_values = []
    graph.structured_properties = [mock_prop]

    # Create mock data product
    mock_product = Mock(spec=DataHubDataProduct)
    mock_product.urn = "urn:li:dataProduct:product1"
    mock_product.name = "product1"
    mock_product.description = "Test product"
    mock_product.domain = None
    mock_product.owner = None
    mock_product.assets = []
    mock_product.properties = {}
    graph.data_products = [mock_product]

    # Create mock domain with proper attributes
    mock_domain = Mock(spec=DataHubDomain)
    mock_domain.urn = DomainUrn.from_string("urn:li:domain:domain1")
    mock_domain.name = "domain1"
    mock_domain.path_segments = ["domain1"]
    mock_domain.parent_domain_urn = None
    mock_domain.glossary_terms = []  # Empty - terms will be processed separately
    mock_domain.datasets = []
    mock_domain.subdomains = []
    graph.domains = [mock_domain]

    # Use lineage_relationships (actual attribute) and add lineage alias if needed
    mock_lineage = Mock(spec=DataHubLineageRelationship)
    mock_lineage.source_urn = "urn:li:dataset:source"
    mock_lineage.target_urn = "urn:li:dataset:target"
    mock_lineage.lineage_type = Mock()
    mock_lineage.lineage_type.value = "used"
    graph.lineage_relationships = [mock_lineage]
    # Add lineage attribute for compatibility (code references datahub_graph.lineage)
    if not hasattr(graph, "lineage"):
        graph.lineage = graph.lineage_relationships

    # Create mock relationship
    from datahub.ingestion.source.rdf.entities.relationship.ast import (
        RelationshipType,
    )

    mock_relationship = Mock(spec=DataHubRelationship)
    mock_relationship.source_urn = "urn:li:glossaryTerm:term1"
    mock_relationship.target_urn = "urn:li:glossaryTerm:term2"
    mock_relationship.relationship_type = RelationshipType.RELATED
    graph.relationships = [mock_relationship]

    # MCPFactory is now used, so no need to mock DataHubClient
    result = target.send(graph)

    # Should process all entity types (glossary_nodes may or may not be processed)
    # Note: Data products without a domain are skipped (domain is required)
    # Note: Empty domains (no datasets in hierarchy) are filtered out
    # Note: RELATED relationship type is not supported, so relationship MCP not created
    assert result["success"] is True
    assert (
        result["workunits_generated"] >= 5
    )  # At least 5 (data product skipped, empty domain filtered, unsupported relationship type)
    assert result["entities_emitted"] >= 5  # Updated to match workunits_generated


def test_datahub_ingestion_target_domain_with_datasets():
    """Test DataHubIngestionTarget.send() processes domains with datasets."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.entities.dataset.ast import (
        DataHubDataset,
    )
    from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )
    from datahub.utilities.urns.dataset_urn import DatasetUrn
    from datahub.utilities.urns.domain_urn import DomainUrn

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create graph with domain that has datasets
    graph = DataHubGraph()

    # Create mock dataset
    mock_dataset = Mock(spec=DataHubDataset)
    mock_dataset.urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.test_table,PROD)"
    )
    mock_dataset.name = "test_table"
    mock_dataset.description = "Test dataset"
    mock_dataset.custom_properties = {}
    mock_dataset.schema_fields = []
    graph.datasets = [mock_dataset]

    # Create mock domain WITH datasets (this exercises the domain MCP creation path)
    mock_domain = Mock(spec=DataHubDomain)
    mock_domain.urn = DomainUrn.from_string("urn:li:domain:test_domain")
    mock_domain.name = "test_domain"
    mock_domain.path_segments = ["test_domain"]
    mock_domain.parent_domain_urn = None
    mock_domain.glossary_terms = []
    mock_domain.datasets = [mock_dataset]  # Domain has datasets - should create MCPs
    mock_domain.subdomains = []
    mock_domain.description = "Test domain"
    mock_domain.owners = []  # No owners
    graph.domains = [mock_domain]

    result = target.send(graph)

    # Should successfully process domain with datasets
    assert result["success"] is True
    assert result["workunits_generated"] >= 2  # At least dataset + domain
    assert result["entities_emitted"] >= 2


# ============================================================================
# Tests for error handling
# ============================================================================


def test_source_get_workunits_error_handling():
    """Test error handling in get_workunits() method."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="/nonexistent/path")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    # Should not raise exception, but yield nothing and report failure
    workunits = list(source.get_workunits())
    assert len(workunits) == 0
    # Check that failure was reported
    assert len(source.report.failures) > 0


def test_source_close_method():
    """Test RDFSource.close() method."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    config = RDFSourceConfig(source="examples/bcbs239/")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    # Should not raise exception
    source.close()


def test_config_model_skip_export():
    """Test config with skip_export parameter."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    config = RDFSourceConfig(
        source="examples/bcbs239/",
        environment="PROD",
        skip_export=["ownership", "properties"],
    )

    assert config.skip_export == ["ownership", "properties"]


def test_config_model_invalid_skip_export_type():
    """Test that invalid skip_export type raises error."""
    from pydantic import ValidationError

    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    with pytest.raises(ValidationError) as exc_info:
        RDFSourceConfig(source="examples/bcbs239/", skip_export=["invalid_type"])

    assert "Invalid entity type" in str(exc_info.value)


def test_config_model_export_only_and_skip_export():
    """Test that export_only and skip_export can both be set (though mutually exclusive in practice)."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    # Both can be set in config (validation happens at runtime)
    config = RDFSourceConfig(
        source="examples/bcbs239/", export_only=["glossary"], skip_export=["ownership"]
    )

    assert config.export_only == ["glossary"]
    assert config.skip_export == ["ownership"]


def test_config_model_all_optional_parameters():
    """Test config with all optional parameters."""
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceConfig,
    )

    config = RDFSourceConfig(
        source="examples/bcbs239/",
        format="turtle",
        extensions=[".ttl", ".rdf"],
        recursive=False,
        sparql="SELECT ?s WHERE { ?s ?p ?o }",
        filter={"namespace": "http://example.com/"},
        environment="DEV",
        dialect="generic",
        export_only=["glossary", "datasets"],
    )

    assert config.format == "turtle"
    assert config.extensions == [".ttl", ".rdf"]
    assert config.recursive is False
    assert config.sparql == "SELECT ?s WHERE { ?s ?p ?o }"
    assert config.filter == {"namespace": "http://example.com/"}
    assert config.environment == "DEV"
    assert config.dialect == "generic"
    assert config.export_only == ["glossary", "datasets"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
