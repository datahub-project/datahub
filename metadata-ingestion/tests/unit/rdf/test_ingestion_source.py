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
        export_only=["glossary"],
    )

    assert config.export_only == ["glossary"]


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
        "export_only": ["glossary"],
        "recursive": True,
    }

    config = RDFSourceConfig.model_validate(config_dict)

    assert config.source == "examples/bcbs239/"
    assert config.environment == "PROD"
    assert config.export_only == ["glossary"]
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
# Tests for RDF loading (replaces _create_source() tests)
# ============================================================================


def test_load_rdf_graph_with_file(tmp_path):
    """Test load_rdf_graph() with a single file."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    # Create a temporary file
    test_file = tmp_path / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    graph = load_rdf_graph(source=str(test_file))
    assert graph is not None
    assert len(graph) > 0


def test_load_rdf_graph_with_folder(tmp_path):
    """Test load_rdf_graph() with a folder path."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    # Create a temporary folder with a file
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    test_file = test_dir / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    graph = load_rdf_graph(source=str(test_dir))
    assert graph is not None
    assert len(graph) > 0


def test_load_rdf_graph_with_url():
    """Test load_rdf_graph() with HTTP URL."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    # Note: This will fail if URL doesn't exist, but tests the code path
    with pytest.raises((ValueError, Exception)):
        load_rdf_graph(source="http://example.com/nonexistent.ttl")


def test_load_rdf_graph_with_comma_separated_files(tmp_path):
    """Test load_rdf_graph() with comma-separated files."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    # Create temporary files
    file1 = tmp_path / "file1.ttl"
    file1.write_text("@prefix ex: <http://example.org/> . ex:test1 a ex:Test .")
    file2 = tmp_path / "file2.ttl"
    file2.write_text("@prefix ex: <http://example.org/> . ex:test2 a ex:Test .")

    graph = load_rdf_graph(source=f"{file1},{file2}")
    assert graph is not None
    assert len(graph) > 0


def test_load_rdf_graph_with_invalid_path():
    """Test load_rdf_graph() raises error for invalid path."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    with pytest.raises(ValueError, match="Source not found"):
        load_rdf_graph(source="/nonexistent/path/that/does/not/exist.ttl")


def test_load_rdf_graph_with_recursive_config(tmp_path):
    """Test load_rdf_graph() respects recursive configuration."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    test_file = test_dir / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    graph = load_rdf_graph(source=str(test_dir), recursive=False)
    assert graph is not None


def test_load_rdf_graph_with_custom_extensions(tmp_path):
    """Test load_rdf_graph() respects custom file extensions."""
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    test_file = test_dir / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    graph = load_rdf_graph(source=str(test_dir), file_extensions=[".ttl", ".custom"])
    assert graph is not None


# ============================================================================
# Tests for RDF to DataHub conversion (replaces _create_transpiler() tests)
# ============================================================================


def test_convert_rdf_to_datahub_ast_with_environment(tmp_path):
    """Test _convert_rdf_to_datahub_ast() sets environment correctly."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    # Create a test file
    test_file = tmp_path / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    config = RDFSourceConfig(source=str(test_file), environment="DEV")
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    graph = load_rdf_graph(source=str(test_file))
    datahub_ast = source._convert_rdf_to_datahub_ast(
        graph, environment="DEV", export_only=None, skip_export=None
    )
    assert datahub_ast is not None


def test_convert_rdf_to_datahub_ast_with_export_only(tmp_path):
    """Test _convert_rdf_to_datahub_ast() respects export_only filter."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    # Create a test file
    test_file = tmp_path / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    config = RDFSourceConfig(source=str(test_file), export_only=["glossary"])
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    graph = load_rdf_graph(source=str(test_file))
    datahub_ast = source._convert_rdf_to_datahub_ast(
        graph, environment="PROD", export_only=["glossary"], skip_export=None
    )
    assert datahub_ast is not None


def test_convert_rdf_to_datahub_ast_with_skip_export(tmp_path):
    """Test _convert_rdf_to_datahub_ast() respects skip_export filter."""
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSource,
        RDFSourceConfig,
    )

    # Create a test file
    test_file = tmp_path / "test.ttl"
    test_file.write_text("@prefix ex: <http://example.org/> . ex:test a ex:Test .")

    config = RDFSourceConfig(source=str(test_file), skip_export=["ownership"])
    ctx = PipelineContext(run_id="test-run")
    source = RDFSource(config, ctx)

    graph = load_rdf_graph(source=str(test_file))
    datahub_ast = source._convert_rdf_to_datahub_ast(
        graph, environment="PROD", export_only=None, skip_export=["ownership"]
    )
    assert datahub_ast is not None


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

    # MCPFactory is now used, so no need to mock DataHubClient
    result = target.send(graph)

    assert result["success"] is True
    assert result["workunits_generated"] >= 1  # At least 1 (term)
    assert result["entities_emitted"] >= 1
    assert len(target.workunits) >= 1


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


def test_datahub_ingestion_target_send_with_mvp_entity_types():
    """Test DataHubIngestionTarget.send() with MVP entity types."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )
    from datahub.ingestion.source.rdf.entities.relationship.ast import (
        DataHubRelationship,
        RelationshipType,
    )
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )
    from datahub.utilities.urns.domain_urn import DomainUrn

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create graph with MVP entity types
    graph = DataHubGraph()

    # Create mock glossary term
    mock_term = Mock(spec=DataHubGlossaryTerm)
    mock_term.urn = "urn:li:glossaryTerm:term1"
    mock_term.name = "term1"
    mock_term.definition = "Test term"
    mock_term.source = "http://example.com/term1"
    mock_term.custom_properties = {}
    graph.glossary_terms = [mock_term]

    # Create mock domain with glossary terms
    mock_domain = Mock(spec=DataHubDomain)
    mock_domain.urn = DomainUrn.from_string("urn:li:domain:domain1")
    mock_domain.name = "domain1"
    mock_domain.path_segments = ["domain1"]
    mock_domain.parent_domain_urn = None
    mock_domain.glossary_terms = [mock_term]  # Domain has glossary terms
    mock_domain.subdomains = []
    graph.domains = [mock_domain]

    # Create mock relationship
    mock_relationship = Mock(spec=DataHubRelationship)
    mock_relationship.source_urn = "urn:li:glossaryTerm:term1"
    mock_relationship.target_urn = "urn:li:glossaryTerm:term2"
    mock_relationship.relationship_type = RelationshipType.BROADER
    graph.relationships = [mock_relationship]

    # MCPFactory is now used, so no need to mock DataHubClient
    result = target.send(graph)

    # Should process MVP entity types
    assert result["success"] is True
    assert result["workunits_generated"] >= 1  # At least glossary term
    assert result["entities_emitted"] >= 1


def test_datahub_ingestion_target_domain_with_glossary_terms():
    """Test DataHubIngestionTarget.send() processes domains with glossary terms."""
    from datahub.ingestion.source.rdf.core.ast import DataHubGraph
    from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
    from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
        DataHubGlossaryTerm,
    )
    from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
        DataHubIngestionTarget,
    )
    from datahub.ingestion.source.rdf.ingestion.rdf_source import (
        RDFSourceReport,
    )
    from datahub.utilities.urns.domain_urn import DomainUrn

    report = RDFSourceReport()
    target = DataHubIngestionTarget(report)

    # Create graph with domain that has glossary terms
    graph = DataHubGraph()

    # Create mock glossary term
    mock_term = Mock(spec=DataHubGlossaryTerm)
    mock_term.urn = "urn:li:glossaryTerm:test"
    mock_term.name = "test_term"
    mock_term.definition = "Test term"
    mock_term.source = None
    mock_term.custom_properties = {}
    graph.glossary_terms = [mock_term]

    # Create mock domain WITH glossary terms
    # Domains are used as data structure - glossary module creates glossary nodes and terms
    mock_domain = Mock(spec=DataHubDomain)
    mock_domain.urn = DomainUrn.from_string("urn:li:domain:test_domain")
    mock_domain.name = "test_domain"
    mock_domain.path_segments = ["test_domain"]
    mock_domain.parent_domain_urn = None
    mock_domain.glossary_terms = [
        mock_term
    ]  # Domain has glossary terms - glossary module will create glossary node and term MCPs
    mock_domain.subdomains = []
    graph.domains = [mock_domain]

    result = target.send(graph)

    # Should successfully process - glossary module creates glossary nodes and terms from domain
    # Domains are NOT ingested as domain entities
    assert result["success"] is True
    assert result["workunits_generated"] >= 1  # At least glossary node and term
    assert result["entities_emitted"] >= 1


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
        skip_export=["ownership"],
    )

    assert config.skip_export == ["ownership"]


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
        environment="DEV",
        dialect="generic",
        export_only=["glossary"],
    )

    assert config.format == "turtle"
    assert config.extensions == [".ttl", ".rdf"]
    assert config.recursive is False
    assert config.environment == "DEV"
    assert config.dialect == "generic"
    assert config.export_only == ["glossary"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
