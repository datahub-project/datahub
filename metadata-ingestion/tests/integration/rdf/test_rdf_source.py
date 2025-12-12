#!/usr/bin/env python3
"""
Integration tests for RDF ingestion source.

These tests verify end-to-end functionality including:
- RDF file loading and parsing
- Entity extraction (glossary terms, relationships, domains)
- Work unit generation
- Error handling
- Stateful ingestion
- Different RDF formats
"""

import pathlib

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

_resources_dir = pathlib.Path(__file__).parent

pytestmark = pytest.mark.integration


@pytest.fixture
def simple_glossary_ttl(tmp_path):
    """Create a simple glossary RDF file for testing."""
    ttl_content = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/glossary/> .

ex:AccountIdentifier a skos:Concept ;
    skos:prefLabel "Account Identifier" ;
    skos:definition "A unique identifier for an account" .

ex:CustomerName a skos:Concept ;
    skos:prefLabel "Customer Name" ;
    skos:definition "The name of the customer" .
"""
    ttl_file = tmp_path / "simple_glossary.ttl"
    ttl_file.write_text(ttl_content)
    return str(ttl_file)


@pytest.fixture
def glossary_with_relationships_ttl(tmp_path):
    """Create a glossary RDF file with relationships."""
    ttl_content = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/glossary/> .

ex:ChildTerm a skos:Concept ;
    skos:prefLabel "Child Term" ;
    skos:definition "A child term" ;
    skos:broader ex:ParentTerm .

ex:ParentTerm a skos:Concept ;
    skos:prefLabel "Parent Term" ;
    skos:definition "A parent term" .
"""
    ttl_file = tmp_path / "glossary_relationships.ttl"
    ttl_file.write_text(ttl_content)
    return str(ttl_file)


@pytest.fixture
def glossary_with_domains_ttl(tmp_path):
    """Create a glossary RDF file with domain hierarchy."""
    ttl_content = """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix trading: <http://bank.com/trading/loans/> .

trading:Customer_Name a skos:Concept ;
    skos:prefLabel "Customer Name" ;
    skos:definition "The name of the customer" .

trading:Loan_Amount a skos:Concept ;
    skos:prefLabel "Loan Amount" ;
    skos:definition "The principal amount of the loan" .
"""
    ttl_file = tmp_path / "glossary_domains.ttl"
    ttl_file.write_text(ttl_content)
    return str(ttl_file)


@pytest.fixture
def malformed_ttl(tmp_path):
    """Create a malformed RDF file for error testing."""
    ttl_file = tmp_path / "malformed.ttl"
    ttl_file.write_text("This is not valid RDF content")
    return str(ttl_file)


@pytest.mark.integration
def test_simple_glossary_ingestion(
    simple_glossary_ttl, pytestconfig, tmp_path, mock_datahub_graph_instance
):
    """Test basic glossary term ingestion."""
    output_path = tmp_path / "simple_glossary_output.json"
    golden_path = _resources_dir / "simple_glossary_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-simple",
            "source": {
                "type": "rdf",
                "config": {
                    "source": simple_glossary_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_glossary_with_relationships_ingestion(
    glossary_with_relationships_ttl,
    pytestconfig,
    tmp_path,
    mock_datahub_graph_instance,
):
    """Test glossary ingestion with term relationships."""
    output_path = tmp_path / "glossary_relationships_output.json"
    golden_path = _resources_dir / "glossary_relationships_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-relationships",
            "source": {
                "type": "rdf",
                "config": {
                    "source": glossary_with_relationships_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_glossary_with_domains_ingestion(
    glossary_with_domains_ttl,
    pytestconfig,
    tmp_path,
    mock_datahub_graph_instance,
):
    """Test glossary ingestion with domain hierarchy."""
    output_path = tmp_path / "glossary_domains_output.json"
    golden_path = _resources_dir / "glossary_domains_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-domains",
            "source": {
                "type": "rdf",
                "config": {
                    "source": glossary_with_domains_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_rdf_xml_format(pytestconfig, tmp_path, mock_datahub_graph_instance):
    """Test RDF/XML format ingestion."""
    rdf_xml_content = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:skos="http://www.w3.org/2004/02/skos/core#">
  <skos:Concept rdf:about="http://example.org/glossary/TestTerm">
    <skos:prefLabel>Test Term</skos:prefLabel>
    <skos:definition>A test term</skos:definition>
  </skos:Concept>
</rdf:RDF>"""
    rdf_file = tmp_path / "test.xml"
    rdf_file.write_text(rdf_xml_content)

    output_path = tmp_path / "rdf_xml_output.json"
    golden_path = _resources_dir / "rdf_xml_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-xml",
            "source": {
                "type": "rdf",
                "config": {
                    "source": str(rdf_file),
                    "format": "xml",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_json_ld_format(pytestconfig, tmp_path, mock_datahub_graph_instance):
    """Test JSON-LD format ingestion."""
    json_ld_content = """{
  "@context": {
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "ex": "http://example.org/glossary/"
  },
  "@id": "ex:TestTerm",
  "@type": "skos:Concept",
  "skos:prefLabel": "Test Term",
  "skos:definition": "A test term"
}"""
    json_file = tmp_path / "test.jsonld"
    json_file.write_text(json_ld_content)

    output_path = tmp_path / "json_ld_output.json"
    golden_path = _resources_dir / "json_ld_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-jsonld",
            "source": {
                "type": "rdf",
                "config": {
                    "source": str(json_file),
                    "format": "json-ld",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_recursive_directory_ingestion(
    simple_glossary_ttl, pytestconfig, tmp_path, mock_datahub_graph_instance
):
    """Test recursive directory ingestion."""
    # Create a directory with multiple RDF files
    rdf_dir = tmp_path / "rdf_data"
    rdf_dir.mkdir()

    # Copy the simple glossary to the directory
    import shutil

    shutil.copy(simple_glossary_ttl, rdf_dir / "file1.ttl")

    # Create another file
    (rdf_dir / "file2.ttl").write_text(
        """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/glossary/> .

ex:AnotherTerm a skos:Concept ;
    skos:prefLabel "Another Term" ;
    skos:definition "Another test term" .
"""
    )

    output_path = tmp_path / "recursive_output.json"
    golden_path = _resources_dir / "recursive_directory_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-recursive",
            "source": {
                "type": "rdf",
                "config": {
                    "source": str(rdf_dir),
                    "format": "turtle",
                    "recursive": True,
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_export_only_filter(
    glossary_with_relationships_ttl, pytestconfig, tmp_path, mock_datahub_graph_instance
):
    """Test export_only filter to only export glossary terms."""
    output_path = tmp_path / "export_only_output.json"
    golden_path = _resources_dir / "export_only_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-export-only",
            "source": {
                "type": "rdf",
                "config": {
                    "source": glossary_with_relationships_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                    "export_only": ["glossary"],
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_skip_export_filter(
    glossary_with_relationships_ttl, pytestconfig, tmp_path, mock_datahub_graph_instance
):
    """Test skip_export filter to skip relationships."""
    output_path = tmp_path / "skip_export_output.json"
    golden_path = _resources_dir / "skip_export_golden.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-skip-export",
            "source": {
                "type": "rdf",
                "config": {
                    "source": glossary_with_relationships_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                    "skip_export": ["relationship"],
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_stateful_ingestion(
    simple_glossary_ttl, pytestconfig, tmp_path, mock_datahub_graph_instance
):
    """Test stateful ingestion with stale entity removal."""
    output_path = tmp_path / "stateful_output.json"
    golden_path = _resources_dir / "stateful_ingestion_golden.json"

    # Use file-based state provider to avoid needing a DataHub graph connection
    # This is simpler than mocking the graph and works well for integration tests
    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-stateful",
            "pipeline_name": "rdf-test-stateful",
            "source": {
                "type": "rdf",
                "config": {
                    "source": simple_glossary_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                    "stateful_ingestion": {
                        "enabled": True,
                        "remove_stale_metadata": True,
                        "state_provider": {
                            "type": "file",
                            "config": {
                                "filename": str(tmp_path / "state.json"),
                            },
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify the output
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )


@pytest.mark.integration
def test_missing_file_error(tmp_path, mock_datahub_graph_instance):
    """Test error handling for missing RDF file."""
    missing_file = tmp_path / "nonexistent.ttl"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-missing-file",
            "source": {
                "type": "rdf",
                "config": {
                    "source": str(missing_file),
                    "format": "turtle",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "error_output.json"),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()

    # Should have failures
    from datahub.ingestion.source.rdf.ingestion.rdf_source import RDFSource

    assert isinstance(pipeline.source, RDFSource)
    assert pipeline.source.report.failures, "Expected failures for missing file"
    assert pipeline.source.report.num_workunits_produced == 0, (
        "Should not produce work units"
    )


@pytest.mark.integration
def test_malformed_rdf_error(malformed_ttl, tmp_path, mock_datahub_graph_instance):
    """Test error handling for malformed RDF."""
    output_path = tmp_path / "malformed_output.json"

    pipeline = Pipeline.create(
        {
            "run_id": "rdf-test-malformed",
            "source": {
                "type": "rdf",
                "config": {
                    "source": malformed_ttl,
                    "format": "turtle",
                    "environment": "PROD",
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )
    pipeline.ctx.graph = mock_datahub_graph_instance
    pipeline.run()

    # Should have failures
    from datahub.ingestion.source.rdf.ingestion.rdf_source import RDFSource

    assert isinstance(pipeline.source, RDFSource)
    assert pipeline.source.report.failures, "Expected failures for malformed RDF"
    # May or may not produce work units depending on error handling granularity
