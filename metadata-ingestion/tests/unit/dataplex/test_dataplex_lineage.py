"""Unit tests for Dataplex lineage extraction."""

import datetime
from unittest.mock import Mock

import pytest

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntityDataTuple
from datahub.ingestion.source.dataplex.dataplex_lineage import (
    DataplexLineageExtractor,
    LineageEdge,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import DatasetLineageTypeClass


@pytest.fixture
def dataplex_config() -> DataplexConfig:
    """Create a test configuration."""
    return DataplexConfig(
        project_ids=["test-project"],
        location="us-central1",
        extract_lineage=True,
    )


@pytest.fixture
def dataplex_report() -> DataplexReport:
    """Create a test report."""
    return DataplexReport()


@pytest.fixture
def mock_lineage_client() -> Mock:
    """Create a mock LineageClient."""
    return Mock()


@pytest.fixture
def lineage_extractor(
    dataplex_config: DataplexConfig,
    dataplex_report: DataplexReport,
    mock_lineage_client: Mock,
) -> DataplexLineageExtractor:
    """Create a lineage extractor with mocked client."""
    return DataplexLineageExtractor(
        config=dataplex_config,
        report=dataplex_report,
        lineage_client=mock_lineage_client,
    )


def test_lineage_extractor_initialization(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that the lineage extractor initializes correctly."""
    assert lineage_extractor.config is not None
    assert lineage_extractor.report is not None
    assert lineage_extractor.lineage_client is not None
    assert lineage_extractor.platform == "dataplex"
    assert isinstance(lineage_extractor.lineage_map, dict)


def test_lineage_extraction_disabled(
    dataplex_report: DataplexReport,
) -> None:
    """Test that lineage extraction is skipped when disabled."""
    config = DataplexConfig(
        project_ids=["test-project"],
        location="us-central1",
        extract_lineage=False,  # Disabled
    )

    extractor = DataplexLineageExtractor(
        config=config,
        report=dataplex_report,
        lineage_client=None,
    )

    result = extractor.get_lineage_for_entity("test-project", "test-entity")
    assert result is None


def test_construct_fqn(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction."""
    fqn = lineage_extractor._construct_fqn(
        "dataplex", "my-project", "my-dataset", "my-entity"
    )
    assert fqn == "dataplex:my-project.my-dataset.my-entity"


def test_extract_entity_id_from_fqn(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entity ID extraction from FQN."""
    # Test with proper format
    entity_id = lineage_extractor._extract_entity_id_from_fqn(
        "dataplex:project-123.entity-456"
    )
    assert entity_id == "project-123.entity-456"

    # Test without prefix
    entity_id = lineage_extractor._extract_entity_id_from_fqn("project-123.entity-456")
    assert entity_id == "project-123.entity-456"

    # Test with invalid format
    entity_id = lineage_extractor._extract_entity_id_from_fqn("invalid")
    assert entity_id == "invalid"


def test_lineage_edge_creation() -> None:
    """Test LineageEdge data structure."""
    edge = LineageEdge(
        entity_id="upstream-entity",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    assert edge.entity_id == "upstream-entity"
    assert edge.lineage_type == DatasetLineageTypeClass.TRANSFORMED


def test_search_links_by_target(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test searching lineage links by target."""
    # Setup mock response
    mock_link = Mock()
    mock_link.source.fully_qualified_name = "upstream:source-entity"
    lineage_extractor.lineage_client.search_links.return_value = [mock_link]

    # Search
    parent = "projects/test-project/locations/us-central1"
    fqn = "dataplex:test-project.test-entity"
    results = list(lineage_extractor._search_links_by_target(parent, fqn))

    assert len(results) == 1
    assert results[0] == mock_link


def test_search_links_by_source(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test searching lineage links by source."""
    # Setup mock response
    mock_link = Mock()
    mock_link.target.fully_qualified_name = "downstream:target-entity"
    lineage_extractor.lineage_client.search_links.return_value = [mock_link]

    # Search
    parent = "projects/test-project/locations/us-central1"
    fqn = "dataplex:test-project.test-entity"
    results = list(lineage_extractor._search_links_by_source(parent, fqn))

    assert len(results) == 1
    assert results[0] == mock_link


def test_get_lineage_for_entity_with_upstream(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage with upstream relationships."""
    # Mock upstream link
    mock_upstream_link = Mock()
    mock_upstream_link.source.fully_qualified_name = "bigquery:project.dataset.table1"

    # Mock downstream link
    mock_downstream_link = Mock()
    mock_downstream_link.target.fully_qualified_name = "bigquery:project.dataset.table2"

    # Setup mock to return different results for target vs source queries
    def search_links_side_effect(request):
        if hasattr(request, "target") and request.target:
            return [mock_upstream_link]
        elif hasattr(request, "source") and request.source:
            return [mock_downstream_link]
        return []

    lineage_extractor.lineage_client.search_links.side_effect = search_links_side_effect

    # Create EntityDataTuple for the test entity
    test_entity = EntityDataTuple(
        lake_id="test-lake",
        zone_id="test-zone",
        entity_id="test-entity",
        asset_id="test-asset",
        source_platform="dataplex",
        dataset_id="test-dataset",
    )

    # Get lineage
    result = lineage_extractor.get_lineage_for_entity("test-project", test_entity)

    assert result is not None
    assert "upstream" in result
    assert "downstream" in result
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 1
    assert result["upstream"][0] == "bigquery:project.dataset.table1"
    assert result["downstream"][0] == "bigquery:project.dataset.table2"


def test_build_lineage_map(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test building lineage map for multiple entities."""
    # Mock lineage response
    mock_link = Mock()
    mock_link.source.fully_qualified_name = (
        "dataplex:test-project.test-dataset.upstream-entity"
    )

    lineage_extractor.lineage_client.search_links.return_value = [mock_link]

    # Create EntityDataTuple objects for test entities
    entity_data = [
        EntityDataTuple(
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="entity1",
            asset_id="test-asset1",
            source_platform="dataplex",
            dataset_id="test-dataset",
        ),
        EntityDataTuple(
            lake_id="test-lake",
            zone_id="test-zone",
            entity_id="entity2",
            asset_id="test-asset2",
            source_platform="dataplex",
            dataset_id="test-dataset",
        ),
    ]

    # Build lineage map
    lineage_map = lineage_extractor.build_lineage_map("test-project", entity_data)

    assert isinstance(lineage_map, dict)
    # The map should have entries for entities that have lineage
    assert lineage_extractor.report.num_lineage_entries_scanned >= 0


def test_get_lineage_for_table_no_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage for table with no lineage."""
    result = lineage_extractor.get_lineage_for_table(
        "entity-with-no-lineage",
        "urn:li:dataset:(urn:li:dataPlatform:dataplex,test.entity,PROD)",
    )

    assert result is None


def test_get_lineage_for_table_with_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage for table with lineage."""
    # Add a lineage edge to the map
    edge = LineageEdge(
        entity_id="test-project.test-dataset.upstream-entity",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    lineage_extractor.lineage_map["test-entity"] = {edge}

    # Get lineage
    result = lineage_extractor.get_lineage_for_table(
        "test-entity",
        "urn:li:dataset:(urn:li:dataPlatform:dataplex,test-project.test-dataset.test-entity,PROD)",
    )

    assert result is not None
    assert len(result.upstreams) == 1
    assert result.upstreams[0].type == DatasetLineageTypeClass.TRANSFORMED


def test_gen_lineage_workunits(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test generating lineage workunits."""
    # Add a lineage edge
    edge = LineageEdge(
        entity_id="test-project.test-dataset.upstream-entity",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    lineage_extractor.lineage_map["test-entity"] = {edge}

    # Generate workunits
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dataplex,test-project.test-dataset.test-entity,PROD)"
    workunits = list(lineage_extractor.gen_lineage("test-entity", dataset_urn))

    assert len(workunits) == 1
    assert workunits[0].metadata.entityUrn == dataset_urn
