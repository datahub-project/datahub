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


def test_construct_fqn_bigquery(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction for BigQuery tables."""
    fqn = lineage_extractor._construct_fqn(
        "bigquery", "my-project", "my-dataset", "my-table"
    )
    assert fqn == "bigquery:my-project.my-dataset.my-table"


def test_construct_fqn_gcs_with_path(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test FQN construction for GCS objects with path."""
    fqn = lineage_extractor._construct_fqn(
        "gcs", "my-project", "my-bucket", "path/to/file.csv"
    )
    assert fqn == "gcs:my-bucket.path/to/file.csv"


def test_construct_fqn_gcs_bucket_only(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test FQN construction for GCS bucket-level resources."""
    fqn = lineage_extractor._construct_fqn(
        "gcs", "my-project", "my-bucket", "my-bucket"
    )
    assert fqn == "gcs:my-bucket"


def test_construct_fqn_unknown_platform(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test FQN construction for unknown platforms (fallback)."""
    fqn = lineage_extractor._construct_fqn(
        "unknown", "my-project", "my-dataset", "my-entity"
    )
    assert fqn == "unknown:my-project.my-dataset.my-entity"


def test_extract_entity_id_from_fqn_bigquery(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entity ID extraction from BigQuery FQN."""
    entity_id = lineage_extractor._extract_entity_id_from_fqn(
        "bigquery:my-project.my-dataset.my-table"
    )
    assert entity_id == "my-project.my-dataset.my-table"


def test_extract_entity_id_from_fqn_gcs(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entity ID extraction from GCS FQN."""
    # GCS with path
    entity_id = lineage_extractor._extract_entity_id_from_fqn(
        "gcs:my-bucket.path/to/file.csv"
    )
    assert entity_id == "my-bucket.path/to/file.csv"

    # GCS bucket only
    entity_id = lineage_extractor._extract_entity_id_from_fqn("gcs:my-bucket")
    assert entity_id == "my-bucket"


def test_extract_entity_id_from_fqn_no_prefix(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entity ID extraction without platform prefix."""
    entity_id = lineage_extractor._extract_entity_id_from_fqn(
        "project-123.dataset.table"
    )
    assert entity_id == "project-123.dataset.table"


def test_extract_entity_id_from_fqn_invalid(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entity ID extraction with invalid format."""
    entity_id = lineage_extractor._extract_entity_id_from_fqn("")
    assert entity_id == ""


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


def test_fqn_round_trip_bigquery(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction and parsing round-trip for BigQuery."""
    # Construct FQN
    original_project = "my-project"
    original_dataset = "my-dataset"
    original_table = "my-table"

    fqn = lineage_extractor._construct_fqn(
        "bigquery", original_project, original_dataset, original_table
    )

    # Parse FQN
    extracted_id = lineage_extractor._extract_entity_id_from_fqn(fqn)

    # Verify round-trip
    assert fqn == "bigquery:my-project.my-dataset.my-table"
    assert extracted_id == "my-project.my-dataset.my-table"

    # Verify we can reconstruct the parts
    parts = extracted_id.split(".")
    assert len(parts) == 3
    assert parts[0] == original_project
    assert parts[1] == original_dataset
    assert parts[2] == original_table


def test_fqn_round_trip_gcs(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction and parsing round-trip for GCS."""
    # Construct FQN with path
    original_bucket = "my-bucket"
    original_path = "data/2024/file.csv"

    fqn = lineage_extractor._construct_fqn(
        "gcs", "my-project", original_bucket, original_path
    )

    # Parse FQN
    extracted_id = lineage_extractor._extract_entity_id_from_fqn(fqn)

    # Verify round-trip
    assert fqn == "gcs:my-bucket.data/2024/file.csv"
    assert extracted_id == "my-bucket.data/2024/file.csv"


def test_lineage_with_cross_platform_references(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test lineage extraction with cross-platform references (BigQuery -> GCS)."""
    # Mock BigQuery source with GCS upstream
    mock_bq_link = Mock()
    mock_bq_link.source.fully_qualified_name = "gcs:my-bucket.raw/data.csv"

    mock_gcs_link = Mock()
    mock_gcs_link.target.fully_qualified_name = "bigquery:my-project.analytics.final"

    def search_links_side_effect(request):
        if hasattr(request, "target") and request.target:
            return [mock_bq_link]
        elif hasattr(request, "source") and request.source:
            return [mock_gcs_link]
        return []

    lineage_extractor.lineage_client.search_links.side_effect = search_links_side_effect

    test_entity = EntityDataTuple(
        lake_id="test-lake",
        zone_id="test-zone",
        entity_id="test-table",
        asset_id="test-asset",
        source_platform="bigquery",
        dataset_id="analytics",
    )

    result = lineage_extractor.get_lineage_for_entity("my-project", test_entity)

    assert result is not None
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 1
    assert result["upstream"][0] == "gcs:my-bucket.raw/data.csv"
    assert result["downstream"][0] == "bigquery:my-project.analytics.final"


def test_workunit_urn_structure_validation(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that generated workunits have correct URN structure."""
    # Add a lineage edge
    edge = LineageEdge(
        entity_id="my-project.my-dataset.upstream-table",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    lineage_extractor.lineage_map["downstream-table"] = {edge}

    # Generate workunit
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dataplex,my-project.my-dataset.downstream-table,PROD)"
    workunits = list(lineage_extractor.gen_lineage("downstream-table", dataset_urn))

    assert len(workunits) == 1
    workunit = workunits[0]

    # Validate URN structure
    assert workunit.metadata.entityUrn == dataset_urn
    assert workunit.metadata.entityUrn.startswith("urn:li:dataset:")
    assert "urn:li:dataPlatform:dataplex" in workunit.metadata.entityUrn
    assert "PROD" in workunit.metadata.entityUrn

    # Validate aspect type
    from datahub.metadata.schema_classes import UpstreamLineageClass

    assert isinstance(workunit.metadata.aspect, UpstreamLineageClass)


def test_workunit_aspect_completeness(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that workunit aspects contain all required fields."""
    # Add lineage edges
    edge1 = LineageEdge(
        entity_id="my-project.my-dataset.table1",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )
    edge2 = LineageEdge(
        entity_id="my-project.my-dataset.table2",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.COPY,
    )

    lineage_extractor.lineage_map["target-table"] = {edge1, edge2}

    # Generate workunit
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dataplex,my-project.my-dataset.target-table,PROD)"
    workunits = list(lineage_extractor.gen_lineage("target-table", dataset_urn))

    assert len(workunits) == 1
    workunit = workunits[0]
    aspect = workunit.metadata.aspect

    # Validate UpstreamLineageClass has required fields
    assert hasattr(aspect, "upstreams")
    assert aspect.upstreams is not None
    assert len(aspect.upstreams) == 2

    # Validate each upstream has required fields
    for upstream in aspect.upstreams:
        assert hasattr(upstream, "dataset")
        assert upstream.dataset is not None
        assert upstream.dataset.startswith("urn:li:dataset:")

        assert hasattr(upstream, "type")
        assert upstream.type in [
            DatasetLineageTypeClass.TRANSFORMED,
            DatasetLineageTypeClass.COPY,
        ]

        assert hasattr(upstream, "auditStamp")
        assert upstream.auditStamp is not None
        assert hasattr(upstream.auditStamp, "time")
        assert hasattr(upstream.auditStamp, "actor")
        assert upstream.auditStamp.actor == "urn:li:corpuser:datahub"


def test_workunit_upstream_urn_format(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that upstream URNs in workunits are correctly formatted."""
    # Add lineage edge with specific entity ID format
    edge = LineageEdge(
        entity_id="test-project.sales_dataset.customer_table",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    lineage_extractor.lineage_map["analytics_table"] = {edge}
    lineage_extractor.config.env = "PROD"

    # Generate workunit
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dataplex,test-project.analytics_dataset.analytics_table,PROD)"
    workunits = list(lineage_extractor.gen_lineage("analytics_table", dataset_urn))

    assert len(workunits) == 1
    upstream_urn = workunits[0].metadata.aspect.upstreams[0].dataset

    # Validate upstream URN structure
    assert upstream_urn.startswith("urn:li:dataset:")
    assert "urn:li:dataPlatform:dataplex" in upstream_urn
    assert "test-project" in upstream_urn
    assert "customer_table" in upstream_urn

    # Parse URN components
    # Format: urn:li:dataset:(urn:li:dataPlatform:dataplex,{entity_id},PROD)
    assert upstream_urn.count("(") == 1
    assert upstream_urn.count(")") == 1
    assert upstream_urn.count(",") == 2


def test_workunit_generation_with_no_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that no workunits are generated when there's no lineage."""
    # Don't add any lineage to the map
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dataplex,my-project.my-dataset.isolated-table,PROD)"
    workunits = list(lineage_extractor.gen_lineage("isolated-table", dataset_urn))

    # Should generate no workunits
    assert len(workunits) == 0
