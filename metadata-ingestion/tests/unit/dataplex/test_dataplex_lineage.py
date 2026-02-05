"""Unit tests for Dataplex lineage extraction."""

import datetime
from unittest.mock import MagicMock, Mock

import pytest

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
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
        entries_location="us",
        include_lineage=True,
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
    assert isinstance(lineage_extractor.lineage_map, dict)


def test_lineage_extraction_disabled(
    dataplex_report: DataplexReport,
) -> None:
    """Test that lineage extraction is skipped when disabled."""
    config = DataplexConfig(
        project_ids=["test-project"],
        entries_location="us",
        include_lineage=False,  # Disabled
    )

    extractor = DataplexLineageExtractor(
        config=config,
        report=dataplex_report,
        lineage_client=None,
    )

    entry_data = EntryDataTuple(
        entry_id="test-entry",
        source_platform="bigquery",
        dataset_id="test-project.test-dataset.test-table",
    )
    result = extractor.get_lineage_for_entry("test-project", entry_data)
    assert result is None


def test_construct_fqn_bigquery(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction for BigQuery tables."""
    fqn = lineage_extractor._construct_fqn("bigquery", "my-project.my-dataset.my-table")
    assert fqn == "bigquery:my-project.my-dataset.my-table"


def test_construct_fqn_gcs(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction for GCS objects."""
    fqn = lineage_extractor._construct_fqn("gcs", "my-bucket/path/to/file.csv")
    assert fqn == "gcs:my-bucket/path/to/file.csv"


def test_construct_fqn_unknown_platform(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test FQN construction for unknown platforms (fallback)."""
    fqn = lineage_extractor._construct_fqn("unknown", "my-dataset-id")
    assert fqn == "unknown:my-dataset-id"


def test_extract_entry_id_from_fqn_bigquery(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entry ID extraction from BigQuery FQN."""
    entry_id = lineage_extractor._extract_entry_id_from_fqn(
        "bigquery:my-project.my-dataset.my-table"
    )
    assert entry_id == "my-project.my-dataset.my-table"


def test_extract_entry_id_from_fqn_gcs(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entry ID extraction from GCS FQN."""
    # GCS with path
    entry_id = lineage_extractor._extract_entry_id_from_fqn(
        "gcs:my-bucket/path/to/file.csv"
    )
    assert entry_id == "my-bucket/path/to/file.csv"

    # GCS bucket only
    entry_id = lineage_extractor._extract_entry_id_from_fqn("gcs:my-bucket")
    assert entry_id == "my-bucket"


def test_extract_entry_id_from_fqn_no_prefix(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entry ID extraction without platform prefix."""
    entry_id = lineage_extractor._extract_entry_id_from_fqn("project-123.dataset.table")
    assert entry_id == "project-123.dataset.table"


def test_extract_entry_id_from_fqn_invalid(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test entry ID extraction with invalid format."""
    entry_id = lineage_extractor._extract_entry_id_from_fqn("")
    assert entry_id == ""


def test_lineage_edge_creation() -> None:
    """Test LineageEdge data structure."""
    edge = LineageEdge(
        entry_id="upstream-entry",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    assert edge.entry_id == "upstream-entry"
    assert edge.lineage_type == DatasetLineageTypeClass.TRANSFORMED


def test_search_links_by_target(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test searching lineage links by target."""
    # Setup mock response
    mock_link = Mock()
    mock_link.source.fully_qualified_name = "upstream:source-entry"
    lineage_extractor.lineage_client.search_links.return_value = [mock_link]  # type: ignore[union-attr]

    # Search
    parent = "projects/test-project/locations/us"
    fqn = "bigquery:test-project.dataset.table"
    results = list(lineage_extractor._search_links_by_target(parent, fqn))

    assert len(results) == 1
    assert results[0] == mock_link


def test_search_links_by_source(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test searching lineage links by source."""
    # Setup mock response
    mock_link = Mock()
    mock_link.target.fully_qualified_name = "downstream:target-entry"
    lineage_extractor.lineage_client.search_links.return_value = [mock_link]  # type: ignore[union-attr]

    # Search
    parent = "projects/test-project/locations/us"
    fqn = "bigquery:test-project.dataset.table"
    results = list(lineage_extractor._search_links_by_source(parent, fqn))

    assert len(results) == 1
    assert results[0] == mock_link


def test_get_lineage_for_entry_with_upstream(
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

    lineage_extractor.lineage_client.search_links.side_effect = search_links_side_effect  # type: ignore[union-attr]

    # Create EntryDataTuple for the test entry
    test_entry = EntryDataTuple(
        entry_id="test-entry",
        source_platform="bigquery",
        dataset_id="test-project.test-dataset.test-table",
    )

    # Get lineage
    result = lineage_extractor.get_lineage_for_entry("test-project", test_entry)

    assert result is not None
    assert "upstream" in result
    assert "downstream" in result
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 1
    assert result["upstream"][0] == "bigquery:project.dataset.table1"
    assert result["downstream"][0] == "bigquery:project.dataset.table2"


def test_build_lineage_map(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test building lineage map for multiple entries."""
    # Mock lineage response
    mock_link = Mock()
    mock_link.source.fully_qualified_name = (
        "bigquery:test-project.test-dataset.upstream-entry"
    )

    lineage_extractor.lineage_client.search_links.return_value = [mock_link]  # type: ignore[union-attr]

    # Create EntryDataTuple objects for test entries
    entry_data = [
        EntryDataTuple(
            entry_id="entry1",
            source_platform="bigquery",
            dataset_id="test-project.dataset.table1",
        ),
        EntryDataTuple(
            entry_id="entry2",
            source_platform="bigquery",
            dataset_id="test-project.dataset.table2",
        ),
    ]

    # Build lineage map
    lineage_map = lineage_extractor.build_lineage_map("test-project", entry_data)

    assert isinstance(lineage_map, dict)
    # The map should have entries for entries that have lineage
    assert lineage_extractor.report.num_lineage_entries_scanned >= 0


def test_get_lineage_for_table_no_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage for table with no lineage."""
    result = lineage_extractor.get_lineage_for_table(
        "test-project.test-dataset.entry-with-no-lineage",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.entry-with-no-lineage,PROD)",
        "bigquery",
    )

    assert result is None


def test_get_lineage_for_table_with_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage for table with lineage."""
    # Add a lineage edge to the map - use full dataset_id as key
    edge = LineageEdge(
        entry_id="test-project.test-dataset.upstream-entry",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id, not just entry name
    lineage_extractor.lineage_map["test-project.test-dataset.test-entry"] = {edge}

    # Get lineage using full dataset_id
    result = lineage_extractor.get_lineage_for_table(
        "test-project.test-dataset.test-entry",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.test-entry,PROD)",
        "bigquery",
    )

    assert result is not None
    assert len(result.upstreams) == 1
    assert result.upstreams[0].type == DatasetLineageTypeClass.TRANSFORMED


def test_gen_lineage_workunits(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test generating lineage workunits."""
    # Add a lineage edge - use full dataset_id as key
    edge = LineageEdge(
        entry_id="test-project.test-dataset.upstream-entry",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id, not just entry name
    lineage_extractor.lineage_map["test-project.test-dataset.test-entry"] = {edge}

    # Generate workunits using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.test-entry,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "test-project.test-dataset.test-entry", dataset_urn, "bigquery"
        )
    )

    assert len(workunits) == 1
    assert workunits[0].metadata.entityUrn == dataset_urn  # type: ignore[union-attr]


def test_fqn_round_trip_bigquery(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction and parsing round-trip for BigQuery."""
    original_dataset_id = "my-project.my-dataset.my-table"

    fqn = lineage_extractor._construct_fqn("bigquery", original_dataset_id)

    # Parse FQN
    extracted_id = lineage_extractor._extract_entry_id_from_fqn(fqn)

    # Verify round-trip
    assert fqn == "bigquery:my-project.my-dataset.my-table"
    assert extracted_id == "my-project.my-dataset.my-table"


def test_fqn_round_trip_gcs(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test FQN construction and parsing round-trip for GCS."""
    original_dataset_id = "my-bucket/data/2024/file.csv"

    fqn = lineage_extractor._construct_fqn("gcs", original_dataset_id)

    # Parse FQN
    extracted_id = lineage_extractor._extract_entry_id_from_fqn(fqn)

    # Verify round-trip
    assert fqn == "gcs:my-bucket/data/2024/file.csv"
    assert extracted_id == "my-bucket/data/2024/file.csv"


def test_lineage_with_cross_platform_references(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test lineage extraction with cross-platform references (BigQuery -> GCS)."""
    # Mock BigQuery source with GCS upstream
    mock_bq_link = Mock()
    mock_bq_link.source.fully_qualified_name = "gcs:my-bucket/raw/data.csv"

    mock_gcs_link = Mock()
    mock_gcs_link.target.fully_qualified_name = "bigquery:my-project.analytics.final"

    def search_links_side_effect(request):
        if hasattr(request, "target") and request.target:
            return [mock_bq_link]
        elif hasattr(request, "source") and request.source:
            return [mock_gcs_link]
        return []

    lineage_extractor.lineage_client.search_links.side_effect = search_links_side_effect  # type: ignore[union-attr]

    test_entry = EntryDataTuple(
        entry_id="test-table",
        source_platform="bigquery",
        dataset_id="my-project.analytics.test-table",
    )

    result = lineage_extractor.get_lineage_for_entry("my-project", test_entry)

    assert result is not None
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 1
    assert result["upstream"][0] == "gcs:my-bucket/raw/data.csv"
    assert result["downstream"][0] == "bigquery:my-project.analytics.final"


def test_workunit_urn_structure_validation(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that generated workunits have correct URN structure."""
    # Add a lineage edge - use full dataset_id as key
    edge = LineageEdge(
        entry_id="my-project.my-dataset.upstream-table",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id
    lineage_extractor.lineage_map["my-project.my-dataset.downstream-table"] = {edge}

    # Generate workunit using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.downstream-table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "my-project.my-dataset.downstream-table", dataset_urn, "bigquery"
        )
    )

    assert len(workunits) == 1
    workunit = workunits[0]

    # Validate URN structure
    assert workunit.metadata.entityUrn == dataset_urn  # type: ignore[union-attr]
    assert workunit.metadata.entityUrn.startswith("urn:li:dataset:")  # type: ignore[union-attr]
    assert "urn:li:dataPlatform:bigquery" in workunit.metadata.entityUrn  # type: ignore[union-attr]
    assert "PROD" in workunit.metadata.entityUrn  # type: ignore[union-attr]

    # Validate aspect type
    from datahub.metadata.schema_classes import UpstreamLineageClass

    assert isinstance(workunit.metadata.aspect, UpstreamLineageClass)  # type: ignore[union-attr]


def test_workunit_aspect_completeness(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that workunit aspects contain all required fields."""
    # Add lineage edges - use full dataset_id as key
    edge1 = LineageEdge(
        entry_id="my-project.my-dataset.table1",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )
    edge2 = LineageEdge(
        entry_id="my-project.my-dataset.table2",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.COPY,
    )

    # Key is now full dataset_id
    lineage_extractor.lineage_map["my-project.my-dataset.target-table"] = {edge1, edge2}

    # Generate workunit using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.target-table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "my-project.my-dataset.target-table", dataset_urn, "bigquery"
        )
    )

    assert len(workunits) == 1
    workunit = workunits[0]
    aspect = workunit.metadata.aspect  # type: ignore[union-attr]

    # Validate UpstreamLineageClass has required fields
    assert hasattr(aspect, "upstreams")
    assert aspect.upstreams is not None  # type: ignore[union-attr]
    assert len(aspect.upstreams) == 2  # type: ignore[union-attr]

    # Validate each upstream has required fields
    for upstream in aspect.upstreams:  # type: ignore[union-attr]
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
    # Add lineage edge with specific entry ID format - use full dataset_id as key
    edge = LineageEdge(
        entry_id="test-project.sales_dataset.customer_table",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id
    lineage_extractor.lineage_map["test-project.analytics_dataset.analytics_table"] = {
        edge
    }
    lineage_extractor.config.env = "PROD"

    # Generate workunit using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.analytics_dataset.analytics_table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "test-project.analytics_dataset.analytics_table", dataset_urn, "bigquery"
        )
    )

    assert len(workunits) == 1
    upstream_urn = workunits[0].metadata.aspect.upstreams[0].dataset  # type: ignore[union-attr]

    # Validate upstream URN structure - now uses bigquery platform for consistency
    assert upstream_urn.startswith("urn:li:dataset:")
    assert "urn:li:dataPlatform:bigquery" in upstream_urn
    assert "test-project" in upstream_urn
    assert "customer_table" in upstream_urn


def test_workunit_generation_with_no_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that no workunits are generated when there's no lineage."""
    # Don't add any lineage to the map - use full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.isolated-table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "my-project.my-dataset.isolated-table", dataset_urn, "bigquery"
        )
    )

    # Should generate no workunits
    assert len(workunits) == 0


def test_pagination_automatic_handling() -> None:
    """Test that pagination is handled automatically by the client library."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
    )
    report = DataplexReport()

    # Create a mock pager that simulates multiple pages
    mock_link1 = MagicMock()
    mock_link1.source.fully_qualified_name = "bigquery:project.dataset.table1"

    mock_link2 = MagicMock()
    mock_link2.source.fully_qualified_name = "bigquery:project.dataset.table2"

    mock_link3 = MagicMock()
    mock_link3.source.fully_qualified_name = "bigquery:project.dataset.table3"

    # Mock client that returns an iterable (simulating pagination)
    mock_client = MagicMock()
    # When list() is called on search_links result, it should return all items
    mock_client.search_links.return_value = [mock_link1, mock_link2, mock_link3]

    extractor = DataplexLineageExtractor(
        config=config, report=report, lineage_client=mock_client
    )

    entry = EntryDataTuple(
        entry_id="test_table",
        source_platform="bigquery",
        dataset_id="test-project.test_dataset.test_table",
    )

    result = extractor.get_lineage_for_entry("test-project", entry)

    # Verify all items from all "pages" are retrieved
    assert result is not None
    assert len(result["upstream"]) == 3
    assert result["upstream"][0] == "bigquery:project.dataset.table1"
    assert result["upstream"][1] == "bigquery:project.dataset.table2"
    assert result["upstream"][2] == "bigquery:project.dataset.table3"


def test_pagination_with_large_result_set() -> None:
    """Test pagination handling with a large number of lineage links."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
    )
    report = DataplexReport()

    # Simulate a large result set (e.g., 100 upstream links)
    mock_links = []
    for i in range(100):
        mock_link = MagicMock()
        mock_link.source.fully_qualified_name = (
            f"bigquery:project.dataset.upstream_table_{i}"
        )
        mock_links.append(mock_link)

    mock_client = MagicMock()
    mock_client.search_links.return_value = mock_links

    extractor = DataplexLineageExtractor(
        config=config, report=report, lineage_client=mock_client
    )

    entry = EntryDataTuple(
        entry_id="target_table",
        source_platform="bigquery",
        dataset_id="test-project.analytics.target_table",
    )

    result = extractor.get_lineage_for_entry("test-project", entry)

    # Verify all 100 items are retrieved
    assert result is not None
    assert len(result["upstream"]) == 100
    # Verify first and last items
    assert result["upstream"][0] == "bigquery:project.dataset.upstream_table_0"
    assert result["upstream"][99] == "bigquery:project.dataset.upstream_table_99"


def test_batched_lineage_processing() -> None:
    """Test that lineage processing works correctly with batching enabled."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        batch_size=2,  # Small batch size to test batching
    )
    report = DataplexReport()

    # Mock lineage client
    mock_client = MagicMock()

    # Create mock lineage responses for 5 entries
    def mock_search_links(request):
        fqn = (
            request.target.fully_qualified_name
            if hasattr(request, "target")
            else request.source.fully_qualified_name
        )
        # Extract entry number from FQN
        if "entry_0" in fqn:
            mock_link = MagicMock()
            mock_link.source.fully_qualified_name = (
                "bigquery:project.dataset.upstream_0"
            )
            return [mock_link]
        elif "entry_1" in fqn:
            mock_link = MagicMock()
            mock_link.source.fully_qualified_name = (
                "bigquery:project.dataset.upstream_1"
            )
            return [mock_link]
        elif "entry_2" in fqn:
            mock_link = MagicMock()
            mock_link.source.fully_qualified_name = (
                "bigquery:project.dataset.upstream_2"
            )
            return [mock_link]
        elif "entry_3" in fqn:
            mock_link = MagicMock()
            mock_link.source.fully_qualified_name = (
                "bigquery:project.dataset.upstream_3"
            )
            return [mock_link]
        elif "entry_4" in fqn:
            mock_link = MagicMock()
            mock_link.source.fully_qualified_name = (
                "bigquery:project.dataset.upstream_4"
            )
            return [mock_link]
        return []

    mock_client.search_links.side_effect = mock_search_links

    extractor = DataplexLineageExtractor(
        config=config, report=report, lineage_client=mock_client
    )

    # Create 5 test entries
    entries = [
        EntryDataTuple(
            entry_id=f"entry_{i}",
            source_platform="bigquery",
            dataset_id=f"test-project.dataset_{i}.entry_{i}",
        )
        for i in range(5)
    ]

    # Call get_lineage_workunits which should process entries in batches of 2
    workunits = list(extractor.get_lineage_workunits("test-project", entries))

    # Should generate 5 workunits (one per entry)
    assert len(workunits) == 5

    # Verify that lineage was extracted for all entries
    assert extractor.report.num_lineage_entries_scanned == 5


def test_batched_lineage_with_batch_size_larger_than_entries() -> None:
    """Test that batching is disabled when batch size is larger than entry count."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        batch_size=100,  # Larger than entry count
    )
    report = DataplexReport()

    mock_client = MagicMock()
    # Mock to return some lineage so we can verify entries are processed
    mock_link = MagicMock()
    mock_link.source.fully_qualified_name = "bigquery:project.dataset.upstream"
    mock_client.search_links.return_value = [mock_link]

    extractor = DataplexLineageExtractor(
        config=config, report=report, lineage_client=mock_client
    )

    # Create 3 test entries
    entries = [
        EntryDataTuple(
            entry_id=f"entry_{i}",
            source_platform="bigquery",
            dataset_id=f"test-project.dataset_{i}.entry_{i}",
        )
        for i in range(3)
    ]

    # Should process all entries in a single batch
    workunits = list(extractor.get_lineage_workunits("test-project", entries))

    # Should generate 3 workunits (one per entry)
    assert len(workunits) == 3
    # Verify all entries were scanned
    assert extractor.report.num_lineage_entries_scanned == 3


def test_batched_lineage_with_batching_disabled() -> None:
    """Test that batching can be disabled by setting batch_size to None."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        batch_size=None,  # Disable batching
    )
    report = DataplexReport()

    mock_client = MagicMock()
    # Mock to return some lineage so we can verify entries are processed
    mock_link = MagicMock()
    mock_link.source.fully_qualified_name = "bigquery:project.dataset.upstream"
    mock_client.search_links.return_value = [mock_link]

    extractor = DataplexLineageExtractor(
        config=config, report=report, lineage_client=mock_client
    )

    # Create 10 test entries
    entries = [
        EntryDataTuple(
            entry_id=f"entry_{i}",
            source_platform="bigquery",
            dataset_id=f"test-project.dataset_{i}.entry_{i}",
        )
        for i in range(10)
    ]

    # Should process all entries at once (no batching)
    workunits = list(extractor.get_lineage_workunits("test-project", entries))

    # Should generate 10 workunits
    assert len(workunits) == 10
    # Verify all entries were scanned
    assert extractor.report.num_lineage_entries_scanned == 10


def test_batched_lineage_memory_cleanup() -> None:
    """Test that lineage map is cleared between batches to free memory."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        batch_size=2,  # Process 2 entries per batch
    )
    report = DataplexReport()

    mock_client = MagicMock()

    # Track lineage map size during processing
    lineage_map_sizes = []

    def mock_search_links(request):
        # Record the current lineage map size
        lineage_map_sizes.append(len(extractor.lineage_map))
        mock_link = MagicMock()
        mock_link.source.fully_qualified_name = "bigquery:project.dataset.upstream"
        return [mock_link]

    mock_client.search_links.side_effect = mock_search_links

    extractor = DataplexLineageExtractor(
        config=config, report=report, lineage_client=mock_client
    )

    # Create 6 entries (will be processed in 3 batches of 2)
    entries = [
        EntryDataTuple(
            entry_id=f"entry_{i}",
            source_platform="bigquery",
            dataset_id=f"test-project.dataset_{i}.entry_{i}",
        )
        for i in range(6)
    ]

    # Process entries
    workunits = list(extractor.get_lineage_workunits("test-project", entries))

    # Should generate 6 workunits
    assert len(workunits) == 6

    # After processing, lineage map should be empty (cleared after last batch)
    assert len(extractor.lineage_map) == 0


class TestLineageMapKeyCollision:
    """Tests for lineage map key collision fix.

    These tests verify that the lineage map uses full dataset_id (project.dataset.table)
    as the key, not just the table name. This prevents incorrect lineage assignment
    when multiple tables have the same name but exist in different datasets.

    Scenario being tested:
    - test-project.analytics.customers (table)
    - test-project.sales.customers (table) - same table name, different dataset
    - test-project.abc.users (source table)

    Lineage: test-project.abc.users -> test-project.analytics.customers (only this relationship)
    test-project.sales.customers should NOT get lineage from test-project.abc.users
    """

    def test_lineage_map_uses_full_dataset_id_as_key(self) -> None:
        """Test that lineage map keys use full dataset_id, not table name.

        This is the core test for the collision bug fix. The lineage map
        must use 'test-project.analytics.customers' as key, not 'customers'.
        """
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
        )
        report = DataplexReport()
        mock_client = MagicMock()

        # Mock lineage: users -> analytics.customers ONLY
        def mock_search_links(request):
            target_fqn = getattr(
                getattr(request, "target", None), "fully_qualified_name", ""
            )
            # Only analytics.customers has upstream from users
            if target_fqn == "bigquery:test-project.analytics.customers":
                mock_link = MagicMock()
                mock_link.source.fully_qualified_name = (
                    "bigquery:test-project.abc.users"
                )
                return [mock_link]
            # sales.customers and others have NO lineage
            return []

        mock_client.search_links.side_effect = mock_search_links

        extractor = DataplexLineageExtractor(
            config=config, report=report, lineage_client=mock_client
        )

        # Create entries with same table name but different datasets
        entries = [
            EntryDataTuple(
                entry_id="analytics_customers",
                source_platform="bigquery",
                dataset_id="test-project.analytics.customers",  # Full path
            ),
            EntryDataTuple(
                entry_id="sales_customers",
                source_platform="bigquery",
                dataset_id="test-project.sales.customers",  # Same table name, different dataset
            ),
            EntryDataTuple(
                entry_id="abc_users",
                source_platform="bigquery",
                dataset_id="test-project.abc.users",
            ),
        ]

        # Build lineage map
        lineage_map = extractor.build_lineage_map("test-project", entries)

        # KEY ASSERTION: Lineage map should have full dataset_id as key
        assert "test-project.analytics.customers" in lineage_map, (
            "Lineage map should use full dataset_id 'test-project.analytics.customers' as key"
        )

        # sales.customers should NOT be in the map (no lineage)
        assert "test-project.sales.customers" not in lineage_map, (
            "test-project.sales.customers should not have lineage"
        )

        # Verify analytics.customers has correct upstream
        analytics_edges = lineage_map["test-project.analytics.customers"]
        assert len(analytics_edges) == 1
        edge = next(iter(analytics_edges))
        assert edge.entry_id == "test-project.abc.users"

    def test_same_table_name_different_datasets_no_collision(self) -> None:
        """Test that tables with same name in different datasets don't collide.

        Scenario:
        - test-project.analytics.customers -> has lineage from test-project.abc.users
        - test-project.sales.customers -> has NO lineage

        Before fix: Both would get lineage because map used 'customers' as key
        After fix: Only analytics.customers gets lineage
        """
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
            batch_size=None,  # Disable batching for simplicity
        )
        report = DataplexReport()
        mock_client = MagicMock()

        # Track which FQNs are queried for lineage
        queried_fqns = []

        def mock_search_links(request):
            target_fqn = getattr(
                getattr(request, "target", None), "fully_qualified_name", ""
            )
            if target_fqn:
                queried_fqns.append(target_fqn)
                # Only analytics.customers has upstream
                if target_fqn == "bigquery:test-project.analytics.customers":
                    mock_link = MagicMock()
                    mock_link.source.fully_qualified_name = (
                        "bigquery:test-project.abc.users"
                    )
                    return [mock_link]
            return []

        mock_client.search_links.side_effect = mock_search_links

        extractor = DataplexLineageExtractor(
            config=config, report=report, lineage_client=mock_client
        )

        # Two tables with same name 'customers' in different datasets
        entries = [
            EntryDataTuple(
                entry_id="customers_analytics",
                source_platform="bigquery",
                dataset_id="test-project.analytics.customers",
            ),
            EntryDataTuple(
                entry_id="customers_sales",
                source_platform="bigquery",
                dataset_id="test-project.sales.customers",
            ),
        ]

        # Get lineage workunits
        workunits = list(extractor.get_lineage_workunits("test-project", entries))

        # Should have exactly 1 workunit (for analytics.customers only)
        assert len(workunits) == 1, (
            f"Expected 1 workunit for analytics.customers, got {len(workunits)}. "
            "If we got 2, the collision bug exists (sales.customers incorrectly got lineage)."
        )

        # Verify the workunit is for analytics.customers
        workunit = workunits[0]
        entity_urn = workunit.metadata.entityUrn  # type: ignore[union-attr]
        assert "analytics.customers" in entity_urn, (
            f"Workunit should be for analytics.customers, got {entity_urn}"
        )
        assert "sales.customers" not in entity_urn, (
            "sales.customers should not have a lineage workunit"
        )

    def test_lineage_lookup_uses_full_dataset_id(self) -> None:
        """Test that get_lineage_for_table uses full dataset_id for lookup.

        The lineage map is keyed by full dataset_id, so lookups must use
        the same key format to find the correct lineage.
        """
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
        )
        report = DataplexReport()

        extractor = DataplexLineageExtractor(
            config=config, report=report, lineage_client=None
        )

        # Manually populate lineage map with full dataset_id keys
        edge = LineageEdge(
            entry_id="test-project.abc.users",
            audit_stamp=datetime.datetime.now(datetime.timezone.utc),
            lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        )
        extractor.lineage_map["test-project.analytics.customers"] = {edge}

        # Lookup with full dataset_id should find lineage
        result = extractor.get_lineage_for_table(
            "test-project.analytics.customers",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.analytics.customers,PROD)",
            "bigquery",
        )
        assert result is not None, (
            "Lookup with full dataset_id 'test-project.analytics.customers' should find lineage"
        )

        # Lookup with just table name should NOT find lineage
        result_by_table_name = extractor.get_lineage_for_table(
            "customers",  # Just table name
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,customers,PROD)",
            "bigquery",
        )
        assert result_by_table_name is None, (
            "Lookup with just table name 'customers' should NOT find lineage"
        )

        # Lookup with different dataset (same table name) should NOT find lineage
        result_different_dataset = extractor.get_lineage_for_table(
            "test-project.sales.customers",  # Different dataset, same table name
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.sales.customers,PROD)",
            "bigquery",
        )
        assert result_different_dataset is None, (
            "Lookup with 'test-project.sales.customers' should NOT find lineage for analytics.customers"
        )

    def test_build_lineage_map_stores_correct_keys(self) -> None:
        """Test that build_lineage_map stores entries with correct full path keys."""
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
        )
        report = DataplexReport()
        mock_client = MagicMock()

        # Mock: users feeds into analytics.customers
        def mock_search_links(request):
            target_fqn = getattr(
                getattr(request, "target", None), "fully_qualified_name", ""
            )
            if target_fqn and "analytics.customers" in target_fqn:
                mock_link = MagicMock()
                mock_link.source.fully_qualified_name = (
                    "bigquery:test-project.abc.users"
                )
                return [mock_link]
            return []

        mock_client.search_links.side_effect = mock_search_links

        extractor = DataplexLineageExtractor(
            config=config, report=report, lineage_client=mock_client
        )

        entries = [
            EntryDataTuple(
                entry_id="analytics_customers",
                source_platform="bigquery",
                dataset_id="test-project.analytics.customers",
            ),
        ]

        lineage_map = extractor.build_lineage_map("test-project", entries)

        # Verify the key format
        keys = list(lineage_map.keys())
        assert len(keys) == 1
        assert keys[0] == "test-project.analytics.customers", (
            f"Key should be full path 'test-project.analytics.customers', got '{keys[0]}'"
        )

        # Verify the value (upstream entry)
        edges = lineage_map["test-project.analytics.customers"]
        assert len(edges) == 1
        edge = next(iter(edges))
        assert edge.entry_id == "test-project.abc.users", (
            f"Upstream should be 'test-project.abc.users', got '{edge.entry_id}'"
        )
