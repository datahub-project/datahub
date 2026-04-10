"""Unit tests for Dataplex lineage extraction."""

import datetime
from typing import cast
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
        entries_regions=["us"],
        lineage_regions=["us-central1"],
        include_lineage=True,
    )


@pytest.fixture
def dataplex_report() -> DataplexReport:
    """Create a test report."""
    return DataplexReport()


@pytest.fixture
def source_report() -> Mock:
    """Create a mock SourceReport-like object for warnings."""
    return Mock()


@pytest.fixture
def mock_lineage_client() -> Mock:
    """Create a mock LineageClient."""
    return Mock()


@pytest.fixture
def lineage_extractor(
    dataplex_config: DataplexConfig,
    dataplex_report: DataplexReport,
    source_report: Mock,
    mock_lineage_client: Mock,
) -> DataplexLineageExtractor:
    """Create a lineage extractor with mocked client."""
    return DataplexLineageExtractor(
        config=dataplex_config,
        report=dataplex_report.lineage_report,
        source_report=source_report,
        lineage_client=mock_lineage_client,
    )


def test_lineage_extractor_initialization(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that the lineage extractor initializes correctly."""
    assert lineage_extractor.config is not None
    assert lineage_extractor.report is not None
    assert lineage_extractor.lineage_client is not None
    assert callable(lineage_extractor.get_lineage_workunits)


def test_lineage_extraction_disabled(
    dataplex_report: DataplexReport,
    source_report: Mock,
) -> None:
    """Test that lineage extraction is skipped when disabled."""
    config = DataplexConfig(
        project_ids=["test-project"],
        entries_regions=["us"],
        lineage_regions=["us-central1"],
        include_lineage=False,  # Disabled
    )

    extractor = DataplexLineageExtractor(
        config=config,
        report=dataplex_report.lineage_report,
        source_report=source_report,
        lineage_client=None,
    )

    entry_data = EntryDataTuple(
        dataplex_entry_short_name="test-entry",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/test-entry",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.test-dataset.test-table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.test-dataset.test-table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )
    result = extractor.get_lineage_for_entry(
        entry_data, [("test-project", "us-central1")]
    )
    assert result is None


def test_lineage_edge_creation() -> None:
    """Test LineageEdge data structure."""
    edge = LineageEdge(
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream-entry,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    assert (
        edge.upstream_datahub_urn
        == "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream-entry,PROD)"
    )
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


def test_get_lineage_for_entry_with_upstream(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage with upstream relationships."""
    # Mock upstream link
    mock_upstream_link = Mock()
    mock_upstream_link.source.fully_qualified_name = "bigquery:project.dataset.table1"

    # Setup mock to return upstream results for target queries only.
    def search_links_side_effect(request):
        if hasattr(request, "target") and request.target:
            return [mock_upstream_link]
        return []

    lineage_extractor.lineage_client.search_links.side_effect = search_links_side_effect  # type: ignore[union-attr]

    # Create EntryDataTuple for the test entry
    test_entry = EntryDataTuple(
        dataplex_entry_short_name="test-entry",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/test-entry",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.test-dataset.test-table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.test-dataset.test-table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    # Get lineage
    result = lineage_extractor.get_lineage_for_entry(
        test_entry, [("test-project", "us-central1")]
    )

    assert result is not None
    assert "upstream" in result
    assert "downstream" in result  # retained for backward-compatible return structure
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 0
    assert result["upstream"][0] == "bigquery:project.dataset.table1"


def test_get_lineage_for_entry_uses_lineage_project_and_location_matrix(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    test_entry = EntryDataTuple(
        dataplex_entry_short_name="entry-eu",
        dataplex_entry_name="projects/p/locations/eu/entryGroups/g/entries/entry-eu",
        dataplex_location="eu",
        dataplex_entry_fqn="bigquery:test-project.ds.table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.ds.table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    lineage_extractor.config.project_ids = ["test-project", "other-project"]
    lineage_extractor.config.lineage_regions = ["us-central1", "europe-west1"]

    lineage_client_mock = cast(Mock, lineage_extractor.lineage_client)
    lineage_client_mock.search_links.return_value = []  # type: ignore[union-attr]

    lineage_extractor.get_lineage_for_entry(
        test_entry,
        [
            ("test-project", "us-central1"),
            ("test-project", "europe-west1"),
            ("other-project", "us-central1"),
            ("other-project", "europe-west1"),
        ],
    )

    assert lineage_extractor.lineage_client is not None
    calls = lineage_client_mock.search_links.call_args_list
    assert len(calls) == 4
    expected_parents = {
        "projects/test-project/locations/us-central1",
        "projects/test-project/locations/europe-west1",
        "projects/other-project/locations/us-central1",
        "projects/other-project/locations/europe-west1",
    }
    observed_parents = set()
    for call in calls:
        request = call.kwargs["request"]
        observed_parents.add(request.parent)
    assert observed_parents == expected_parents
    assert (
        lineage_extractor.report.scan_stats_by_project_location_pair[
            ("test-project", "us-central1")
        ]["calls"]
        == 1
    )
    assert (
        lineage_extractor.report.scan_stats_by_project_location_pair[
            ("test-project", "europe-west1")
        ]["calls"]
        == 1
    )
    assert (
        lineage_extractor.report.scan_stats_by_project_location_pair[
            ("other-project", "us-central1")
        ]["calls"]
        == 1
    )
    assert (
        lineage_extractor.report.scan_stats_by_project_location_pair[
            ("other-project", "europe-west1")
        ]["calls"]
        == 1
    )
    assert all(
        stats["empty"] == 1 and stats["hits"] == 0 and stats["errors"] == 0
        for stats in lineage_extractor.report.scan_stats_by_project_location_pair.values()
    )


def test_get_lineage_for_entry_uses_provided_active_project_location_pairs(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    test_entry = EntryDataTuple(
        dataplex_entry_short_name="entry-discovered",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-discovered",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.ds.table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.ds.table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    provided_pairs = [
        ("test-project", "us-central1"),
        ("other-project", "europe-west1"),
    ]

    lineage_client_mock = cast(Mock, lineage_extractor.lineage_client)
    lineage_client_mock.search_links.return_value = []  # type: ignore[union-attr]

    lineage_extractor.get_lineage_for_entry(test_entry, provided_pairs)

    calls = lineage_client_mock.search_links.call_args_list
    assert len(calls) == 2
    observed_parents = {call.kwargs["request"].parent for call in calls}
    assert observed_parents == {
        "projects/test-project/locations/us-central1",
        "projects/other-project/locations/europe-west1",
    }


def test_get_lineage_for_entry_requires_active_project_location_pairs() -> None:
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
    )
    report = DataplexReport()
    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=Mock(),
    )
    test_entry = EntryDataTuple(
        dataplex_entry_short_name="entry-required-pairs",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-required-pairs",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.ds.table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.ds.table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    # Required positional arg enforcement is handled by Python signature.
    with pytest.raises(TypeError):
        extractor.get_lineage_for_entry(test_entry)  # type: ignore[call-arg]


def test_get_lineage_for_entry_keeps_duplicates_from_multiple_regions(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    test_entry = EntryDataTuple(
        dataplex_entry_short_name="entry-dedup",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-dedup",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.ds.table_dedup",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.ds.table_dedup",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    lineage_extractor.config.lineage_regions = ["us-central1", "us-east1"]

    duplicate_link = Mock()
    duplicate_link.name = "projects/123/locations/us-central1/links/p:shared"
    duplicate_link.source.fully_qualified_name = (
        "bigquery:project.dataset.same_upstream"
    )

    lineage_client_mock = cast(Mock, lineage_extractor.lineage_client)
    lineage_client_mock.search_links.return_value = [duplicate_link]  # type: ignore[union-attr]

    result = lineage_extractor.get_lineage_for_entry(
        test_entry,
        [("test-project", "us-central1"), ("test-project", "us-east1")],
    )

    assert result is not None
    assert result["upstream"] == [
        "bigquery:project.dataset.same_upstream",
        "bigquery:project.dataset.same_upstream",
    ]


def test_get_lineage_for_entry_continues_after_single_region_error(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    test_entry = EntryDataTuple(
        dataplex_entry_short_name="entry-error-isolation",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry-error-isolation",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.ds.table_error_isolation",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.ds.table_error_isolation",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    lineage_extractor.config.project_ids = ["test-project"]
    lineage_extractor.config.lineage_regions = ["me-central2", "us-central1"]

    successful_link = Mock()
    successful_link.source.fully_qualified_name = "bigquery:project.dataset.upstream_ok"

    def search_links_side_effect(request):
        if request.parent.endswith("/locations/me-central2"):
            raise RuntimeError("region unsupported")
        return [successful_link]

    lineage_client_mock = cast(Mock, lineage_extractor.lineage_client)
    lineage_client_mock.search_links.side_effect = search_links_side_effect  # type: ignore[union-attr]

    result = lineage_extractor.get_lineage_for_entry(
        test_entry,
        [("test-project", "me-central2"), ("test-project", "us-central1")],
    )

    assert result is not None
    assert result["upstream"] == ["bigquery:project.dataset.upstream_ok"]
    stats = lineage_extractor.report.scan_stats_by_project_location_pair
    assert stats[("test-project", "me-central2")] == {
        "calls": 1,
        "hits": 0,
        "empty": 0,
        "errors": 1,
    }
    assert stats[("test-project", "us-central1")] == {
        "calls": 1,
        "hits": 1,
        "empty": 0,
        "errors": 0,
    }


def test_get_lineage_for_table_deduplicates_same_upstream_dataset() -> None:
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
    )
    report = DataplexReport()
    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=None,
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    duplicate_upstream = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.ds.shared_upstream,PROD)"

    # Simulate duplicate lineage observations for the same upstream dataset.
    extractor.lineage_by_full_dataset_id["test-project.ds.target"] = {
        LineageEdge(
            upstream_datahub_urn=duplicate_upstream,
            audit_stamp=now,
            lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        ),
        LineageEdge(
            upstream_datahub_urn=duplicate_upstream,
            audit_stamp=now + datetime.timedelta(seconds=1),
            lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        ),
    }

    result = extractor.get_lineage_for_table(
        "test-project.ds.target",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.ds.target,PROD)",
    )

    assert result is not None
    assert len(result.upstreams) == 1
    assert result.upstreams[0].dataset == duplicate_upstream


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
            dataplex_entry_short_name="entry1",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry1",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:test-project.dataset.table1",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="test-project.dataset.table1",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        ),
        EntryDataTuple(
            dataplex_entry_short_name="entry2",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry2",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:test-project.dataset.table2",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="test-project.dataset.table2",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        ),
    ]

    # Build lineage map
    lineage_by_full_dataset_id = lineage_extractor.build_lineage_map(entry_data)

    assert isinstance(lineage_by_full_dataset_id, dict)
    assert lineage_extractor.report.num_lineage_entries_scanned == 2
    assert lineage_extractor.report.num_lineage_entries_processed == 2
    assert lineage_extractor.report.num_lineage_entries_without_lineage == 0
    assert lineage_extractor.report.num_lineage_upstream_fqns_skipped == 0
    assert lineage_extractor.report.num_lineage_edges_added == 2


def test_get_lineage_for_table_no_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage for table with no lineage."""
    result = lineage_extractor.get_lineage_for_table(
        "test-project.test-dataset.entry-with-no-lineage",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.entry-with-no-lineage,PROD)",
    )

    assert result is None


def test_get_lineage_for_table_with_lineage(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test getting lineage for table with lineage."""
    # Add a lineage edge to the map - use full dataset_id as key
    edge = LineageEdge(
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.upstream-entry,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id, not just entry name
    lineage_extractor.lineage_by_full_dataset_id[
        "test-project.test-dataset.test-entry"
    ] = {edge}

    # Get lineage using full dataset_id
    result = lineage_extractor.get_lineage_for_table(
        "test-project.test-dataset.test-entry",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.test-entry,PROD)",
    )

    assert result is not None
    assert len(result.upstreams) == 1
    assert result.upstreams[0].type == DatasetLineageTypeClass.TRANSFORMED


def test_gen_lineage_workunits(lineage_extractor: DataplexLineageExtractor) -> None:
    """Test generating lineage workunits."""
    # Add a lineage edge - use full dataset_id as key
    edge = LineageEdge(
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.upstream-entry,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id, not just entry name
    lineage_extractor.lineage_by_full_dataset_id[
        "test-project.test-dataset.test-entry"
    ] = {edge}

    # Generate workunits using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.test-dataset.test-entry,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "test-project.test-dataset.test-entry", dataset_urn
        )
    )

    assert len(workunits) == 1
    assert workunits[0].metadata.entityUrn == dataset_urn  # type: ignore[union-attr]


def test_lineage_with_cross_platform_references(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test lineage extraction with cross-platform references (BigQuery -> GCS)."""
    # Mock BigQuery source with GCS upstream
    mock_bq_link = Mock()
    mock_bq_link.source.fully_qualified_name = "gcs:my-bucket/raw/data.csv"

    def search_links_side_effect(request):
        if hasattr(request, "target") and request.target:
            return [mock_bq_link]
        return []

    lineage_extractor.lineage_client.search_links.side_effect = search_links_side_effect  # type: ignore[union-attr]

    test_entry = EntryDataTuple(
        dataplex_entry_short_name="test-table",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/test-table",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:my-project.analytics.test-table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="my-project.analytics.test-table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    result = lineage_extractor.get_lineage_for_entry(
        test_entry,
        [("test-project", "us-central1")],
    )

    assert result is not None
    assert len(result["upstream"]) == 1
    assert len(result["downstream"]) == 0
    assert result["upstream"][0] == "gcs:my-bucket/raw/data.csv"

    # GCS upstream is not yet part of DATAPLEX_ENTRY_TYPE_MAPPINGS datasets.
    # Confirm it is skipped during edge normalization.
    lineage_map = lineage_extractor.build_lineage_map(
        [test_entry],
        active_lineage_project_location_pairs=[("test-project", "us-central1")],
    )
    assert lineage_map == {}


def test_build_lineage_map_skips_unsupported_upstream_platform() -> None:
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
    )
    report = DataplexReport()
    mock_client = MagicMock()

    # Return an upstream lineage edge with unsupported platform prefix.
    mock_link = MagicMock()
    mock_link.source.fully_qualified_name = "unknown:project.dataset.upstream"
    mock_client.search_links.return_value = [mock_link]

    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    entries = [
        EntryDataTuple(
            dataplex_entry_short_name="entry_1",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry_1",
            dataplex_location="us",
            dataplex_entry_type_short_name="bigquery-table",
            dataplex_entry_fqn="bigquery:test-project.dataset.table_1",
            datahub_platform="bigquery",
            datahub_dataset_name="test-project.dataset.table_1",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
    ]

    lineage_map = extractor.build_lineage_map(
        entries,
        active_lineage_project_location_pairs=[("test-project", "us-central1")],
    )
    assert lineage_map == {}
    assert report.lineage_report.num_lineage_entries_without_lineage == 0
    assert report.lineage_report.num_lineage_upstream_fqns_skipped == 1
    assert report.lineage_report.num_lineage_edges_added == 0


def test_build_lineage_map_parses_cross_platform_upstream_fqn() -> None:
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
    )
    report = DataplexReport()
    mock_client = MagicMock()

    # Downstream BigQuery table has an upstream Pub/Sub topic.
    mock_link = MagicMock()
    mock_link.source.fully_qualified_name = "pubsub:topic:acryl-staging.observe-topic"
    mock_client.search_links.return_value = [mock_link]

    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    entries = [
        EntryDataTuple(
            dataplex_entry_short_name="entry_1",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/entry_1",
            dataplex_location="us",
            dataplex_entry_type_short_name="bigquery-table",
            dataplex_entry_fqn="bigquery:test-project.dataset.table_1",
            datahub_platform="bigquery",
            datahub_dataset_name="test-project.dataset.table_1",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
    ]

    lineage_map = extractor.build_lineage_map(
        entries,
        active_lineage_project_location_pairs=[("test-project", "us-central1")],
    )
    assert "test-project.dataset.table_1" in lineage_map
    edges = lineage_map["test-project.dataset.table_1"]
    assert len(edges) == 1
    edge = next(iter(edges))
    assert (
        edge.upstream_datahub_urn
        == "urn:li:dataset:(urn:li:dataPlatform:pubsub,acryl-staging.observe-topic,PROD)"
    )
    assert report.lineage_report.num_lineage_upstream_fqns_skipped == 0


def test_workunit_urn_structure_validation(
    lineage_extractor: DataplexLineageExtractor,
) -> None:
    """Test that generated workunits have correct URN structure."""
    # Add a lineage edge - use full dataset_id as key
    edge = LineageEdge(
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.upstream-table,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id
    lineage_extractor.lineage_by_full_dataset_id[
        "my-project.my-dataset.downstream-table"
    ] = {edge}

    # Generate workunit using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.downstream-table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "my-project.my-dataset.downstream-table", dataset_urn
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
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.table1,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )
    edge2 = LineageEdge(
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.table2,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.COPY,
    )

    # Key is now full dataset_id
    lineage_extractor.lineage_by_full_dataset_id[
        "my-project.my-dataset.target-table"
    ] = {edge1, edge2}

    # Generate workunit using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.target-table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage("my-project.my-dataset.target-table", dataset_urn)
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
        upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.sales_dataset.customer_table,PROD)",
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Key is now full dataset_id
    lineage_extractor.lineage_by_full_dataset_id[
        "test-project.analytics_dataset.analytics_table"
    ] = {edge}
    lineage_extractor.config.env = "PROD"

    # Generate workunit using full dataset_id
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.analytics_dataset.analytics_table,PROD)"
    workunits = list(
        lineage_extractor.gen_lineage(
            "test-project.analytics_dataset.analytics_table", dataset_urn
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
            "my-project.my-dataset.isolated-table", dataset_urn
        )
    )

    # Should generate no workunits
    assert len(workunits) == 0


def test_pagination_automatic_handling() -> None:
    """Test that pagination is handled automatically by the client library."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
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
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    entry = EntryDataTuple(
        dataplex_entry_short_name="test_table",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/test_table",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.test_dataset.test_table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.test_dataset.test_table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    result = extractor.get_lineage_for_entry(
        entry,
        [("test-project", "us-central1")],
    )

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
        lineage_regions=["us-central1"],
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
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    entry = EntryDataTuple(
        dataplex_entry_short_name="target_table",
        dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/target_table",
        dataplex_location="us",
        dataplex_entry_fqn="bigquery:test-project.analytics.target_table",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name="test-project.analytics.target_table",
        datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
    )

    result = extractor.get_lineage_for_entry(
        entry,
        [("test-project", "us-central1")],
    )

    # Verify all 100 items are retrieved
    assert result is not None
    assert len(result["upstream"]) == 100
    # Verify first and last items
    assert result["upstream"][0] == "bigquery:project.dataset.upstream_table_0"
    assert result["upstream"][99] == "bigquery:project.dataset.upstream_table_99"


def test_streaming_lineage_processing() -> None:
    """Test that lineage processing works correctly in streaming mode."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
        batch_size=2,  # Should be ignored in streaming mode
    )
    report = DataplexReport()

    # Mock lineage client
    mock_client = MagicMock()

    # Create mock lineage responses for 5 entries
    def mock_search_links(request):
        fqn = request.target.fully_qualified_name
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
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    # Create 5 test entries
    entries = [
        EntryDataTuple(
            dataplex_entry_short_name=f"entry_{i}",
            dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/entry_{i}",
            dataplex_location="us",
            dataplex_entry_fqn=f"bigquery:test-project.dataset_{i}.entry_{i}",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name=f"test-project.dataset_{i}.entry_{i}",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
        for i in range(5)
    ]

    # Call get_lineage_workunits which should process entries in streaming mode
    workunits = list(
        extractor.get_lineage_workunits(
            entries,
            active_lineage_project_location_pairs=[("test-project", "us-central1")],
        )
    )

    # Should generate 5 workunits (one per entry)
    assert len(workunits) == 5

    # Verify that lineage was extracted for all entries
    assert extractor.report.num_lineage_entries_scanned == 5


def test_streaming_lineage_ignores_large_batch_size_config() -> None:
    """Test that a large batch_size config does not affect streaming behavior."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
        batch_size=100,  # Should be ignored in streaming mode
    )
    report = DataplexReport()

    mock_client = MagicMock()
    # Mock to return some lineage so we can verify entries are processed
    mock_link = MagicMock()
    mock_link.source.fully_qualified_name = "bigquery:project.dataset.upstream"
    mock_client.search_links.return_value = [mock_link]

    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    # Create 3 test entries
    entries = [
        EntryDataTuple(
            dataplex_entry_short_name=f"entry_{i}",
            dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/entry_{i}",
            dataplex_location="us",
            dataplex_entry_fqn=f"bigquery:test-project.dataset_{i}.entry_{i}",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name=f"test-project.dataset_{i}.entry_{i}",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
        for i in range(3)
    ]

    # Should process all entries in streaming mode
    workunits = list(
        extractor.get_lineage_workunits(
            entries,
            active_lineage_project_location_pairs=[("test-project", "us-central1")],
        )
    )

    # Should generate 3 workunits (one per entry)
    assert len(workunits) == 3
    # Verify all entries were scanned
    assert extractor.report.num_lineage_entries_scanned == 3


def test_streaming_lineage_with_batching_disabled_config() -> None:
    """Test streaming lineage when batch_size is explicitly set to None."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
        batch_size=None,  # Still streaming mode
    )
    report = DataplexReport()

    mock_client = MagicMock()
    # Mock to return some lineage so we can verify entries are processed
    mock_link = MagicMock()
    mock_link.source.fully_qualified_name = "bigquery:project.dataset.upstream"
    mock_client.search_links.return_value = [mock_link]

    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    # Create 10 test entries
    entries = [
        EntryDataTuple(
            dataplex_entry_short_name=f"entry_{i}",
            dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/entry_{i}",
            dataplex_location="us",
            dataplex_entry_fqn=f"bigquery:test-project.dataset_{i}.entry_{i}",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name=f"test-project.dataset_{i}.entry_{i}",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
        for i in range(10)
    ]

    # Should process all entries in streaming mode
    workunits = list(
        extractor.get_lineage_workunits(
            entries,
            active_lineage_project_location_pairs=[("test-project", "us-central1")],
        )
    )

    # Should generate 10 workunits
    assert len(workunits) == 10
    # Verify all entries were scanned
    assert extractor.report.num_lineage_entries_scanned == 10


def test_streaming_lineage_memory_cleanup() -> None:
    """Test that transient lineage map entries are cleared after emission."""
    config = DataplexConfig(
        project_ids=["test-project"],
        include_lineage=True,
        lineage_regions=["us-central1"],
        batch_size=2,  # Should be ignored in streaming mode
    )
    report = DataplexReport()

    mock_client = MagicMock()

    # Track lineage map size during processing
    lineage_by_full_dataset_id_sizes = []

    def mock_search_links(request):
        # Record the current lineage map size
        lineage_by_full_dataset_id_sizes.append(
            len(extractor.lineage_by_full_dataset_id)
        )
        mock_link = MagicMock()
        mock_link.source.fully_qualified_name = "bigquery:project.dataset.upstream"
        return [mock_link]

    mock_client.search_links.side_effect = mock_search_links

    extractor = DataplexLineageExtractor(
        config=config,
        report=report.lineage_report,
        source_report=Mock(),
        lineage_client=mock_client,
    )

    # Create 6 entries
    entries = [
        EntryDataTuple(
            dataplex_entry_short_name=f"entry_{i}",
            dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/entry_{i}",
            dataplex_location="us",
            dataplex_entry_fqn=f"bigquery:test-project.dataset_{i}.entry_{i}",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name=f"test-project.dataset_{i}.entry_{i}",
            datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
        )
        for i in range(6)
    ]

    # Process entries
    workunits = list(
        extractor.get_lineage_workunits(
            entries,
            active_lineage_project_location_pairs=[("test-project", "us-central1")],
        )
    )

    # Should generate 6 workunits
    assert len(workunits) == 6

    # After processing, lineage map should be empty (entry-level cleanup)
    assert len(extractor.lineage_by_full_dataset_id) == 0


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

    def test_lineage_by_full_dataset_id_uses_full_dataset_id_as_key(self) -> None:
        """Test that lineage map keys use full dataset_id, not table name.

        This is the core test for the collision bug fix. The lineage map
        must use 'test-project.analytics.customers' as key, not 'customers'.
        """
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
            lineage_regions=["us-central1"],
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
            config=config,
            report=report.lineage_report,
            source_report=Mock(),
            lineage_client=mock_client,
        )

        # Create entries with same table name but different datasets
        entries = [
            EntryDataTuple(
                dataplex_entry_short_name="analytics_customers",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/analytics_customers",
                dataplex_location="us",
                dataplex_entry_fqn="bigquery:test-project.analytics.customers",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="test-project.analytics.customers",  # Full path
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
            ),
            EntryDataTuple(
                dataplex_entry_short_name="sales_customers",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/sales_customers",
                dataplex_location="us",
                dataplex_entry_fqn="bigquery:test-project.sales.customers",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="test-project.sales.customers",  # Same table name, different dataset
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
            ),
            EntryDataTuple(
                dataplex_entry_short_name="abc_users",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/abc_users",
                dataplex_location="us",
                dataplex_entry_fqn="bigquery:test-project.abc.users",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="test-project.abc.users",
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
            ),
        ]

        # Build lineage map
        lineage_by_full_dataset_id = extractor.build_lineage_map(
            entries,
            active_lineage_project_location_pairs=[("test-project", "us-central1")],
        )

        # KEY ASSERTION: Lineage map should have full dataset_id as key
        assert "test-project.analytics.customers" in lineage_by_full_dataset_id, (
            "Lineage map should use full dataset_id 'test-project.analytics.customers' as key"
        )

        # sales.customers should NOT be in the map (no lineage)
        assert "test-project.sales.customers" not in lineage_by_full_dataset_id, (
            "test-project.sales.customers should not have lineage"
        )

        # Verify analytics.customers has correct upstream
        analytics_edges = lineage_by_full_dataset_id["test-project.analytics.customers"]
        assert len(analytics_edges) == 1
        edge = next(iter(analytics_edges))
        assert (
            edge.upstream_datahub_urn
            == "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.abc.users,PROD)"
        )

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
            lineage_regions=["us-central1"],
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
            config=config,
            report=report.lineage_report,
            source_report=Mock(),
            lineage_client=mock_client,
        )

        # Two tables with same name 'customers' in different datasets
        entries = [
            EntryDataTuple(
                dataplex_entry_short_name="customers_analytics",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/customers_analytics",
                dataplex_location="us",
                dataplex_entry_fqn="bigquery:test-project.analytics.customers",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="test-project.analytics.customers",
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
            ),
            EntryDataTuple(
                dataplex_entry_short_name="customers_sales",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/customers_sales",
                dataplex_location="us",
                dataplex_entry_fqn="bigquery:test-project.sales.customers",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="test-project.sales.customers",
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
            ),
        ]

        # Get lineage workunits
        workunits = list(
            extractor.get_lineage_workunits(
                entries,
                active_lineage_project_location_pairs=[("test-project", "us-central1")],
            )
        )

        # Should have exactly 1 workunit (for analytics.customers only)
        assert len(workunits) == 1, (
            f"Expected 1 workunit for analytics.customers, got {len(workunits)}. "
            "If we got 2, the collision bug exists (sales.customers incorrectly got lineage)."
        )

        # Verify the workunit is for analytics.customers
        workunit = workunits[0]
        entity_urn = workunit.metadata.entityUrn  # type: ignore[union-attr]
        assert entity_urn is not None
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
            lineage_regions=["us-central1"],
        )
        report = DataplexReport()

        extractor = DataplexLineageExtractor(
            config=config,
            report=report.lineage_report,
            source_report=Mock(),
            lineage_client=None,
        )

        # Manually populate lineage map with full dataset_id keys
        edge = LineageEdge(
            upstream_datahub_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.abc.users,PROD)",
            audit_stamp=datetime.datetime.now(datetime.timezone.utc),
            lineage_type=DatasetLineageTypeClass.TRANSFORMED,
        )
        extractor.lineage_by_full_dataset_id["test-project.analytics.customers"] = {
            edge
        }

        # Lookup with full dataset_id should find lineage
        result = extractor.get_lineage_for_table(
            "test-project.analytics.customers",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.analytics.customers,PROD)",
        )
        assert result is not None, (
            "Lookup with full dataset_id 'test-project.analytics.customers' should find lineage"
        )

        # Lookup with just table name should NOT find lineage
        result_by_table_name = extractor.get_lineage_for_table(
            "customers",  # Just table name
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,customers,PROD)",
        )
        assert result_by_table_name is None, (
            "Lookup with just table name 'customers' should NOT find lineage"
        )

        # Lookup with different dataset (same table name) should NOT find lineage
        result_different_dataset = extractor.get_lineage_for_table(
            "test-project.sales.customers",  # Different dataset, same table name
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.sales.customers,PROD)",
        )
        assert result_different_dataset is None, (
            "Lookup with 'test-project.sales.customers' should NOT find lineage for analytics.customers"
        )

    def test_build_lineage_map_stores_correct_keys(self) -> None:
        """Test that build_lineage_map stores entries with correct full path keys."""
        config = DataplexConfig(
            project_ids=["test-project"],
            include_lineage=True,
            lineage_regions=["us-central1"],
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
            config=config,
            report=report.lineage_report,
            source_report=Mock(),
            lineage_client=mock_client,
        )

        entries = [
            EntryDataTuple(
                dataplex_entry_short_name="analytics_customers",
                dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/analytics_customers",
                dataplex_location="us",
                dataplex_entry_fqn="bigquery:test-project.analytics.customers",
                dataplex_entry_type_short_name="bigquery-table",
                datahub_platform="bigquery",
                datahub_dataset_name="test-project.analytics.customers",
                datahub_dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,test-placeholder,PROD)",
            ),
        ]

        lineage_by_full_dataset_id = extractor.build_lineage_map(
            entries,
            active_lineage_project_location_pairs=[("test-project", "us-central1")],
        )

        # Verify the key format
        keys = list(lineage_by_full_dataset_id.keys())
        assert len(keys) == 1
        assert keys[0] == "test-project.analytics.customers", (
            f"Key should be full path 'test-project.analytics.customers', got '{keys[0]}'"
        )

        # Verify the value (upstream entry)
        edges = lineage_by_full_dataset_id["test-project.analytics.customers"]
        assert len(edges) == 1
        edge = next(iter(edges))
        assert (
            edge.upstream_datahub_urn
            == "urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.abc.users,PROD)"
        ), (
            f"Upstream should be 'urn:li:dataset:(urn:li:dataPlatform:bigquery,test-project.abc.users,PROD)', got '{edge.upstream_datahub_urn}'"
        )
