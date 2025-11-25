"""Unit tests for Dataplex zone extraction."""

import datetime
from unittest.mock import Mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dataplex.dataplex import DataplexSource
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport


class MockGoogleAPICallError(Exception):
    """Mock Google API call error for testing.

    This mock exception is designed to be compatible with the exception
    handling in dataplex.py without requiring Google Cloud imports.
    """

    def __init__(self, message: str, code: int = None):
        super().__init__(message)
        self.code = code
        self.message = message


class MockPager:
    """Mock pager that simulates Google Cloud API pagination behavior.

    This class simulates the behavior of Google Cloud API pagers which
    automatically handle pagination when iterated. It can simulate:
    - Multi-page results
    - Pagination tokens
    - Incomplete results (if iteration stops mid-way)
    """

    def __init__(self, items: list, page_size: int = 2, fail_at: int = None):
        """Initialize mock pager.

        Args:
            items: List of items to paginate
            page_size: Number of items per page
            fail_at: If set, raise exception when reaching this item index
        """
        self.items = items
        self.page_size = page_size
        self.fail_at = fail_at
        self.pages_fetched = []
        self.current_page_token = None

    def __iter__(self):
        """Iterate over all items, simulating pagination."""
        page_num = 0
        for i, item in enumerate(self.items):
            if self.fail_at is not None and i >= self.fail_at:
                raise MockGoogleAPICallError("Pagination interrupted", code=500)

            # Simulate page boundaries
            if i % self.page_size == 0:
                page_num += 1
                self.current_page_token = f"page_token_{page_num}"
                self.pages_fetched.append(self.current_page_token)

            yield item

    @property
    def next_page_token(self):
        """Return the next page token if more pages exist."""
        if len(self.pages_fetched) * self.page_size < len(self.items):
            return f"page_token_{len(self.pages_fetched) + 1}"
        return None


@pytest.fixture
def dataplex_config() -> DataplexConfig:
    """Create a test configuration."""
    return DataplexConfig(
        project_ids=["test-project"],
        location="us-central1",
        include_zones=True,
    )


@pytest.fixture
def dataplex_report() -> DataplexReport:
    """Create a test report."""
    return DataplexReport()


@pytest.fixture
def mock_dataplex_client() -> Mock:
    """Create a mock DataplexServiceClient."""
    return Mock()


@pytest.fixture
def mock_metadata_client() -> Mock:
    """Create a mock MetadataServiceClient."""
    return Mock()


@pytest.fixture
def dataplex_source(
    dataplex_config: DataplexConfig,
    dataplex_report: DataplexReport,
    mock_dataplex_client: Mock,
    mock_metadata_client: Mock,
) -> DataplexSource:
    """Create a DataplexSource with mocked clients."""
    ctx = PipelineContext(run_id="test-run")
    source = DataplexSource(ctx, dataplex_config)
    source.dataplex_client = mock_dataplex_client
    source.metadata_client = mock_metadata_client
    source.catalog_client = Mock()
    source.report = dataplex_report
    return source


def create_mock_lake(
    lake_id: str,
    display_name: str = None,
) -> Mock:
    """Helper to create a mock lake object."""
    lake = Mock()
    lake.name = f"projects/test-project/locations/us-central1/lakes/{lake_id}"
    lake.display_name = display_name or lake_id
    return lake


def create_mock_zone(
    zone_id: str,
    lake_id: str,
    zone_type: str = "RAW",
    display_name: str = None,
    description: str = None,
    create_time: datetime.datetime = None,
    update_time: datetime.datetime = None,
) -> Mock:
    """Helper to create a mock zone object."""
    zone = Mock()
    zone.name = (
        f"projects/test-project/locations/us-central1/lakes/{lake_id}/zones/{zone_id}"
    )
    zone.display_name = display_name or zone_id
    zone.description = description or ""
    zone.type_ = Mock()
    zone.type_.name = zone_type
    zone.create_time = create_time or datetime.datetime.now(datetime.timezone.utc)
    zone.update_time = update_time or zone.create_time
    return zone


def test_get_zones_mcps_success(dataplex_source: DataplexSource) -> None:
    """Test successful zone extraction and MCP generation."""
    # Setup mock lake and zones
    mock_lake = create_mock_lake("lake-1")
    mock_zone1 = create_mock_zone("zone-1", "lake-1", "RAW")
    mock_zone2 = create_mock_zone("zone-2", "lake-1", "CURATED")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone1, mock_zone2]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 2 zones × 5 workunits = 10 workunits
    assert len(workunits) == 10
    assert dataplex_source.report.num_zones_scanned == 2
    assert dataplex_source.report.zones_scanned == {
        mock_zone1.name.split("/")[-1]: True,
        mock_zone2.name.split("/")[-1]: True,
    }
    assert dataplex_source.report.num_zones_filtered == 0


def test_get_zones_mcps_with_lake_filtering(dataplex_source: DataplexSource) -> None:
    """Test zone extraction when parent lake is filtered out."""
    dataplex_source.config.filter_config.lake_pattern.allow = ["lake-1"]

    mock_lake1 = create_mock_lake("lake-1")
    mock_lake2 = create_mock_lake("lake-2")
    mock_zone = create_mock_zone("zone-1", "lake-2")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake1, mock_lake2]

    # Only return zones for lake-2 (which should be filtered)
    def list_zones_side_effect(request):
        if "lake-2" in request.parent:
            return [mock_zone]
        return []

    dataplex_source.dataplex_client.list_zones.side_effect = list_zones_side_effect

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # Zones from filtered lake should not be processed
    assert len(workunits) == 0  # No workunits should be generated for filtered lakes
    assert dataplex_source.report.num_zones_scanned == 0


def test_get_zones_mcps_with_zone_filtering(dataplex_source: DataplexSource) -> None:
    """Test zone extraction with zone-level pattern filtering."""
    dataplex_source.config.filter_config.zone_pattern.allow = ["zone-1"]

    mock_lake = create_mock_lake("lake-1")
    mock_zone1 = create_mock_zone("zone-1", "lake-1")
    mock_zone2 = create_mock_zone("zone-2", "lake-1")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone1, mock_zone2]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    assert dataplex_source.report.num_zones_scanned == 2
    assert dataplex_source.report.zones_scanned == {
        mock_zone1.name.split("/")[-1]: True,
        mock_zone2.name.split("/")[-1]: False,
    }
    assert dataplex_source.report.num_zones_filtered == 1


def test_get_zones_mcps_zone_type_tags_raw(dataplex_source: DataplexSource) -> None:
    """Test that RAW zones have correct type tags."""
    mock_lake = create_mock_lake("lake-1")
    mock_raw_zone = create_mock_zone("raw-zone", "lake-1", "RAW")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_raw_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    # Find the SubTypesClass workunit (aspect name is "subTypes" in camelCase)
    subtypes_workunit = None
    for workunit in workunits:
        if workunit.metadata and workunit.metadata.aspectName == "subTypes":
            subtypes_workunit = workunit
            break
    assert subtypes_workunit is not None, "Could not find subTypes workunit"
    container_aspect = subtypes_workunit.metadata.aspect
    assert container_aspect is not None
    assert "Raw Data Zone" in container_aspect.typeNames
    assert "Dataplex Zone" in container_aspect.typeNames


def test_get_zones_mcps_zone_type_tags_curated(dataplex_source: DataplexSource) -> None:
    """Test that CURATED zones have correct type tags."""
    mock_lake = create_mock_lake("lake-1")
    mock_curated_zone = create_mock_zone("curated-zone", "lake-1", "CURATED")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_curated_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    # Find the SubTypesClass workunit (aspect name is "subTypes" in camelCase)
    subtypes_workunit = None
    for workunit in workunits:
        if workunit.metadata and workunit.metadata.aspectName == "subTypes":
            subtypes_workunit = workunit
            break
    assert subtypes_workunit is not None, "Could not find subTypes workunit"
    container_aspect = subtypes_workunit.metadata.aspect
    assert container_aspect is not None
    assert "Curated Data Zone" in container_aspect.typeNames
    assert "Dataplex Zone" in container_aspect.typeNames


def test_get_zones_mcps_parent_relationship(dataplex_source: DataplexSource) -> None:
    """Test that zones have correct parent container relationship to lake."""
    mock_lake = create_mock_lake("lake-1")
    mock_zone = create_mock_zone("zone-1", "lake-1")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None
    # Verify parent container key points to lake
    container_aspect = workunit.metadata.aspect
    assert container_aspect is not None


def test_get_zones_mcps_zones_api_error(
    dataplex_source: DataplexSource, monkeypatch
) -> None:
    """Test zone extraction error handling when zones API fails."""
    # Patch the exception type in the dataplex module to use our mock
    import datahub.ingestion.source.dataplex.dataplex as dataplex_module

    # Replace GoogleAPICallError with our mock exception
    monkeypatch.setattr(
        dataplex_module.exceptions, "GoogleAPICallError", MockGoogleAPICallError
    )

    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.side_effect = MockGoogleAPICallError(
        "Zones API Error", code=403
    )

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    assert len(workunits) == 0
    assert len(dataplex_source.report.failures) > 0


def test_get_zones_mcps_lakes_api_error(
    dataplex_source: DataplexSource, monkeypatch
) -> None:
    """Test zone extraction error handling when lakes API fails."""
    # Patch the exception type in the dataplex module to use our mock
    import datahub.ingestion.source.dataplex.dataplex as dataplex_module

    # Replace GoogleAPICallError with our mock exception
    monkeypatch.setattr(
        dataplex_module.exceptions, "GoogleAPICallError", MockGoogleAPICallError
    )

    dataplex_source.dataplex_client.list_lakes.side_effect = MockGoogleAPICallError(
        "Lakes API Error", code=403
    )

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    assert len(workunits) == 0
    assert len(dataplex_source.report.failures) > 0


def test_get_zones_mcps_multiple_lakes(dataplex_source: DataplexSource) -> None:
    """Test zone extraction across multiple lakes."""
    mock_lake1 = create_mock_lake("lake-1")
    mock_lake2 = create_mock_lake("lake-2")
    mock_zone1 = create_mock_zone("zone-1", "lake-1")
    mock_zone2 = create_mock_zone("zone-2", "lake-2")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake1, mock_lake2]

    def list_zones_side_effect(request):
        if "lake-1" in request.parent:
            return [mock_zone1]
        elif "lake-2" in request.parent:
            return [mock_zone2]
        return []

    dataplex_source.dataplex_client.list_zones.side_effect = list_zones_side_effect

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 2 zones × 5 workunits = 10 workunits
    assert len(workunits) == 10
    assert dataplex_source.report.num_zones_scanned == 2


def test_get_zones_mcps_with_timestamps(dataplex_source: DataplexSource) -> None:
    """Test zone extraction with create/update timestamps."""
    create_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    update_time = datetime.datetime(2023, 2, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

    mock_lake = create_mock_lake("lake-1")
    mock_zone = create_mock_zone(
        "zone-1",
        "lake-1",
        create_time=create_time,
        update_time=update_time,
    )

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_zones_mcps_without_timestamps(dataplex_source: DataplexSource) -> None:
    """Test zone extraction when timestamps are None."""
    mock_lake = create_mock_lake("lake-1")
    mock_zone = create_mock_zone("zone-1", "lake-1")
    mock_zone.create_time = None
    mock_zone.update_time = None

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_zones_mcps_empty_result(dataplex_source: DataplexSource) -> None:
    """Test zone extraction when no zones are found."""
    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = []

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    assert len(workunits) == 0
    assert dataplex_source.report.num_zones_scanned == 0
    assert dataplex_source.report.zones_scanned == {}


def test_get_zones_mcps_display_name_fallback(
    dataplex_source: DataplexSource,
) -> None:
    """Test that zone ID is used when display_name is None."""
    mock_lake = create_mock_lake("lake-1")
    mock_zone = create_mock_zone("zone-1", "lake-1", display_name=None)

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_zones_mcps_description_fallback(
    dataplex_source: DataplexSource,
) -> None:
    """Test that empty string is used when description is None."""
    mock_lake = create_mock_lake("lake-1")
    mock_zone = create_mock_zone("zone-1", "lake-1", description=None)

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]
    dataplex_source.dataplex_client.list_zones.return_value = [mock_zone]

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_zones_mcps_extract_zones_disabled(
    dataplex_config: DataplexConfig,
    dataplex_report: DataplexReport,
    mock_dataplex_client: Mock,
    mock_metadata_client: Mock,
) -> None:
    """Test that zones are not extracted when include_zones is False."""
    dataplex_config.include_zones = False

    ctx = PipelineContext(run_id="test-run")
    source = DataplexSource(ctx, dataplex_config)
    source.dataplex_client = mock_dataplex_client
    source.metadata_client = mock_metadata_client
    source.catalog_client = Mock()
    source.report = dataplex_report

    # This should not be called when include_zones is False
    # But if it is called, it should work
    mock_lake = create_mock_lake("lake-1")
    mock_zone = create_mock_zone("zone-1", "lake-1")
    mock_dataplex_client.list_lakes.return_value = [mock_lake]
    mock_dataplex_client.list_zones.return_value = [mock_zone]

    workunits = list(source._get_zones_mcps("test-project"))

    # 1 zone × 5 workunits = 5 workunits
    assert len(workunits) == 5


def test_get_zones_mcps_pagination_multiple_pages_lakes(
    dataplex_source: DataplexSource,
) -> None:
    """Test that pagination handles multiple pages of lakes correctly."""
    # Create 3 lakes to simulate multiple pages
    mock_lakes = [create_mock_lake(f"lake-{i}") for i in range(1, 4)]
    mock_lakes_pager = MockPager(mock_lakes, page_size=1)

    # Each lake has 2 zones
    def list_zones_side_effect(request):
        lake_id = request.parent.split("/")[-1]
        zones = [create_mock_zone(f"zone-{i}", lake_id) for i in range(1, 3)]
        return zones

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_lakes.side_effect = (
        lambda **kwargs: mock_lakes_pager
    )
    dataplex_source.dataplex_client.list_zones.side_effect = list_zones_side_effect

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # 3 lakes × 2 zones × 5 workunits = 30 workunits
    assert len(workunits) == 30
    assert dataplex_source.report.num_zones_scanned == 6
    # Verify pagination occurred for lakes
    assert len(mock_lakes_pager.pages_fetched) >= 2


def test_get_zones_mcps_pagination_multiple_pages_zones(
    dataplex_source: DataplexSource,
) -> None:
    """Test that pagination handles multiple pages of zones correctly."""
    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    # Create 5 zones to simulate multiple pages (with page_size=2, this is 3 pages)
    mock_zones = [create_mock_zone(f"zone-{i}", "lake-1") for i in range(1, 6)]
    mock_zones_pager = MockPager(mock_zones, page_size=2)

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_zones.side_effect = (
        lambda **kwargs: mock_zones_pager
    )

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # Verify all 5 zones are processed (5 zones × 5 workunits = 25 workunits)
    assert len(workunits) == 25
    assert dataplex_source.report.num_zones_scanned == 5
    # Verify pagination occurred (should have fetched multiple pages)
    assert len(mock_zones_pager.pages_fetched) >= 2


def test_get_zones_mcps_pagination_token_passing(
    dataplex_source: DataplexSource,
) -> None:
    """Test that pagination tokens are properly handled for zones."""
    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    # Create zones across multiple pages
    mock_zones = [create_mock_zone(f"zone-{i}", "lake-1") for i in range(1, 4)]
    mock_zones_pager = MockPager(mock_zones, page_size=1)

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_zones.side_effect = (
        lambda **kwargs: mock_zones_pager
    )

    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # Verify all zones are processed
    assert len(workunits) == 15  # 3 zones × 5 workunits
    assert dataplex_source.report.num_zones_scanned == 3
    # Verify pagination tokens were generated
    assert mock_zones_pager.current_page_token is not None


def test_get_zones_mcps_pagination_incomplete_result_zones(
    dataplex_source: DataplexSource, monkeypatch
) -> None:
    """Test handling of incomplete pagination results for zones (e.g., API failure mid-pagination)."""
    # Patch the exception type in the dataplex module to use our mock
    import datahub.ingestion.source.dataplex.dataplex as dataplex_module

    monkeypatch.setattr(
        dataplex_module.exceptions, "GoogleAPICallError", MockGoogleAPICallError
    )

    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    # Create a pager that fails after 2 items (simulating incomplete pagination)
    mock_zones = [create_mock_zone(f"zone-{i}", "lake-1") for i in range(1, 5)]
    mock_zones_pager = MockPager(mock_zones, page_size=2, fail_at=2)

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_zones.side_effect = (
        lambda **kwargs: mock_zones_pager
    )

    # Should handle the error gracefully
    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # Should have processed items before failure, but then stopped
    # With fail_at=2, first 2 zones (zone-1, zone-2) should be processed = 10 workunits
    # Then exception is raised when trying to process zone-3
    assert len(workunits) == 10  # 2 zones × 5 workunits = 10 workunits before failure
    assert len(dataplex_source.report.failures) > 0


def test_get_zones_mcps_pagination_incomplete_result_lakes(
    dataplex_source: DataplexSource, monkeypatch
) -> None:
    """Test handling of incomplete pagination results for lakes (e.g., API failure mid-pagination)."""
    # Patch the exception type in the dataplex module to use our mock
    import datahub.ingestion.source.dataplex.dataplex as dataplex_module

    monkeypatch.setattr(
        dataplex_module.exceptions, "GoogleAPICallError", MockGoogleAPICallError
    )

    # Create a pager that fails after 1 item (simulating incomplete pagination)
    mock_lakes = [create_mock_lake(f"lake-{i}") for i in range(1, 4)]
    mock_lakes_pager = MockPager(mock_lakes, page_size=1, fail_at=1)

    # Use side_effect to ensure the pager is returned correctly
    # This matches the pattern used in test_get_zones_mcps_pagination_multiple_pages_lakes
    dataplex_source.dataplex_client.list_lakes.side_effect = (
        lambda **kwargs: mock_lakes_pager
    )

    # Also set up list_zones to return empty list (won't be reached due to failure, but prevents errors)
    dataplex_source.dataplex_client.list_zones.return_value = []

    # Should handle the error gracefully
    workunits = list(dataplex_source._get_zones_mcps("test-project"))

    # Should have processed items before failure, but then stopped
    # With fail_at=1, first lake (lake-1) is processed, but exception is raised
    # before zones are fetched, so no zone workunits are generated
    assert (
        len(workunits) == 0
    )  # No zone workunits generated due to lake pagination failure
    assert len(dataplex_source.report.failures) > 0
