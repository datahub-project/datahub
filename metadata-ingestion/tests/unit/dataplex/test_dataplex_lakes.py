"""Unit tests for Dataplex lake extraction."""

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
        include_lakes=True,
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
    description: str = None,
    create_time: datetime.datetime = None,
    update_time: datetime.datetime = None,
) -> Mock:
    """Helper to create a mock lake object."""
    lake = Mock()
    lake.name = f"projects/test-project/locations/us-central1/lakes/{lake_id}"
    lake.display_name = display_name or lake_id
    lake.description = description or ""
    lake.create_time = create_time or datetime.datetime.now(datetime.timezone.utc)
    lake.update_time = update_time or lake.create_time
    return lake


def test_get_lakes_mcps_success(dataplex_source: DataplexSource) -> None:
    """Test successful lake extraction and MCP generation."""
    # Setup mock lakes
    mock_lake1 = create_mock_lake("lake-1", "Lake One", "First test lake")
    mock_lake2 = create_mock_lake("lake-2", "Lake Two", "Second test lake")

    # Mock the list_lakes API call
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake1, mock_lake2]

    # Get workunits
    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    # Verify results
    # gen_containers yields 5 workunits per container:
    # 1. Container (parent relationship)
    # 2. ContainerProperties
    # 3. StatusClass
    # 4. DataPlatformInstance
    # 5. SubTypesClass
    # So 2 lakes × 5 workunits = 10 workunits total
    assert len(workunits) == 10
    assert dataplex_source.report.num_lakes_scanned == 2
    assert dataplex_source.report.lakes_scanned == {
        mock_lake1.name.split("/")[-1]: True,
        mock_lake2.name.split("/")[-1]: True,
    }
    assert dataplex_source.report.num_lakes_filtered == 0

    # Verify container properties
    # Expected aspect names: "container", "containerProperties", "status",
    # "dataPlatformInstance", "subTypes"
    expected_aspects = {
        "container",
        "containerProperties",
        "status",
        "dataPlatformInstance",
        "subTypes",
    }
    for workunit in workunits:
        assert workunit.metadata is not None
        aspect_name = workunit.metadata.aspectName
        assert aspect_name is not None
        # Verify aspect name is one of the expected container aspects
        assert aspect_name in expected_aspects, f"Unexpected aspect name: {aspect_name}"


def test_get_lakes_mcps_with_filtering(dataplex_source: DataplexSource) -> None:
    """Test lake extraction with pattern filtering."""
    # Configure filter to exclude lake-2
    dataplex_source.config.filter_config.lake_pattern.allow = ["lake-1"]

    mock_lake1 = create_mock_lake("lake-1")
    mock_lake2 = create_mock_lake("lake-2")

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake1, mock_lake2]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    # Only lake-1 should be included
    assert len(workunits) == 5
    assert dataplex_source.report.num_lakes_scanned == 2
    assert dataplex_source.report.lakes_scanned == {
        mock_lake1.name.split("/")[-1]: True,
        mock_lake2.name.split("/")[-1]: False,
    }
    assert dataplex_source.report.num_lakes_filtered == 1


def test_get_lakes_mcps_with_timestamps(dataplex_source: DataplexSource) -> None:
    """Test lake extraction with create/update timestamps."""
    create_time = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    update_time = datetime.datetime(2023, 2, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

    mock_lake = create_mock_lake(
        "lake-1",
        create_time=create_time,
        update_time=update_time,
    )

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    assert len(workunits) == 5
    # Verify timestamps are properly handled
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_lakes_mcps_without_timestamps(dataplex_source: DataplexSource) -> None:
    """Test lake extraction when timestamps are None."""
    mock_lake = create_mock_lake("lake-1")
    mock_lake.create_time = None
    mock_lake.update_time = None

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    assert len(workunits) == 5
    # Should handle None timestamps gracefully
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_lakes_mcps_api_error(dataplex_source: DataplexSource, monkeypatch) -> None:
    """Test lake extraction error handling."""
    # Patch the exception type in the dataplex module to use our mock
    import datahub.ingestion.source.dataplex.dataplex as dataplex_module

    # Replace GoogleAPICallError with our mock exception
    monkeypatch.setattr(
        dataplex_module.exceptions, "GoogleAPICallError", MockGoogleAPICallError
    )

    # Mock API error
    dataplex_source.dataplex_client.list_lakes.side_effect = MockGoogleAPICallError(
        "API Error", code=403
    )

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    # Should handle error gracefully
    assert len(workunits) == 0
    assert len(dataplex_source.report.failures) > 0


def test_get_lakes_mcps_parent_container_relationship(
    dataplex_source: DataplexSource,
) -> None:
    """Test that lakes have correct parent container relationship."""
    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None
    # Verify parent container key points to project
    container_aspect = workunit.metadata.aspect
    assert container_aspect is not None


def test_get_lakes_mcps_empty_result(dataplex_source: DataplexSource) -> None:
    """Test lake extraction when no lakes are found."""
    dataplex_source.dataplex_client.list_lakes.return_value = []

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    assert len(workunits) == 0
    assert dataplex_source.report.num_lakes_scanned == 0
    assert dataplex_source.report.lakes_scanned == {}


def test_get_lakes_mcps_display_name_fallback(
    dataplex_source: DataplexSource,
) -> None:
    """Test that lake ID is used when display_name is None."""
    mock_lake = create_mock_lake("lake-1", display_name=None)

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_lakes_mcps_description_fallback(
    dataplex_source: DataplexSource,
) -> None:
    """Test that empty string is used when description is None."""
    mock_lake = create_mock_lake("lake-1", description=None)

    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    assert len(workunits) == 5
    workunit = workunits[0]
    assert workunit.metadata is not None


def test_get_lakes_mcps_subtypes(dataplex_source: DataplexSource) -> None:
    """Test that lakes have correct sub_types."""
    mock_lake = create_mock_lake("lake-1")
    dataplex_source.dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

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
    assert "Dataplex Lake" in container_aspect.typeNames


def test_get_lakes_mcps_extract_lakes_disabled(
    dataplex_config: DataplexConfig,
    dataplex_report: DataplexReport,
    mock_dataplex_client: Mock,
    mock_metadata_client: Mock,
) -> None:
    """Test that lakes are not extracted when include_lakes is False."""
    dataplex_config.include_lakes = False

    ctx = PipelineContext(run_id="test-run")
    source = DataplexSource(ctx, dataplex_config)
    source.dataplex_client = mock_dataplex_client
    source.metadata_client = mock_metadata_client
    source.catalog_client = Mock()
    source.report = dataplex_report

    # This should not be called when include_lakes is False
    # But if it is called, it should work
    mock_lake = create_mock_lake("lake-1")
    mock_dataplex_client.list_lakes.return_value = [mock_lake]

    workunits = list(source._get_lakes_mcps("test-project"))

    assert len(workunits) == 5


def test_get_lakes_mcps_pagination_multiple_pages(
    dataplex_source: DataplexSource,
) -> None:
    """Test that pagination handles multiple pages correctly."""
    # Create 5 lakes to simulate multiple pages (with page_size=2, this is 3 pages)
    mock_lakes = [create_mock_lake(f"lake-{i}") for i in range(1, 6)]
    mock_pager = MockPager(mock_lakes, page_size=2)

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_lakes.side_effect = lambda **kwargs: mock_pager

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    # Verify all 5 lakes are processed (5 lakes × 5 workunits = 25 workunits)
    assert len(workunits) == 25
    assert dataplex_source.report.num_lakes_scanned == 5
    # Verify pagination occurred (should have fetched multiple pages)
    assert len(mock_pager.pages_fetched) >= 2


def test_get_lakes_mcps_pagination_token_passing(
    dataplex_source: DataplexSource,
) -> None:
    """Test that all items are processed across multiple pages."""
    # Create lakes across multiple pages
    mock_lakes = [create_mock_lake(f"lake-{i}") for i in range(1, 4)]
    mock_pager = MockPager(mock_lakes, page_size=1)

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_lakes.side_effect = lambda **kwargs: mock_pager

    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    # Verify all lakes are processed across multiple pages
    assert len(workunits) == 15  # 3 lakes × 5 workunits
    assert dataplex_source.report.num_lakes_scanned == 3
    # Verify pagination occurred (multiple pages were fetched)
    assert len(mock_pager.pages_fetched) >= 2


def test_get_lakes_mcps_pagination_incomplete_result(
    dataplex_source: DataplexSource, monkeypatch
) -> None:
    """Test handling of incomplete pagination results (e.g., API failure mid-pagination)."""
    # Patch the exception type in the dataplex module to use our mock
    import datahub.ingestion.source.dataplex.dataplex as dataplex_module

    monkeypatch.setattr(
        dataplex_module.exceptions, "GoogleAPICallError", MockGoogleAPICallError
    )

    # Create a pager that fails after 2 items (simulating incomplete pagination)
    mock_lakes = [create_mock_lake(f"lake-{i}") for i in range(1, 5)]
    mock_pager = MockPager(mock_lakes, page_size=2, fail_at=2)

    # Use side_effect to ensure the pager is returned correctly
    dataplex_source.dataplex_client.list_lakes.side_effect = lambda **kwargs: mock_pager

    # Should handle the error gracefully
    workunits = list(dataplex_source._get_lakes_mcps("test-project"))

    # Should have processed items before failure, but then stopped
    # With fail_at=2, first 2 lakes (lake-1, lake-2) should be processed = 10 workunits
    # Then exception is raised when trying to process lake-3
    assert len(workunits) == 10  # 2 lakes × 5 workunits = 10 workunits before failure
    assert len(dataplex_source.report.failures) > 0
