"""
Tests for stateful and incremental features added to the Dremio connector:

- auto_incremental_properties: DatasetProperties emitted as PATCH operations
- Time-window advancement: extract_all_queries / DremioCatalog.get_queries respect
  caller-supplied start_time / end_time overrides so the stateful handler can
  advance the window to only process new job history
"""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_properties_helper import (
    auto_incremental_properties,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_entities import DremioCatalog
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    MetadataChangeProposalClass,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_props_workunit(
    urn: str = "urn:li:dataset:(urn:li:dataPlatform:dremio,x,PROD)",
) -> MetadataWorkUnit:
    props = DatasetPropertiesClass(name="test", description="Test dataset")
    mcp = MetadataChangeProposalWrapper(entityUrn=urn, aspect=props)
    return MetadataWorkUnit(id="test-wu", mcp=mcp)


# ---------------------------------------------------------------------------
# auto_incremental_properties
# ---------------------------------------------------------------------------


class TestAutoIncrementalProperties:
    def test_converts_dataset_properties_to_patch(self):
        """When incremental=True, DatasetProperties MCPs are converted to PATCH operations."""
        wu = _make_props_workunit()
        output = list(auto_incremental_properties(True, [wu]))

        assert len(output) == 1
        output_mcp = output[0].metadata
        # convert_dataset_properties_to_patch uses mcp_raw, so metadata is MetadataChangeProposalClass
        assert isinstance(output_mcp, MetadataChangeProposalClass)
        assert output_mcp.changeType == ChangeTypeClass.PATCH


# ---------------------------------------------------------------------------
# Time-window override in extract_all_queries / get_queries
# ---------------------------------------------------------------------------


class TestTimeWindowOverride:
    """Verify that passed-in start/end times override the instance-level window."""

    def test_extract_all_queries_uses_override_start_time(self):
        """extract_all_queries respects a caller-supplied start_time."""

        # Re-create a real instance to call the real method
        with patch.object(
            DremioAPIOperations,
            "_get_queries_chunked",
            return_value=iter([]),
        ) as mock_chunked:
            real_api = DremioAPIOperations.__new__(DremioAPIOperations)
            real_api.start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            real_api.end_time = datetime(2024, 1, 7, tzinfo=timezone.utc)
            real_api.edition = DremioEdition.COMMUNITY

            override_start = datetime(2024, 1, 5, tzinfo=timezone.utc)
            list(real_api.extract_all_queries(start_time=override_start))

            # The SQL passed to _get_queries_chunked should contain the override start
            assert mock_chunked.called
            sql_arg = mock_chunked.call_args[0][0]
            assert "2024-01-05" in sql_arg

    def test_extract_all_queries_falls_back_to_instance_times(self):
        """extract_all_queries falls back to self.start_time/end_time when no override."""
        with patch.object(
            DremioAPIOperations,
            "_get_queries_chunked",
            return_value=iter([]),
        ) as mock_chunked:
            real_api = DremioAPIOperations.__new__(DremioAPIOperations)
            real_api.start_time = datetime(2024, 3, 1, tzinfo=timezone.utc)
            real_api.end_time = datetime(2024, 3, 7, tzinfo=timezone.utc)
            real_api.edition = DremioEdition.COMMUNITY

            list(real_api.extract_all_queries())  # no override

            assert mock_chunked.called
            sql_arg = mock_chunked.call_args[0][0]
            assert "2024-03-01" in sql_arg

    def test_catalog_get_queries_forwards_override_times(self):
        """DremioCatalog.get_queries passes override times through to extract_all_queries."""
        mock_api = Mock()
        mock_api.extract_all_queries.return_value = iter([])

        catalog = DremioCatalog.__new__(DremioCatalog)
        catalog.api = mock_api

        override_start = datetime(2024, 2, 10, tzinfo=timezone.utc)
        override_end = datetime(2024, 2, 15, tzinfo=timezone.utc)

        list(catalog.get_queries(start_time=override_start, end_time=override_end))

        mock_api.extract_all_queries.assert_called_once_with(
            start_time=override_start, end_time=override_end
        )
