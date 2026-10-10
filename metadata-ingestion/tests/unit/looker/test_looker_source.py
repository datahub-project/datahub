from unittest import mock

from looker_sdk.error import SDKError

from datahub.ingestion.source.looker.looker_common import LookerDashboardSourceReport
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource


def test_fetch_dashboard_from_api_reports_failure_not_warning():
    """A fetch error must be a report.failure(), not a warning().

    StaleEntityRemovalHandler.gen_removed_entity_workunits only skips
    soft-deletion when report.failures is non-empty: a dashboard that fails
    to fetch was never "seen" this run, and if that's reported as a mere
    warning, stale-entity removal still soft-deletes it even though it
    still exists in Looker.
    """
    source = object.__new__(LookerDashboardSource)
    source.looker_api = mock.MagicMock()
    source.looker_api.dashboard.side_effect = SDKError("boom")
    source.reporter = LookerDashboardSourceReport()

    result = source._fetch_dashboard_from_api(dashboard_id="1", fields=["id"])

    assert result is None
    assert len(source.reporter.failures) == 1
    assert len(source.reporter.warnings) == 0
