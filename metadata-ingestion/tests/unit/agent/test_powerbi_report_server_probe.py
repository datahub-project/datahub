from typing import Any, Callable

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.powerbi_report_server.powerbi_report_server_probe import (
    list_powerbi_report_server_children,
)


class _Report:
    def __init__(self, name: str):
        self.name = name


class _PowerBiReportServerClient:
    def __init__(self, reports):
        self._reports = reports

    def get_all_reports(self):
        return self._reports


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for report_pattern, which the probe now resolves by
# convention rather than declaring explicitly.
class _Config(ConfigModel):
    get_client: Callable[[], Any]
    report_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def _config():
    client = _PowerBiReportServerClient(
        [_Report("Sales"), _Report("LegacyLeads")],
    )
    return _Config(
        get_client=lambda: client,
        report_pattern=AllowDenyPattern(allow=[".*"], deny=["^Legacy.*"]),
    )


def test_powerbi_report_server_lists_reports_with_pattern_verdict():
    result = list_powerbi_report_server_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["Sales"].kind == BIAssetSubTypes.REPORT
    assert by_name["Sales"].pattern_field == "report_pattern"
    assert by_name["Sales"].included is True
    # Reuses the connector's own report_pattern deny for the verdict.
    assert by_name["LegacyLeads"].included is False
    assert by_name["LegacyLeads"].excluded_by == "report_pattern"


def test_powerbi_report_server_is_flat():
    result = list_powerbi_report_server_children(_config(), ["Sales"], 100)
    assert result.nodes == []
