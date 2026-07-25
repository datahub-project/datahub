from types import SimpleNamespace
from typing import List

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.grafana_probe import list_grafana_children
from datahub.ingestion.source.grafana.models import Dashboard, Folder


class _FakeGrafanaClient:
    def __init__(self, folders: List[Folder], dashboards: List[Dashboard]) -> None:
        self._folders = folders
        self._dashboards = dashboards

    def get_folders(self) -> List[Folder]:
        return self._folders

    def get_dashboards(self) -> List[Dashboard]:
        return self._dashboards


def _config():
    folders = [Folder(id="1", title="analytics"), Folder(id="2", title="scratch")]
    dashboards = [
        Dashboard(uid="d1", title="revenue", panels=[], folder_id="1"),
        Dashboard(uid="d2", title="tmp_debug", panels=[], folder_id="1"),
        Dashboard(uid="d3", title="unfiled", panels=[]),
    ]
    return SimpleNamespace(
        get_client=lambda: _FakeGrafanaClient(folders, dashboards),
        folder_pattern=AllowDenyPattern(allow=[".*"]),
        dashboard_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_grafana_lists_folders_with_pattern_verdict():
    result = list_grafana_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["analytics"].kind == BIContainerSubTypes.GRAFANA_FOLDER
    assert by_name["analytics"].pattern_field == "folder_pattern"
    assert by_name["analytics"].included is True


def test_grafana_lists_dashboards_reusing_dashboard_pattern():
    result = list_grafana_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["revenue"].kind == BIContainerSubTypes.GRAFANA_DASHBOARD
    assert by_name["revenue"].included is True
    # The connector's own dashboard_pattern deny (^tmp_) is reused for the verdict.
    assert by_name["tmp_debug"].included is False
    assert by_name["tmp_debug"].excluded_by == "dashboard_pattern"
    # Dashboards outside the folder are not listed as its children.
    assert "unfiled" not in by_name
