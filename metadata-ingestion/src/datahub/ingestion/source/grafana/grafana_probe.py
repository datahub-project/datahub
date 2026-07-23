from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes


def _folders(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [folder.title for folder in client.get_folders()]


def _dashboards(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # get_dashboards() fetches each result individually (get_dashboard(uid)), which
    # is the only call that populates folder_id from the dashboard's meta — so the
    # full list must be fetched and filtered by the parent folder's id here, rather
    # than asking the API for one folder's dashboards directly.
    folder_ids_by_title = {folder.title: folder.id for folder in client.get_folders()}
    folder_id = folder_ids_by_title.get(parent_path[0])
    return [
        dashboard.title
        for dashboard in client.get_dashboards()
        if dashboard.folder_id == folder_id
    ]


# Grafana is folder -> dashboard, reached through the connector's own REST client
# (config.get_client()). Dashboards with no folder are not reachable through this
# hierarchy, matching ingestion's own behavior of leaving them out of any folder
# container.
GRAFANA_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    levels=[
        ProbeLevel(BIContainerSubTypes.GRAFANA_FOLDER, "folder_pattern", _folders),
        ProbeLevel(
            BIContainerSubTypes.GRAFANA_DASHBOARD, "dashboard_pattern", _dashboards
        ),
    ],
)

GRAFANA_PROBE_HIERARCHY: List[ProbeNodeKind] = GRAFANA_PROBE.hierarchy()


def list_grafana_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return GRAFANA_PROBE.list_children(config, parent_path, limit)
