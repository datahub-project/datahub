from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import UNFILTERED, ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes

# Mode's own client is (session, workspace_uri), built by ModeConfig.get_mode_session()
# so the probe and ModeSource share one construction path.
ModeClient = Tuple[requests.Session, str]

# Mode's API and its own config field (space_pattern) call this level "space", but
# the connector's emitted container subtype is BIContainerSubTypes.MODE_COLLECTION
# ("Collection") — Mode renamed Spaces to Collections in its product UI. There is no
# shared subtype whose value is "Space", so this level uses the API's own term
# rather than a mismatched container subtype.
MODE_SPACE: ProbeNodeKind = "Space"


def _get_embedded(
    session: requests.Session, url: str, key: str
) -> List[Dict[str, Any]]:
    response = session.get(url)
    if not response.ok:
        return []
    return list(response.json().get("_embedded", {}).get(key, []))


def _display_name(item: Dict[str, Any]) -> str:
    # Live Mode workspaces can hold reports with a null "name" (seen against a
    # real workspace); fall back to the token, then "unknown" — mirroring
    # mode.py's own name-or-token-or-"unknown" convention (see
    # construct_query_or_dataset's report_name resolution) rather than letting
    # AllowDenyPattern.allowed() blow up on a non-string name.
    return str(item.get("name") or item.get("token") or "unknown")


def _space_token(
    session: requests.Session, workspace_uri: str, space_name: str
) -> Optional[str]:
    for space in _get_embedded(session, f"{workspace_uri}/spaces", "spaces"):
        if _display_name(space) == space_name:
            return space.get("token")
    return None


def _report_token(
    session: requests.Session, workspace_uri: str, space_token: str, report_name: str
) -> Optional[str]:
    url = f"{workspace_uri}/spaces/{space_token}/reports"
    for report in _get_embedded(session, url, "reports"):
        if _display_name(report) == report_name:
            return report.get("token")
    return None


def _spaces(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    url = f"{workspace_uri}/spaces"
    return [_display_name(space) for space in _get_embedded(session, url, "spaces")]


def _reports(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    space_token = _space_token(session, workspace_uri, parent_path[0])
    if space_token is None:
        return []
    url = f"{workspace_uri}/spaces/{space_token}/reports"
    return [_display_name(report) for report in _get_embedded(session, url, "reports")]


def _datasets(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    space_token = _space_token(session, workspace_uri, parent_path[0])
    if space_token is None:
        return []
    url = f"{workspace_uri}/spaces/{space_token}/datasets"
    return [
        _display_name(dataset) for dataset in _get_embedded(session, url, "datasets")
    ]


def _queries(client: ModeClient, config: Any, parent_path: List[str]) -> Sequence[str]:
    session, workspace_uri = client
    space_name, report_name = parent_path[0], parent_path[1]
    space_token = _space_token(session, workspace_uri, space_name)
    if space_token is None:
        return []
    report_token = _report_token(session, workspace_uri, space_token, report_name)
    if report_token is None:
        return []
    url = f"{workspace_uri}/reports/{report_token}/queries"
    return [_display_name(query) for query in _get_embedded(session, url, "queries")]


# Mode is a Space holding BOTH Reports and Datasets — the first branching probe,
# reached through the connector's own session (config.get_mode_session()). Mode's
# API is token-addressed while parent_path carries names, so each lister below
# resolves the parent name to its token by re-fetching the parent listing (mirroring
# how grafana_probe resolves a folder title to an id). Dataset and Query take
# UNFILTERED: Mode declares no dataset_pattern/query_pattern to filter them.
MODE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_mode_session(),
    levels=[
        ProbeLevel(MODE_SPACE, "space_pattern", _spaces),
        ProbeLevel(
            BIAssetSubTypes.MODE_REPORT, "report_pattern", _reports, parent=MODE_SPACE
        ),
        ProbeLevel(
            BIAssetSubTypes.MODE_DATASET, UNFILTERED, _datasets, parent=MODE_SPACE
        ),
        ProbeLevel(
            BIAssetSubTypes.MODE_QUERY,
            UNFILTERED,
            _queries,
            parent=BIAssetSubTypes.MODE_REPORT,
        ),
    ],
)


def list_mode_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return MODE_PROBE.list_children(config, parent_path, limit)
