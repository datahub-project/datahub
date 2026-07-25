from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes


def _reports(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [r.name for r in client.get_all_reports() if r.name]


# PowerBI Report Server is a flat report namespace filtered by the connector's own
# report_pattern, reached through the connector's own NTLM-authenticated REST client
# (config.get_client()). Uses the native BIAssetSubTypes.REPORT subtype ("Report").
POWERBI_REPORT_SERVER_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    levels=[ProbeLevel(BIAssetSubTypes.REPORT, list_names=_reports)],
)

POWERBI_REPORT_SERVER_PROBE_HIERARCHY: List[ProbeNodeKind] = (
    POWERBI_REPORT_SERVER_PROBE.hierarchy()
)


def list_powerbi_report_server_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return POWERBI_REPORT_SERVER_PROBE.list_children(config, parent_path, limit)
