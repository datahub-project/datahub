from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeLeafKind, ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _client(config: Any) -> Any:
    # Lazy import: the ThoughtSpot SDK client is only needed when a probe runs;
    # reusing it keeps auth/session behaviour identical to a real ingestion run.
    from datahub.ingestion.source.thoughtspot.client import ThoughtSpotClient

    return ThoughtSpotClient(config.connection)


def _worksheets(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [t.name for t in client.get_logical_tables() if t.name]


def _columns(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # get_logical_tables(include_details=True) already carries each table's columns.
    worksheet = parent_path[0]
    table = next((t for t in client.get_logical_tables() if t.name == worksheet), None)
    if table is None:
        return []
    return [c.name for c in (table.columns or []) if c.name]


# ThoughtSpot is not SQL, but opts into the same probe by reusing its REST client.
# This first cut probes the schema-bearing path (Worksheet -> Column); Liveboard
# and Answer are parallel content types whose subtypes sit outside ProbeNodeKind,
# a follow-up once that union is widened.
THOUGHTSPOT_PROBE = ClientProbe(
    client_factory=_client,
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(
            DatasetSubTypes.THOUGHTSPOT_WORKSHEET, "worksheet_pattern", _worksheets
        ),
        ProbeLevel(ProbeLeafKind.COLUMN, None, _columns),
    ],
)

THOUGHTSPOT_PROBE_HIERARCHY: List[ProbeNodeKind] = THOUGHTSPOT_PROBE.hierarchy()


def list_thoughtspot_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return THOUGHTSPOT_PROBE.list_children(config, parent_path, limit)
