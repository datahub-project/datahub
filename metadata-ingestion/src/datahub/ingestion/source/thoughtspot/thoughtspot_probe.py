from typing import Any, Callable, Dict, List

from datahub.ingestion.agent.models import (
    ProbeLeafKind,
    ProbeNodeKind,
    ProbeResult,
)
from datahub.ingestion.agent.probe import Verdict, column_nodes, container_nodes
from datahub.ingestion.source.common.subtypes import DatasetSubTypes

# ThoughtSpot is not SQL, but it still opts into the same probe interface by
# reusing its REST client. Its content types (Worksheet, Liveboard, Answer) are
# parallel rather than a container tree; this first cut probes the schema-bearing
# path — Worksheet -> Column — which is what recipe filtering and lineage care
# about. Liveboard/Answer (dashboards/charts) are a follow-up: their subtypes sit
# outside the current ProbeNodeKind union, so surfacing them cleanly needs that
# union widened first.
THOUGHTSPOT_PROBE_HIERARCHY: List[ProbeNodeKind] = [
    DatasetSubTypes.THOUGHTSPOT_WORKSHEET,
    ProbeLeafKind.COLUMN,
]


def _worksheet_classifier(config: Any) -> Callable[[str, str], Verdict]:
    # Reuse the connector's own worksheet_pattern so the verdict matches ingestion.
    worksheet_pattern = config.worksheet_pattern

    def classify(name: str, node_fqn: str) -> Verdict:
        if not worksheet_pattern.allowed(name):
            return (False, "worksheet_pattern")
        return (True, None)

    return classify


def list_thoughtspot_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    # Lazy import: the ThoughtSpot SDK client is only needed when a probe runs,
    # and reusing it keeps auth/session behaviour identical to a real run.
    from datahub.ingestion.source.thoughtspot.client import ThoughtSpotClient

    client = ThoughtSpotClient(config.connection)
    try:
        # get_logical_tables(include_details=True) already carries each table's
        # columns, so both levels come from one client round-trip.
        tables = list(client.get_logical_tables())
        if not parent_path:
            names = [t.name for t in tables if t.name]
            nodes, truncated = container_nodes(
                names,
                limit,
                DatasetSubTypes.THOUGHTSPOT_WORKSHEET,
                "worksheet_pattern",
                classify=_worksheet_classifier(config),
            )
            return ProbeResult(
                source_type="",
                supported=True,
                parent_path=parent_path,
                nodes=nodes,
                truncated=truncated,
            )
        worksheet = parent_path[0]
        table = next((t for t in tables if t.name == worksheet), None)
        cols: List[Dict[str, object]] = (
            [{"name": c.name} for c in (table.columns or []) if c.name]
            if table is not None
            else []
        )
        nodes, truncated = column_nodes(cols, limit, fqn_prefix=worksheet)
        return ProbeResult(
            source_type="",
            supported=True,
            parent_path=parent_path,
            nodes=nodes,
            truncated=truncated,
        )
    finally:
        client.close()
