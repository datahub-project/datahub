from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _tables(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # DynamoDB filters on the region-qualified name (region.table), so probe
    # under the same name the connector's table_pattern is matched against.
    region = client.meta.region_name
    names: List[str] = []
    for page in client.get_paginator("list_tables").paginate():
        names.extend(f"{region}.{t}" for t in page.get("TableNames", []))
    return names


# DynamoDB is a flat table namespace reached through the boto3 client the config
# already exposes (config.dynamodb_client) — no connector refactor.
DYNAMODB_PROBE = ClientProbe(
    client_factory=lambda config: config.dynamodb_client,
    levels=[ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _tables)],
)

DYNAMODB_PROBE_HIERARCHY: List[ProbeNodeKind] = DYNAMODB_PROBE.hierarchy()


def list_dynamodb_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return DYNAMODB_PROBE.list_children(config, parent_path, limit)
