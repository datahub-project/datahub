from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def _databases(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    paginator = client.get_paginator("get_databases")
    return [
        db["Name"]
        for page in paginator.paginate()
        for db in page.get("DatabaseList", [])
    ]


def _tables(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    paginator = client.get_paginator("get_tables")
    return [
        t["Name"]
        for page in paginator.paginate(DatabaseName=parent_path[0])
        for t in page.get("TableList", [])
    ]


# Glue is a 2-level catalog (database -> table) reached through the boto3 client
# the config already exposes (config.glue_client) — no connector refactor needed.
GLUE_PROBE = ClientProbe(
    client_factory=lambda config: config.glue_client,
    levels=[
        ProbeLevel(DatasetContainerSubTypes.DATABASE, "database_pattern", _databases),
        ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _tables),
    ],
)

GLUE_PROBE_HIERARCHY: List[ProbeNodeKind] = GLUE_PROBE.hierarchy()


def list_glue_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return GLUE_PROBE.list_children(config, parent_path, limit)
