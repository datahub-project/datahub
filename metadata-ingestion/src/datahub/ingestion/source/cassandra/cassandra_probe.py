from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def _build_client(config: Any) -> Any:
    client = config.get_client()
    client.authenticate()
    return client


def _keyspaces(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [keyspace.keyspace_name for keyspace in client.get_keyspaces()]


def _tables(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [table.table_name for table in client.get_tables(parent_path[0])]


# Cassandra is a 2-level catalog (keyspace -> table), reached through the client
# the config now exposes via get_client() (hoisted out of Source.__init__ for
# reuse here). The client isn't authenticated on construction, so the factory
# authenticates it before use, mirroring what Source.get_workunits_internal does.
CASSANDRA_PROBE = ClientProbe(
    client_factory=_build_client,
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(DatasetContainerSubTypes.KEYSPACE, list_names=_keyspaces),
        ProbeLevel(
            DatasetSubTypes.TABLE,
            list_names=_tables,
            parent=DatasetContainerSubTypes.KEYSPACE,
        ),
    ],
)

CASSANDRA_PROBE_HIERARCHY: List[ProbeNodeKind] = CASSANDRA_PROBE.hierarchy()


def list_cassandra_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return CASSANDRA_PROBE.list_children(config, parent_path, limit)
