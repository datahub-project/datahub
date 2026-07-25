from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def _databases(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return list(client.list_database_names())


def _collections(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return list(client[parent_path[0]].list_collection_names())


# MongoDB is database -> collection, reached through the client the config now
# exposes via get_mongo_client(). Collections have no dedicated subtype, so they
# surface under the generic Table dataset kind.
MONGODB_PROBE = ClientProbe(
    client_factory=lambda config: config.get_mongo_client(),
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(DatasetContainerSubTypes.DATABASE, list_names=_databases),
        # Mongo's own noun; the kind is DataHub's normalized Table subtype.
        # mongodb.py's get_workunits_internal matches collection_pattern against
        # "<database>.<collection>", not the bare collection name.
        ProbeLevel(
            DatasetSubTypes.TABLE,
            list_names=_collections,
            classify_on_fqn=True,
            parent=DatasetContainerSubTypes.DATABASE,
        ),
    ],
)

MONGODB_PROBE_HIERARCHY: List[ProbeNodeKind] = MONGODB_PROBE.hierarchy()


def list_mongodb_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return MONGODB_PROBE.list_children(config, parent_path, limit)
