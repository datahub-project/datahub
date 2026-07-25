from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes


def _indexes(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [index.name for index in client.list_indexes()]


def _namespaces(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [namespace.name for namespace in client.list_namespaces(parent_path[0])]


# Pinecone is index -> namespace, reached through the client the config now
# exposes via get_client(). A namespace is Pinecone's leaf level (not a
# dataset), so it is modeled as a container using the closest existing
# subtype, PINECONE_NAMESPACE.
PINECONE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    levels=[
        ProbeLevel(DatasetContainerSubTypes.PINECONE_INDEX, list_names=_indexes),
        ProbeLevel(
            DatasetContainerSubTypes.PINECONE_NAMESPACE,
            list_names=_namespaces,
            parent=DatasetContainerSubTypes.PINECONE_INDEX,
        ),
    ],
)

PINECONE_PROBE_HIERARCHY: List[ProbeNodeKind] = PINECONE_PROBE.hierarchy()


def list_pinecone_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return PINECONE_PROBE.list_children(config, parent_path, limit)
