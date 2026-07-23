from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _indices(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return sorted(client.indices.get_alias().keys())


# Elasticsearch is a flat index namespace filtered by index_pattern, reached
# through the client the config now exposes via get_client().
ELASTICSEARCH_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    levels=[ProbeLevel(DatasetSubTypes.ELASTIC_INDEX, "index_pattern", _indices)],
)

ELASTICSEARCH_PROBE_HIERARCHY: List[ProbeNodeKind] = ELASTICSEARCH_PROBE.hierarchy()


def list_elasticsearch_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return ELASTICSEARCH_PROBE.list_children(config, parent_path, limit)
