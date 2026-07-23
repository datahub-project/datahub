from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.aerospike import AerospikeSet
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def _all_sets(client: Any) -> List[AerospikeSet]:
    # Mirrors AerospikeSource.get_sets(), parsing the raw "sets" info-command
    # reply directly off the client rather than the Source (which also handles
    # ignore_empty_sets, not needed here — the probe surfaces everything).
    sets_info: str = client.info_random_node("sets")
    sets_info = (
        sets_info[len("sets\t") :] if sets_info.startswith("sets\t") else sets_info
    )
    sets_info = sets_info[: -len(";\n")] if sets_info.endswith(";\n") else sets_info
    return [
        AerospikeSet.from_info_string(item) for item in sets_info.split(";") if item
    ]


def _namespaces(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return sorted({aerospike_set.ns for aerospike_set in _all_sets(client)})


def _sets(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    namespace = parent_path[0]
    return [
        aerospike_set.set
        for aerospike_set in _all_sets(client)
        if aerospike_set.ns == namespace
    ]


# Aerospike is a 2-level catalog (namespace -> set), reached through the
# pyaerospike client the config now exposes via get_client() (hoisted out of
# Source.__init__ for reuse here). A "set" is the leaf dataset level; there is
# no dedicated Set subtype, so it surfaces under the generic Table dataset
# kind, filtered by set_pattern.
AEROSPIKE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(
            DatasetContainerSubTypes.NAMESPACE, "namespace_pattern", _namespaces
        ),
        ProbeLevel(DatasetSubTypes.TABLE, "set_pattern", _sets),
    ],
)

AEROSPIKE_PROBE_HIERARCHY: List[ProbeNodeKind] = AEROSPIKE_PROBE.hierarchy()


def list_aerospike_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return AEROSPIKE_PROBE.list_children(config, parent_path, limit)
