from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _objects(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [entity["QualifiedApiName"] for entity in client.list_objects()]


def _object_kind(name: str) -> ProbeNodeKind:
    # Custom vs standard is per-object (the "__c" suffix, same check as
    # get_subtypes_workunit), so the kind is derived per node within the one level.
    return (
        DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT
        if name.endswith("__c")
        else DatasetSubTypes.SALESFORCE_STANDARD_OBJECT
    )


# Salesforce is a flat namespace of sObjects, reached through the connector's own
# API wrapper (config.get_client()); each object's kind is custom/standard per name.
SALESFORCE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    levels=[
        ProbeLevel(
            DatasetSubTypes.SALESFORCE_STANDARD_OBJECT,
            "object_pattern",
            _objects,
            kind_for=_object_kind,
        )
    ],
)

SALESFORCE_PROBE_HIERARCHY: List[ProbeNodeKind] = SALESFORCE_PROBE.hierarchy()


def list_salesforce_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return SALESFORCE_PROBE.list_children(config, parent_path, limit)
