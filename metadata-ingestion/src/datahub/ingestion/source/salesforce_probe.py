from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _objects(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [entity["QualifiedApiName"] for entity in client.list_objects()]


# Salesforce is a flat namespace of sObjects, reached through the connector's own
# API wrapper (config.get_client()). ClientProbe assigns one fixed kind per level,
# but the object list itself carries custom-vs-standard per name (the "__c" suffix,
# the same check get_subtypes_workunit uses) rather than as a separate level, so
# list_salesforce_children patches each node's kind after the fact below.
SALESFORCE_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    levels=[
        ProbeLevel(
            DatasetSubTypes.SALESFORCE_STANDARD_OBJECT, "object_pattern", _objects
        )
    ],
)

SALESFORCE_PROBE_HIERARCHY: List[ProbeNodeKind] = SALESFORCE_PROBE.hierarchy()


def list_salesforce_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    result = SALESFORCE_PROBE.list_children(config, parent_path, limit)
    for node in result.nodes:
        node.kind = (
            DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT
            if node.name.endswith("__c")
            else DatasetSubTypes.SALESFORCE_STANDARD_OBJECT
        )
    return result
