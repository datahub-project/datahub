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
        # Stays explicit: kind_for reclassifies some items to
        # SALESFORCE_CUSTOM_OBJECT ("Custom Object"), a kind with no
        # custom_object_pattern field of its own — Salesforce reuses object_pattern
        # for both custom and standard objects, so the per-item kind can't be
        # resolved by convention for every item at this level. Deleting this
        # (as Task 2's brief instructs) breaks resolution for Custom-Object-kind
        # items: with pattern_field=None, _resolved looks up a field per the
        # ITEM'S kind, not the level's kind, and "Custom Object" has none by
        # convention — see task-2-report.md for the empirical failure.
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
