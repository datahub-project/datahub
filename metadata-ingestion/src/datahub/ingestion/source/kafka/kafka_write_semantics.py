from typing import Dict, List, Optional, Tuple

from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
)


# Same merge rules as the dbt source's PATCH write semantics, so the two behave alike.
def merge_tags(
    graph: DataHubGraph, entity_urn: str, new_tag_urns: List[str], tag_prefix: str
) -> List[str]:
    """New tags plus the existing ones, minus existing tags carrying `tag_prefix` (ours)."""
    merged = dict.fromkeys(new_tag_urns)
    existing = graph.get_tags(entity_urn)
    if existing and existing.tags:
        prefix_urn = make_tag_urn(tag_prefix) if tag_prefix else None
        for association in existing.tags:
            if prefix_urn and association.tag.startswith(prefix_urn):
                continue
            merged.setdefault(association.tag)
    return list(merged)


def merge_owners(
    graph: DataHubGraph,
    entity_urn: str,
    new_owners: OwnershipClass,
    source_type: str,
) -> OwnershipClass:
    """New owners plus existing owners that this source (`source_type`) did not add."""
    owners: List[OwnerClass] = list(new_owners.owners)
    existing = graph.get_ownership(entity_urn)
    if existing and existing.owners:
        new_urns = {owner.owner for owner in new_owners.owners}
        for owner in existing.owners:
            if owner.owner in new_urns:
                continue
            if not owner.source or owner.source.type != source_type:
                owners.append(owner)
    deduped: Dict[Tuple[str, str, str], OwnerClass] = {}
    for owner in owners:
        deduped.setdefault((owner.owner, str(owner.type), str(owner.typeUrn)), owner)
    return OwnershipClass(
        owners=list(deduped.values()), lastModified=new_owners.lastModified
    )


def merge_terms(
    graph: DataHubGraph, entity_urn: str, new_terms: GlossaryTermsClass
) -> GlossaryTermsClass:
    existing: Optional[GlossaryTermsClass] = graph.get_glossary_terms(entity_urn)
    if existing and existing.terms:
        known = {term.urn for term in new_terms.terms}
        new_terms.terms = new_terms.terms + [
            term for term in existing.terms if term.urn not in known
        ]
    return new_terms
