"""
Relationship Collector for Glossary Terms

Collects relationships from DataHubGlossaryTerm objects and creates
DataHubRelationship objects for the global relationships list.
"""

import logging
from typing import List

from datahub.ingestion.source.rdf.entities.glossary_term.ast import DataHubGlossaryTerm
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubRelationship,
    RelationshipType,
)

logger = logging.getLogger(__name__)


def collect_relationships_from_terms(
    terms: List[DataHubGlossaryTerm],
) -> List[DataHubRelationship]:
    """
    Collect all relationships from glossary terms as DataHubRelationship objects.

    This is used to populate the global relationships list in the DataHub AST.

    Args:
        terms: List of DataHubGlossaryTerm objects

    Returns:
        List of DataHubRelationship objects
    """
    all_relationships = []
    seen = set()

    for term in terms:
        # Process broader relationships
        for target_urn in term.relationships.get("broader", []):
            rel_key = (term.urn, target_urn, RelationshipType.BROADER)
            if rel_key not in seen:
                seen.add(rel_key)
                all_relationships.append(
                    DataHubRelationship(
                        source_urn=term.urn,
                        target_urn=target_urn,
                        relationship_type=RelationshipType.BROADER,
                        properties={},
                    )
                )

        # Process narrower relationships
        for target_urn in term.relationships.get("narrower", []):
            rel_key = (term.urn, target_urn, RelationshipType.NARROWER)
            if rel_key not in seen:
                seen.add(rel_key)
                all_relationships.append(
                    DataHubRelationship(
                        source_urn=term.urn,
                        target_urn=target_urn,
                        relationship_type=RelationshipType.NARROWER,
                        properties={},
                    )
                )

    if all_relationships:
        logger.info(
            f"Collected {len(all_relationships)} relationships from glossary terms"
        )

    return all_relationships
