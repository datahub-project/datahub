"""Data models for the DataHub RDF ontology (TBox)."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


class AlignmentDirection(str, Enum):
    SUBJECT_TO_OBJECT = "subject_to_object"
    OBJECT_TO_SUBJECT = "object_to_subject"


@dataclass(frozen=True)
class NativeRelationship:
    """A first-class DataHub relationship from the ontology."""

    name: str
    aspect: str
    field: str
    entity_type: str = "glossaryTerm"


@dataclass(frozen=True)
class PredicateAlignment:
    """Maps an external predicate IRI to a native DataHub relationship."""

    predicate_iri: str
    native: NativeRelationship
    direction: AlignmentDirection = AlignmentDirection.SUBJECT_TO_OBJECT


@dataclass
class DataHubOntology:
    """Loaded TBox used by the ingestor to route relationship triples."""

    native_relationships: Dict[str, NativeRelationship] = field(default_factory=dict)
    alignments: Dict[str, PredicateAlignment] = field(default_factory=dict)

    def resolve_predicate(self, predicate_iri: str) -> Optional[PredicateAlignment]:
        return self.alignments.get(predicate_iri)

    @property
    def is_empty(self) -> bool:
        return not self.native_relationships and not self.alignments
