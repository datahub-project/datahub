"""
Relationship Extractor

Harvests and routes RDF relationship triples using the DataHub ontology TBox.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import Graph

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.relationship.ast import RDFStatement
from datahub.ingestion.source.rdf.entities.relationship.router import RelationshipRouter
from datahub.ingestion.source.rdf.entities.relationship.triple_harvester import (
    TripleHarvester,
)
from datahub.ingestion.source.rdf.ontology.models import DataHubOntology

logger = logging.getLogger(__name__)


class RelationshipExtractor(EntityExtractor[RDFStatement]):
    """Harvests object-property triples from RDF graphs."""

    def __init__(self) -> None:
        self._harvester = TripleHarvester()

    @property
    def entity_type(self) -> str:
        return "relationship"

    def can_extract(self, graph: Graph, uri: Any) -> bool:
        return False

    def extract(
        self, graph: Graph, uri: Any, context: Optional[Dict[str, Any]] = None
    ) -> Optional[RDFStatement]:
        return None

    def extract_all(
        self, graph: Graph, context: Optional[Dict[str, Any]] = None
    ) -> List[RDFStatement]:
        return self._harvester.harvest(graph)

    def extract_and_route(
        self,
        graph: Graph,
        ontology: DataHubOntology,
        skip_owl_axioms: bool = True,
        context: Optional[Dict[str, Any]] = None,
    ) -> tuple[list, list, list]:
        """Harvest triples and route them via the ontology."""
        statements = self._harvester.harvest(graph)
        router = RelationshipRouter(
            ontology=ontology,
            skip_owl_axioms=skip_owl_axioms,
        )
        return router.route(statements)
