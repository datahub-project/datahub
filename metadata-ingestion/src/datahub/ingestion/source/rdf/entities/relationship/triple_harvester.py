"""Harvest object-property triples from an RDF graph."""

import logging
from typing import List, Set

from rdflib import Graph, URIRef
from rdflib.namespace import DCTERMS, OWL, RDF, RDFS, SKOS

from datahub.ingestion.source.rdf.entities.relationship.ast import RDFStatement

logger = logging.getLogger(__name__)

# Predicates that describe entity metadata, not inter-entity relationships.
METADATA_PREDICATES: Set[str] = {
    str(RDF.type),
    str(RDFS.label),
    str(RDFS.comment),
    str(RDFS.seeAlso),
    str(RDFS.isDefinedBy),
    str(RDFS.subPropertyOf),
    str(RDFS.domain),
    str(RDFS.range),
    str(SKOS.prefLabel),
    str(SKOS.altLabel),
    str(SKOS.hiddenLabel),
    str(SKOS.definition),
    str(SKOS.note),
    str(SKOS.scopeNote),
    str(SKOS.editorialNote),
    str(SKOS.changeNote),
    str(SKOS.historyNote),
    str(SKOS.example),
    str(SKOS.notation),
    str(SKOS.inScheme),
    str(SKOS.topConceptOf),
    "http://www.w3.org/2004/02/skos/core#broadTransitive",
    "http://www.w3.org/2004/02/skos/core#narrowTransitive",
    str(DCTERMS.title),
    str(DCTERMS.description),
    str(DCTERMS.source),
    str(DCTERMS.rights),
    str(DCTERMS.creator),
    str(DCTERMS.contributor),
    str(DCTERMS.created),
    str(DCTERMS.modified),
    str(OWL.versionInfo),
    str(OWL.imports),
    str(OWL.versionIRI),
    str(OWL.priorVersion),
    str(OWL.backwardCompatibleWith),
    str(OWL.incompatibleWith),
}


class TripleHarvester:
    """Extract URI-object property triples suitable for relationship routing."""

    def harvest(self, graph: Graph) -> List[RDFStatement]:
        statements: List[RDFStatement] = []
        seen: set[tuple[str, str, str]] = set()

        for subject, predicate, obj in graph.triples((None, None, None)):
            if not isinstance(subject, URIRef) or not isinstance(obj, URIRef):
                continue
            if not isinstance(predicate, URIRef):
                continue

            predicate_iri = str(predicate)
            if predicate_iri in METADATA_PREDICATES:
                continue

            key = (str(subject), predicate_iri, str(obj))
            if key in seen:
                continue
            seen.add(key)

            statements.append(
                RDFStatement(
                    subject_iri=str(subject),
                    predicate_iri=predicate_iri,
                    object_iri=str(obj),
                )
            )

        logger.debug("Harvested %d relationship triples", len(statements))
        return statements
