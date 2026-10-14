"""Glossary term classification for RDF ingestion custom properties."""

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, SKOS

ENTITY_KIND_PROPERTY = "rdf:entityKind"

# Values stored on glossary terms under ENTITY_KIND_PROPERTY.
CONCEPT = "concept"
CLASS = "class"
NAMED_INDIVIDUAL = "namedIndividual"
PREDICATE = "predicate"
MATERIALIZED = "materialized"

# Internal RDF predicate used to mark materialized entities in the expanded graph.
MATERIALIZED_ENTITY_KIND_PREDICATE = "https://datahubproject.io/rdf/entityKind"


def resolve_entity_kind(graph: Graph, uri: URIRef) -> str:
    """Infer glossary term kind from graph typing and ingestion markers."""
    kind_predicate = URIRef(MATERIALIZED_ENTITY_KIND_PREDICATE)
    for obj in graph.objects(uri, kind_predicate):
        if obj is not None:
            return str(obj)

    if (uri, RDF.type, OWL.NamedIndividual) in graph:
        return NAMED_INDIVIDUAL
    if (uri, RDF.type, OWL.Class) in graph:
        return CLASS
    if (uri, RDF.type, SKOS.Concept) in graph:
        return CONCEPT
    return CONCEPT
