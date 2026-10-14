"""Expand a filtered RDF graph to include referenced URI entities from the full graph."""

from __future__ import annotations

import logging
import re
from typing import Iterable, Set

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from datahub.ingestion.source.rdf.entities.glossary_term.kinds import (
    MATERIALIZED,
    MATERIALIZED_ENTITY_KIND_PREDICATE,
)

logger = logging.getLogger(__name__)

# Predicates whose object URIs should be pulled in when referenced from the seed graph.
_HIERARCHY_UP = frozenset({RDFS.subClassOf, SKOS.broader})
_HIERARCHY_DOWN = frozenset({SKOS.narrower})
_EQUIVALENCE = frozenset(
    {OWL.equivalentClass, OWL.sameAs, SKOS.exactMatch, SKOS.closeMatch}
)
_HIERARCHY_REFERENCE = _HIERARCHY_UP | _HIERARCHY_DOWN | _EQUIVALENCE


def _uri_subjects(graph: Graph) -> Set[str]:
    return {str(subject) for subject in graph.subjects() if isinstance(subject, URIRef)}


def _uri_objects(graph: Graph) -> Set[str]:
    return {str(obj) for obj in graph.objects() if isinstance(obj, URIRef)}


def _follow_objects(full: Graph, uri: URIRef, predicates: Iterable[URIRef]) -> Set[str]:
    found: Set[str] = set()
    for predicate in predicates:
        for obj in full.objects(uri, predicate):
            if isinstance(obj, URIRef):
                found.add(str(obj))
    return found


def _follow_subjects(
    full: Graph, uri: URIRef, predicates: Iterable[URIRef]
) -> Set[str]:
    found: Set[str] = set()
    for predicate in predicates:
        for subject in full.subjects(predicate, uri):
            if isinstance(subject, URIRef):
                found.add(str(subject))
    return found


def _transitive_closure(
    full: Graph, start_uris: Set[str], predicates: frozenset[URIRef]
) -> Set[str]:
    closure: Set[str] = set()
    frontier = set(start_uris)
    while frontier:
        current = frontier.pop()
        for found in _follow_objects(full, URIRef(current), predicates):
            if found not in closure:
                closure.add(found)
                frontier.add(found)
    return closure


def _collect_referenced_entity_iris(seed: Graph, full: Graph) -> Set[str]:
    included = _uri_subjects(seed) | _uri_objects(seed)

    changed = True
    while changed:
        changed = False
        expanded: Set[str] = set()

        expanded |= _transitive_closure(full, included, _HIERARCHY_UP)
        expanded |= _transitive_closure(full, included, _HIERARCHY_DOWN)

        for uri in included:
            uri_ref = URIRef(uri)
            for predicate in _EQUIVALENCE:
                expanded |= _follow_objects(full, uri_ref, [predicate])
                expanded |= _follow_subjects(full, uri_ref, [predicate])

        new_iris = expanded - included
        if new_iris:
            included |= new_iris
            changed = True

    # Relationship targets and other URI objects on included entities.
    for subject, _, obj in full:
        if (
            isinstance(subject, URIRef)
            and str(subject) in included
            and isinstance(obj, URIRef)
        ):
            included.add(str(obj))

    return included


def _local_name(uri: URIRef) -> str:
    uri_str = str(uri)
    if "#" in uri_str:
        return uri_str.rsplit("#", 1)[-1]
    return uri_str.rstrip("/").rsplit("/", 1)[-1]


def _humanize_label(local_name: str) -> str:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", local_name)
    spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", spaced)
    return spaced.replace("_", " ").strip() or local_name


def _has_subject_triples(graph: Graph, uri: URIRef) -> bool:
    return any(graph.triples((uri, None, None)))


def _is_hierarchy_reference(full: Graph, iri: str) -> bool:
    uri = URIRef(iri)
    return any(any(full.subjects(predicate, uri)) for predicate in _HIERARCHY_REFERENCE)


def _materialize_external_hierarchy_entity(
    uri: URIRef,
) -> list[tuple[URIRef, URIRef, object]]:
    label = _humanize_label(_local_name(uri))
    return [
        (uri, RDF.type, OWL.Class),
        (uri, RDFS.label, Literal(label)),
        (uri, URIRef(MATERIALIZED_ENTITY_KIND_PREDICATE), Literal(MATERIALIZED)),
    ]


def _materialize_undefined_hierarchy_entities(
    expanded: Graph, full: Graph, included_iris: Set[str]
) -> int:
    """
    Synthesize minimal owl:Class + rdfs:label triples for hierarchy targets that
    are referenced in the source but not defined in it (e.g. OMG Commons classes
    referenced by FIBO rdfs:subClassOf axioms).
    """
    materialized = 0
    for iri in included_iris:
        uri = URIRef(iri)
        if _has_subject_triples(full, uri):
            continue
        if not _is_hierarchy_reference(full, iri):
            continue
        for triple in _materialize_external_hierarchy_entity(uri):
            if triple not in expanded:
                expanded.add(triple)
                materialized += 1
    if materialized:
        logger.info(
            "Materialized %d triples for undefined external hierarchy entities",
            materialized,
        )
    return materialized


def expand_referenced_entities(seed: Graph, full: Graph) -> Graph:
    """
    Include triples from ``full`` for URI entities referenced by ``seed``.

    Used after SPARQL filtering so module-scoped slices still import superclass
    classes, SKOS broader/narrower parents/children, equivalent terms, and other
    URI objects needed for complete glossary and relationship ingestion.
    """
    if len(seed) == 0 or len(full) == 0:
        return seed

    included_iris = _collect_referenced_entity_iris(seed, full)
    if not included_iris:
        return seed

    expanded = Graph()
    for triple in seed:
        expanded.add(triple)

    added = 0
    for subject, predicate, obj in full:
        if isinstance(subject, URIRef) and str(subject) in included_iris:
            if (subject, predicate, obj) not in expanded:
                expanded.add((subject, predicate, obj))
                added += 1

    materialized = _materialize_undefined_hierarchy_entities(
        expanded, full, included_iris
    )

    logger.info(
        "Reference closure added %d triples and materialized %d for %d referenced "
        "entities (%d → %d triples)",
        added,
        materialized,
        len(included_iris) - len(_uri_subjects(seed)),
        len(seed),
        len(expanded),
    )
    return expanded
