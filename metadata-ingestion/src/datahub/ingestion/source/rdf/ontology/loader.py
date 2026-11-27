"""Load the DataHub ontology TBox for relationship routing."""

import logging
from pathlib import Path
from typing import Optional

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF

from datahub.ingestion.source.rdf.ontology.models import (
    AlignmentDirection,
    DataHubOntology,
    NativeRelationship,
    PredicateAlignment,
)

logger = logging.getLogger(__name__)

DH = Namespace("https://datahubproject.io/ontology/")

DEFAULT_ONTOLOGY_RESOURCE = "datahub-glossary.ttl"


def default_ontology_path() -> Path:
    """Return the path to the bundled default glossary ontology TBox."""
    return Path(__file__).resolve().parent / DEFAULT_ONTOLOGY_RESOURCE


def load_datahub_ontology(path: Optional[str] = None) -> DataHubOntology:
    """
    Load a DataHub ontology TBox from Turtle.

    Args:
        path: Filesystem path to a .ttl file, or None for an empty ontology.

    Returns:
        Parsed DataHubOntology instance.
    """
    if path is None:
        return DataHubOntology()

    ontology_path = Path(path)
    if not ontology_path.is_file():
        raise FileNotFoundError(f"DataHub ontology file not found: {path}")

    graph = Graph()
    graph.parse(str(ontology_path), format="turtle")
    return _parse_ontology_graph(graph)


def _parse_ontology_graph(graph: Graph) -> DataHubOntology:
    native_by_uri: dict[str, NativeRelationship] = {}

    for subject in graph.subjects(RDF.type, DH.NativeRelationship):
        if not isinstance(subject, URIRef):
            continue
        name = _local_name(subject)
        aspect = _literal(graph, subject, DH.aspect)
        field = _literal(graph, subject, DH.field)
        entity_type = _literal(graph, subject, DH.entityType) or "glossaryTerm"
        if not aspect or not field:
            logger.warning("Skipping incomplete native relationship: %s", subject)
            continue
        native_by_uri[str(subject)] = NativeRelationship(
            name=name,
            aspect=aspect,
            field=field,
            entity_type=entity_type,
        )

    alignments: dict[str, PredicateAlignment] = {}
    for subject in graph.subjects(RDF.type, DH.PredicateAlignment):
        if not isinstance(subject, URIRef):
            continue
        predicate = graph.value(subject, DH.predicate)
        maps_to = graph.value(subject, DH.mapsTo)
        direction_literal = _literal(graph, subject, DH.direction)
        if not isinstance(predicate, URIRef) or not isinstance(maps_to, URIRef):
            logger.warning("Skipping incomplete predicate alignment: %s", subject)
            continue
        native = native_by_uri.get(str(maps_to))
        if native is None:
            logger.warning(
                "Alignment %s maps to unknown native relationship %s", subject, maps_to
            )
            continue
        direction = AlignmentDirection.SUBJECT_TO_OBJECT
        if direction_literal == AlignmentDirection.OBJECT_TO_SUBJECT.value:
            direction = AlignmentDirection.OBJECT_TO_SUBJECT
        alignments[str(predicate)] = PredicateAlignment(
            predicate_iri=str(predicate),
            native=native,
            direction=direction,
        )

    logger.info(
        "Loaded DataHub ontology: %d native relationships, %d alignments",
        len(native_by_uri),
        len(alignments),
    )
    return DataHubOntology(
        native_relationships={rel.name: rel for rel in native_by_uri.values()},
        alignments=alignments,
    )


def _literal(graph: Graph, subject: URIRef, predicate: URIRef) -> Optional[str]:
    value = graph.value(subject, predicate)
    if isinstance(value, Literal):
        return str(value)
    return None


def _local_name(uri: URIRef) -> str:
    uri_str = str(uri)
    if "#" in uri_str:
        return uri_str.rsplit("#", 1)[-1]
    return uri_str.rstrip("/").rsplit("/", 1)[-1]
