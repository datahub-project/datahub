"""Unit tests for ontology-gated relationship routing."""

from rdflib import Graph

from datahub.ingestion.source.rdf.entities.relationship.ast import MappingClass
from datahub.ingestion.source.rdf.entities.relationship.router import RelationshipRouter
from datahub.ingestion.source.rdf.entities.relationship.triple_harvester import (
    TripleHarvester,
)
from datahub.ingestion.source.rdf.ontology.models import DataHubOntology
from datahub.ingestion.source.rdf.ontology.predicate_utils import (
    predicate_iri_to_qualified_name,
)
from datahub.ingestion.source.rdf.ontology.resolver import resolve_ontology


def _minimal_graph() -> Graph:
    graph = Graph()
    graph.parse(
        data="""
@prefix ex: <http://example.org/glossary/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

ex:CustomerData a skos:Concept ; skos:prefLabel "Customer Data" .
ex:AccountID a skos:Concept ; skos:prefLabel "Account Identifier" .
ex:CustomerName a skos:Concept ; skos:prefLabel "Customer Name" .

ex:AccountID skos:broader ex:CustomerData .
ex:AccountID skos:related ex:CustomerName .
ex:AccountID ex:dependsOn ex:CustomerName .
""",
        format="turtle",
    )
    return graph


def test_scenario_a_no_ontology_all_extensions() -> None:
    graph = _minimal_graph()
    statements = TripleHarvester().harvest(graph)
    native, assignments, definitions = RelationshipRouter(
        ontology=DataHubOntology()
    ).route(statements)

    assert len(native) == 0
    assert len(assignments) == 3
    assert len(definitions) == 3
    assert all(a.mapping_class == MappingClass.EXTENSION for a in assignments)
    assert {d.predicate_iri for d in definitions} == {
        "http://www.w3.org/2004/02/skos/core#broader",
        "http://www.w3.org/2004/02/skos/core#related",
        "http://example.org/glossary/dependsOn",
    }


def test_scenario_b_default_ontology_aligns_skos() -> None:
    graph = _minimal_graph()
    statements = TripleHarvester().harvest(graph)
    ontology = resolve_ontology("default")
    native, assignments, definitions = RelationshipRouter(ontology=ontology).route(
        statements
    )

    assert len(native) == 2
    assert len(assignments) == 1
    assert len(definitions) == 1

    fields = {rel.field for rel in native}
    assert fields == {"isRelatedTerms", "relatedTerms"}

    assert assignments[0].predicate_iri.endswith("dependsOn")
    assert assignments[0].mapping_class == MappingClass.EXTENSION
    assert assignments[0].qualified_name == predicate_iri_to_qualified_name(
        assignments[0].predicate_iri
    )


def test_subclass_of_harvested_and_aligned() -> None:
    graph = Graph()
    graph.parse(
        data="""
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Child rdfs:subClassOf ex:Parent .
ex:Child ex:partOf ex:Parent .
""",
        format="turtle",
    )
    statements = TripleHarvester().harvest(graph)
    predicate_iris = {s.predicate_iri for s in statements}
    assert "http://www.w3.org/2000/01/rdf-schema#subClassOf" in predicate_iris

    ontology = resolve_ontology("default")
    native, assignments, definitions = RelationshipRouter(ontology=ontology).route(
        statements
    )
    assert len(native) == 1
    assert native[0].field == "isRelatedTerms"
    assert len(assignments) == 1
    assert len(definitions) == 1
    assert assignments[0].predicate_iri == "http://example.org/partOf"
