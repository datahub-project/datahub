"""Unit tests for glossary term kind classification."""

from rdflib import Graph, URIRef

from datahub.ingestion.source.rdf.dialects import DefaultDialect
from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect
from datahub.ingestion.source.rdf.entities.glossary_term.extractor import (
    GlossaryTermExtractor,
)
from datahub.ingestion.source.rdf.entities.glossary_term.kinds import (
    CLASS,
    CONCEPT,
    ENTITY_KIND_PROPERTY,
    MATERIALIZED,
    NAMED_INDIVIDUAL,
    resolve_entity_kind,
)
from datahub.ingestion.source.rdf.ingestion.reference_closure import (
    expand_referenced_entities,
)


def test_resolve_entity_kind_from_rdf_types() -> None:
    graph = Graph()
    graph.parse(
        data="""
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/> .

ex:Concept a skos:Concept .
ex:Class a owl:Class .
ex:Individual a owl:NamedIndividual .
""",
        format="turtle",
    )

    assert resolve_entity_kind(graph, URIRef("http://example.org/Concept")) == CONCEPT
    assert resolve_entity_kind(graph, URIRef("http://example.org/Class")) == CLASS
    assert (
        resolve_entity_kind(graph, URIRef("http://example.org/Individual"))
        == NAMED_INDIVIDUAL
    )


def test_extractor_sets_class_kind_for_fibo_term() -> None:
    graph = Graph()
    graph.parse(
        data="""
@prefix ex: <https://spec.edmcouncil.org/fibo/ontology/SEC/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:Security a owl:Class ; rdfs:label "Security" .
""",
        format="turtle",
    )
    terms = GlossaryTermExtractor().extract_all(graph, {"dialect": FIBODialect()})
    assert len(terms) == 1
    assert terms[0].custom_properties[ENTITY_KIND_PROPERTY] == CLASS


def test_extractor_sets_concept_kind_for_skos_term() -> None:
    graph = Graph()
    graph.parse(
        data="""
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/glossary/> .

ex:CustomerData a skos:Concept ; skos:prefLabel "Customer Data" .
""",
        format="turtle",
    )
    terms = GlossaryTermExtractor().extract_all(graph, {"dialect": DefaultDialect()})
    assert len(terms) == 1
    assert terms[0].custom_properties[ENTITY_KIND_PROPERTY] == CONCEPT


def test_materialized_external_term_is_tagged() -> None:
    ttl = """
@prefix sec: <https://spec.edmcouncil.org/fibo/ontology/SEC/Debt/Bonds/> .
@prefix cmns: <https://www.omg.org/spec/Commons/DatesAndTimes/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

sec:FirstRegularCouponDate a owl:Class ;
    rdfs:label "first regular coupon date" ;
    rdfs:subClassOf cmns:ExplicitDate .
"""
    full = Graph()
    full.parse(data=ttl, format="turtle")
    seed = Graph()
    for triple in full:
        if str(triple[0]).startswith("https://spec.edmcouncil.org/fibo/ontology/SEC/"):
            seed.add(triple)

    expanded = expand_referenced_entities(seed, full)
    terms = GlossaryTermExtractor().extract_all(expanded, {"dialect": FIBODialect()})
    kinds = {t.source: t.custom_properties[ENTITY_KIND_PROPERTY] for t in terms}

    assert (
        kinds[
            "https://spec.edmcouncil.org/fibo/ontology/SEC/Debt/Bonds/FirstRegularCouponDate"
        ]
        == CLASS
    )
    assert (
        kinds["https://www.omg.org/spec/Commons/DatesAndTimes/ExplicitDate"]
        == MATERIALIZED
    )
