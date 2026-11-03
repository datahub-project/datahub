"""Unit tests for reference-closure expansion after SPARQL filtering."""

from rdflib import OWL, RDF, RDFS, Graph, URIRef

from datahub.ingestion.source.rdf.ingestion.reference_closure import (
    expand_referenced_entities,
)


def test_expands_subclass_superclass_outside_filter_scope() -> None:
    ttl = """
@prefix sec: <https://spec.edmcouncil.org/fibo/ontology/SEC/> .
@prefix fbc: <https://spec.edmcouncil.org/fibo/ontology/FBC/> .
@prefix fnd: <https://spec.edmcouncil.org/fibo/ontology/FND/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

fnd:Entity a owl:Class ; rdfs:label "Entity" .
fbc:FinancialInstrument a owl:Class ; rdfs:label "Financial Instrument" ;
    rdfs:subClassOf fnd:Entity .
sec:Security a owl:Class ; rdfs:label "Security" ;
    rdfs:subClassOf fbc:FinancialInstrument .
"""
    full = Graph()
    full.parse(data=ttl, format="turtle")

    seed = Graph()
    for triple in full:
        if str(triple[0]).startswith("https://spec.edmcouncil.org/fibo/ontology/SEC/"):
            seed.add(triple)

    expanded = expand_referenced_entities(seed, full)
    subjects = {str(s) for s in expanded.subjects()}

    assert "https://spec.edmcouncil.org/fibo/ontology/SEC/Security" in subjects
    assert (
        "https://spec.edmcouncil.org/fibo/ontology/FBC/FinancialInstrument" in subjects
    )
    assert "https://spec.edmcouncil.org/fibo/ontology/FND/Entity" in subjects
    assert len(expanded) > len(seed)


def test_expands_skos_broader_parent() -> None:
    ttl = """
@prefix child: <http://example.org/child/> .
@prefix parent: <http://example.org/parent/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

child:Term a skos:Concept ; skos:prefLabel "Child Term" ; skos:broader parent:Term .
parent:Term a skos:Concept ; skos:prefLabel "Parent Term" .
"""
    full = Graph()
    full.parse(data=ttl, format="turtle")

    seed = Graph()
    for triple in full:
        if str(triple[0]).startswith("http://example.org/child/"):
            seed.add(triple)

    expanded = expand_referenced_entities(seed, full)
    subjects = {str(s) for s in expanded.subjects()}

    assert "http://example.org/child/Term" in subjects
    assert "http://example.org/parent/Term" in subjects


def test_empty_seed_returns_unchanged() -> None:
    full = Graph()
    full.parse(
        data="@prefix ex: <http://example.org/> . ex:a ex:p ex:b .",
        format="turtle",
    )
    seed = Graph()
    assert expand_referenced_entities(seed, full) is seed


def test_materializes_external_superclass_not_defined_in_source() -> None:
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
    explicit = URIRef("https://www.omg.org/spec/Commons/DatesAndTimes/ExplicitDate")

    assert (explicit, RDFS.label, None) in expanded
    labels = [str(label) for label in expanded.objects(explicit, RDFS.label)]
    assert labels == ["Explicit Date"]
    assert (explicit, RDF.type, OWL.Class) in expanded
