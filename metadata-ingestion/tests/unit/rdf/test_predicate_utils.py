"""Unit tests for lossless predicate IRI to structured property mapping."""

from datahub.ingestion.source.rdf.ontology.predicate_utils import (
    predicate_display_name,
    predicate_iri_to_qualified_name,
    predicate_iri_to_structured_property_urn,
)


def test_predicate_iri_to_qualified_name_uses_full_path() -> None:
    iri = "http://example.org/glossary/dependsOn"
    assert (
        predicate_iri_to_qualified_name(iri)
        == "io.datahub.rdf.predicate.example.org.glossary.dependsOn"
    )
    assert (
        predicate_iri_to_structured_property_urn(iri)
        == "urn:li:structuredProperty:io.datahub.rdf.predicate.example.org.glossary.dependsOn"
    )


def test_predicate_iri_to_qualified_name_includes_hash_fragment() -> None:
    broader = "http://www.w3.org/2004/02/skos/core#broader"
    related = "http://www.w3.org/2004/02/skos/core#related"
    assert (
        predicate_iri_to_qualified_name(broader)
        == "io.datahub.rdf.predicate.w3.org.2004.02.skos.core.broader"
    )
    assert (
        predicate_iri_to_qualified_name(related)
        == "io.datahub.rdf.predicate.w3.org.2004.02.skos.core.related"
    )
    assert predicate_iri_to_qualified_name(broader) != predicate_iri_to_qualified_name(
        related
    )


def test_predicate_display_name_uses_local_name() -> None:
    assert (
        predicate_display_name("http://example.org/glossary/dependsOn") == "dependsOn"
    )
    assert (
        predicate_display_name("http://www.w3.org/2004/02/skos/core#broader")
        == "broader"
    )
