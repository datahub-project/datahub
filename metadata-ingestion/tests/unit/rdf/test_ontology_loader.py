"""Unit tests for DataHub ontology TBox loading."""

import pytest

from datahub.ingestion.source.rdf.ontology.loader import (
    default_ontology_path,
    load_datahub_ontology,
)
from datahub.ingestion.source.rdf.ontology.models import AlignmentDirection
from datahub.ingestion.source.rdf.ontology.resolver import resolve_ontology

SKOS_BROADER = "http://www.w3.org/2004/02/skos/core#broader"
SKOS_RELATED = "http://www.w3.org/2004/02/skos/core#related"


def test_default_ontology_path_exists() -> None:
    path = default_ontology_path()
    assert path.is_file()


def test_load_default_glossary_ontology() -> None:
    ontology = load_datahub_ontology(str(default_ontology_path()))
    assert "IsA" in ontology.native_relationships
    assert "IsRelatedTo" in ontology.native_relationships

    broader = ontology.resolve_predicate(SKOS_BROADER)
    assert broader is not None
    assert broader.native.name == "IsA"
    assert broader.direction == AlignmentDirection.SUBJECT_TO_OBJECT

    related = ontology.resolve_predicate(SKOS_RELATED)
    assert related is not None
    assert related.native.name == "IsRelatedTo"


def test_resolve_ontology_default_and_empty() -> None:
    default = resolve_ontology("default")
    assert default.resolve_predicate(SKOS_BROADER) is not None

    empty = resolve_ontology(None)
    assert empty.resolve_predicate(SKOS_BROADER) is None

    empty2 = resolve_ontology("")
    assert empty2.is_empty


def test_resolve_ontology_missing_file() -> None:
    with pytest.raises(FileNotFoundError):
        load_datahub_ontology("/tmp/nonexistent-datahub-ontology.ttl")
