"""Lossless mapping between RDF predicate IRIs and structured property qualified names."""

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)
from datahub.utilities.urn_encoder import UrnEncoder

PREDICATE_SP_PREFIX = "io.datahub.rdf.predicate"


def _encode_path_id(iri: str, path_segments: list[str]) -> str:
    term_id = ".".join(path_segments)
    if any(ord(c) > 127 for c in term_id):
        return datahub_guid({"path": term_id, "iri": iri})
    encoded = UrnEncoder.encode_string(term_id)
    if UrnEncoder.contains_extended_reserved_char(encoded):
        return datahub_guid({"path": term_id, "iri": iri})
    return encoded


def predicate_iri_to_qualified_name(predicate_iri: str) -> str:
    """
    Derive a stable structured property qualifiedName from a predicate IRI.

    Uses the same IRI path encoding as glossary term URNs so export can recover
    the original predicate IRI from the structured property URN. Hash fragments
    are appended as a final path segment because urlparse drops them from path.
    """
    generator = GlossaryTermUrnGenerator()
    path_segments = list(
        generator.derive_path_from_iri(predicate_iri, include_last=True)
    )
    if "#" in predicate_iri:
        fragment = predicate_iri.rsplit("#", 1)[-1]
        if fragment and (not path_segments or path_segments[-1] != fragment):
            path_segments.append(fragment)
    encoded = _encode_path_id(predicate_iri, path_segments)
    return f"{PREDICATE_SP_PREFIX}.{encoded}"


def predicate_iri_to_structured_property_urn(predicate_iri: str) -> str:
    return f"urn:li:structuredProperty:{predicate_iri_to_qualified_name(predicate_iri)}"


def predicate_display_name(predicate_iri: str) -> str:
    if "#" in predicate_iri:
        return predicate_iri.rsplit("#", 1)[-1]
    return predicate_iri.rstrip("/").rsplit("/", 1)[-1]
