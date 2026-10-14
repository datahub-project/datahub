"""Unit tests for per-predicate structured property MCP builder."""

from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)
from datahub.ingestion.source.rdf.entities.rdf_structured_property.mcp_builder import (
    RdfStructuredPropertyMCPBuilder,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubStructuredPropertyAssignment,
    DataHubStructuredPropertyDefinition,
    MappingClass,
)
from datahub.ingestion.source.rdf.ontology.predicate_utils import (
    predicate_iri_to_qualified_name,
    predicate_iri_to_structured_property_urn,
)
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
)

DEPENDS_ON_IRI = "http://example.org/glossary/dependsOn"
RELATED_IRI = "http://example.org/glossary/related"
GLOSSARY_TERM_ENTITY_TYPE = "urn:li:entityType:datahub.glossaryTerm"


def test_definition_mcp_uses_lossless_predicate_qualified_name() -> None:
    qualified_name = predicate_iri_to_qualified_name(DEPENDS_ON_IRI)
    definition = DataHubStructuredPropertyDefinition(
        qualified_name=qualified_name,
        predicate_iri=DEPENDS_ON_IRI,
        display_name="dependsOn",
    )
    mcps = RdfStructuredPropertyMCPBuilder().build_all_mcps([definition])

    assert len(mcps) == 1
    assert mcps[0].entityUrn == predicate_iri_to_structured_property_urn(DEPENDS_ON_IRI)
    aspect = mcps[0].aspect
    assert isinstance(aspect, StructuredPropertyDefinitionClass)
    assert (
        aspect.qualifiedName
        == "io.datahub.rdf.predicate.example.org.glossary.dependsOn"
    )
    assert aspect.description == f"RDF predicate {DEPENDS_ON_IRI}"
    assert aspect.entityTypes == [GLOSSARY_TERM_ENTITY_TYPE]
    assert aspect.typeQualifier == {"allowedTypes": [GLOSSARY_TERM_ENTITY_TYPE]}


def test_per_predicate_assignments() -> None:
    generator = GlossaryTermUrnGenerator()
    depends_qn = predicate_iri_to_qualified_name(DEPENDS_ON_IRI)
    related_qn = predicate_iri_to_qualified_name(RELATED_IRI)

    graph = DataHubGraph()
    graph.structured_property_assignments = [
        DataHubStructuredPropertyAssignment(
            subject_urn="urn:li:glossaryTerm:example.org.account",
            object_urn=generator.generate_glossary_term_urn(
                "http://example.org/glossary/customer"
            ),
            predicate_iri=DEPENDS_ON_IRI,
            qualified_name=depends_qn,
            mapping_class=MappingClass.EXTENSION,
        ),
        DataHubStructuredPropertyAssignment(
            subject_urn="urn:li:glossaryTerm:example.org.account",
            object_urn=generator.generate_glossary_term_urn(
                "http://example.org/glossary/name"
            ),
            predicate_iri=RELATED_IRI,
            qualified_name=related_qn,
            mapping_class=MappingClass.EXTENSION,
        ),
    ]

    mcps = list(RdfStructuredPropertyMCPBuilder().build_post_processing_mcps(graph))
    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    assert len(aspect.properties) == 2

    property_urns = {prop.propertyUrn for prop in aspect.properties}
    assert property_urns == {
        predicate_iri_to_structured_property_urn(DEPENDS_ON_IRI),
        predicate_iri_to_structured_property_urn(RELATED_IRI),
    }


def test_collect_structured_property_definitions_from_assignments() -> None:
    qualified_name = predicate_iri_to_qualified_name(DEPENDS_ON_IRI)
    graph = DataHubGraph()
    graph.structured_property_assignments = [
        DataHubStructuredPropertyAssignment(
            subject_urn="urn:li:glossaryTerm:example.org.account",
            object_urn="urn:li:glossaryTerm:example.org.customer",
            predicate_iri=DEPENDS_ON_IRI,
            qualified_name=qualified_name,
            mapping_class=MappingClass.EXTENSION,
        )
    ]

    from datahub.ingestion.source.rdf.entities.rdf_structured_property.mcp_builder import (
        collect_structured_property_definitions,
    )

    definitions = collect_structured_property_definitions(graph)
    assert len(definitions) == 1
    assert definitions[0].qualified_name == qualified_name
    assert definitions[0].predicate_iri == DEPENDS_ON_IRI


def test_register_definitions_sync_emits_missing_properties() -> None:
    from unittest.mock import MagicMock

    from datahub.emitter.rest_emitter import EmitMode

    qualified_name = predicate_iri_to_qualified_name(DEPENDS_ON_IRI)
    definition = DataHubStructuredPropertyDefinition(
        qualified_name=qualified_name,
        predicate_iri=DEPENDS_ON_IRI,
        display_name="dependsOn",
    )
    graph = MagicMock()
    graph.get_aspect.return_value = None

    created = RdfStructuredPropertyMCPBuilder.register_definitions_sync(
        graph, [definition]
    )

    assert created == 1
    graph.emit_mcp.assert_called_once()
    _, kwargs = graph.emit_mcp.call_args
    assert kwargs["emit_mode"] == EmitMode.SYNC_PRIMARY
