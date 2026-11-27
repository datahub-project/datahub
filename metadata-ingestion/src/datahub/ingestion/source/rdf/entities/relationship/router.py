"""Route harvested RDF triples to native or extension DataHub representations."""

import logging
from typing import Dict, List, Set, Tuple

from rdflib.namespace import OWL, RDFS

from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)
from datahub.ingestion.source.rdf.entities.relationship.ast import (
    DataHubNativeRelationship,
    DataHubStructuredPropertyAssignment,
    DataHubStructuredPropertyDefinition,
    MappingClass,
    RDFStatement,
)
from datahub.ingestion.source.rdf.ontology.models import (
    AlignmentDirection,
    DataHubOntology,
)
from datahub.ingestion.source.rdf.ontology.predicate_utils import (
    predicate_display_name,
    predicate_iri_to_qualified_name,
)

logger = logging.getLogger(__name__)

OWL_AXIOM_PREDICATES: Set[str] = {
    str(RDFS.subPropertyOf),
    str(OWL.disjointWith),
    str(OWL.equivalentClass),
    str(OWL.equivalentProperty),
    str(OWL.inverseOf),
    str(OWL.sameAs),
    str(OWL.differentFrom),
    str(OWL.allValuesFrom),
    str(OWL.someValuesFrom),
    str(OWL.hasValue),
    str(OWL.cardinality),
    str(OWL.minCardinality),
    str(OWL.maxCardinality),
    str(OWL.unionOf),
    str(OWL.intersectionOf),
    str(OWL.complementOf),
    str(OWL.oneOf),
    str(OWL.onProperty),
    str(OWL.onClass),
}


class RelationshipRouter:
    """Route RDF statements using the DataHub ontology TBox."""

    def __init__(
        self,
        ontology: DataHubOntology,
        urn_generator: GlossaryTermUrnGenerator | None = None,
        skip_owl_axioms: bool = True,
    ) -> None:
        self.ontology = ontology
        self.urn_generator = urn_generator or GlossaryTermUrnGenerator()
        self.skip_owl_axioms = skip_owl_axioms

    def route(
        self, statements: List[RDFStatement]
    ) -> Tuple[
        List[DataHubNativeRelationship],
        List[DataHubStructuredPropertyAssignment],
        List[DataHubStructuredPropertyDefinition],
    ]:
        native: List[DataHubNativeRelationship] = []
        assignments: List[DataHubStructuredPropertyAssignment] = []
        definitions: Dict[str, DataHubStructuredPropertyDefinition] = {}

        for stmt in statements:
            if self.skip_owl_axioms and stmt.predicate_iri in OWL_AXIOM_PREDICATES:
                continue
            try:
                alignment = self.ontology.resolve_predicate(stmt.predicate_iri)
                if alignment is not None:
                    source_iri, target_iri = self._resolve_direction(
                        stmt.subject_iri,
                        stmt.object_iri,
                        alignment.direction,
                    )
                    native.append(
                        DataHubNativeRelationship(
                            source_urn=self.urn_generator.generate_glossary_term_urn(
                                source_iri
                            ),
                            target_urn=self.urn_generator.generate_glossary_term_urn(
                                target_iri
                            ),
                            field=alignment.native.field,
                            aspect=alignment.native.aspect,
                            original_predicate_iri=stmt.predicate_iri,
                            mapping_class=MappingClass.ALIGNED,
                            maps_to=alignment.native.name,
                        )
                    )
                else:
                    qualified_name = predicate_iri_to_qualified_name(stmt.predicate_iri)
                    if qualified_name not in definitions:
                        definitions[qualified_name] = (
                            DataHubStructuredPropertyDefinition(
                                qualified_name=qualified_name,
                                predicate_iri=stmt.predicate_iri,
                                display_name=predicate_display_name(stmt.predicate_iri),
                            )
                        )
                    assignments.append(
                        DataHubStructuredPropertyAssignment(
                            subject_urn=self.urn_generator.generate_glossary_term_urn(
                                stmt.subject_iri
                            ),
                            object_urn=self.urn_generator.generate_glossary_term_urn(
                                stmt.object_iri
                            ),
                            predicate_iri=stmt.predicate_iri,
                            qualified_name=qualified_name,
                            mapping_class=MappingClass.EXTENSION,
                        )
                    )
            except (ValueError, RuntimeError) as exc:
                logger.warning(
                    "Skipping relationship triple (%s, %s, %s): %s",
                    stmt.subject_iri,
                    stmt.predicate_iri,
                    stmt.object_iri,
                    exc,
                )

        logger.info(
            "Routed %d native relationships, %d extension assignments, %d SP definitions",
            len(native),
            len(assignments),
            len(definitions),
        )
        return native, assignments, list(definitions.values())

    @staticmethod
    def _resolve_direction(
        subject_iri: str,
        object_iri: str,
        direction: AlignmentDirection,
    ) -> tuple[str, str]:
        if direction == AlignmentDirection.OBJECT_TO_SUBJECT:
            return object_iri, subject_iri
        return subject_iri, object_iri
