"""
Glossary Term Extractor

Extracts glossary terms from RDF graphs and creates DataHub AST objects directly.
Supports SKOS Concepts, OWL Classes, and other glossary-like entities.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import RDF, RDFS, Graph, Literal, URIRef
from rdflib.namespace import DC, DCTERMS, OWL, SKOS

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.glossary_term.ast import DataHubGlossaryTerm
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)

logger = logging.getLogger(__name__)


class GlossaryTermExtractor(EntityExtractor[DataHubGlossaryTerm]):
    """
    Extracts glossary terms from RDF graphs.

    Identifies entities as glossary terms if they:
    - Have type skos:Concept, owl:Class, or owl:NamedIndividual
    - Have a label (rdfs:label or skos:prefLabel) of at least 3 characters

    Extracts:
    - Basic properties (name, definition, source)
    - Relationships (skos:broader, skos:narrower only)
    - Custom properties (including FIBO-specific if applicable)
    - SKOS metadata (notation, scopeNote, altLabel, hiddenLabel)
    """

    def __init__(
        self, dialect=None, urn_generator: GlossaryTermUrnGenerator | None = None
    ):
        """
        Initialize the extractor.

        Args:
            dialect: Optional dialect for dialect-specific extraction
            urn_generator: URN generator for creating DataHub URNs
        """
        self.dialect = dialect
        self._detected_dialect = None
        self.urn_generator = urn_generator or GlossaryTermUrnGenerator()

    @property
    def entity_type(self) -> str:
        return "glossary_term"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI represents a glossary term."""
        # Excluded types (per old implementation) - ontology constructs are not terms
        excluded_types = {
            OWL.Ontology,
            RDF.Property,
            OWL.ObjectProperty,
            OWL.DatatypeProperty,
            OWL.FunctionalProperty,
            RDFS.Class,
        }

        # Check for excluded types first
        for rdf_type in graph.objects(uri, RDF.type):
            if rdf_type in excluded_types:
                return False

        # Check for glossary term types
        term_types = {SKOS.Concept, OWL.Class, OWL.NamedIndividual}

        for rdf_type in graph.objects(uri, RDF.type):
            if rdf_type in term_types:
                # Also check for valid label
                name = self._extract_name(graph, uri)
                return name is not None and len(name) >= 3

        return False

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] | None = None
    ) -> Optional[DataHubGlossaryTerm]:
        """
        Extract a single glossary term from the RDF graph and return DataHub AST directly.

        Args:
            graph: The RDF graph
            uri: The URI of the term to extract
            context: Optional context with 'dialect' for dialect-specific extraction
        """
        try:
            # Extract basic properties
            name = self._extract_name(graph, uri)
            if not name or len(name) < 3:
                return None

            definition = self._extract_definition(graph, uri)
            source_uri = str(uri)

            # Extract relationships (only broader/narrower supported) - keep as URIs for now
            relationship_uris = self._extract_relationship_uris(graph, uri)

            # Extract custom properties
            custom_properties = self._extract_custom_properties(graph, uri, context)
            custom_properties["rdf:originalIRI"] = source_uri

            # Extract SHACL constraints and add as custom property if term is also a PropertyShape
            shacl_constraints = self._extract_shacl_constraints_description(graph, uri)
            if shacl_constraints:
                custom_properties["shacl:dataConstraints"] = shacl_constraints

            # Extract SKOS-specific properties and add to custom_properties
            alternative_labels = self._extract_alternative_labels(graph, uri)
            hidden_labels = self._extract_hidden_labels(graph, uri)
            notation = self._extract_notation(graph, uri)
            scope_note = self._extract_scope_note(graph, uri)

            if alternative_labels:
                custom_properties["skos:altLabel"] = ",".join(alternative_labels)
            if hidden_labels:
                custom_properties["skos:hiddenLabel"] = ",".join(hidden_labels)
            if notation:
                custom_properties["skos:notation"] = notation
            if scope_note:
                custom_properties["skos:scopeNote"] = scope_note

            # Generate DataHub URN
            term_urn = self.urn_generator.generate_glossary_term_urn(source_uri)

            # Extract path segments for domain hierarchy
            path_segments = list(
                self.urn_generator.derive_path_from_iri(source_uri, include_last=True)
            )

            # Convert relationship URIs to URNs and create dict format
            relationships = self._convert_relationship_uris_to_urns(relationship_uris)

            return DataHubGlossaryTerm(
                urn=term_urn,
                name=name,
                definition=definition,
                source=source_uri,
                relationships=relationships,
                custom_properties=custom_properties,
                path_segments=path_segments,
            )

        except Exception as e:
            logger.warning(f"Error extracting glossary term from {uri}: {e}")
            return None

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] | None = None
    ) -> List[DataHubGlossaryTerm]:
        """Extract all glossary terms from the RDF graph."""
        terms = []
        seen_uris = set()

        # Excluded types (per old implementation) - ontology constructs are not terms
        excluded_types = {
            OWL.Ontology,
            RDF.Property,
            OWL.ObjectProperty,
            OWL.DatatypeProperty,
            OWL.FunctionalProperty,
            RDFS.Class,
        }

        # Find all potential glossary term types
        term_type_predicates = [SKOS.Concept, OWL.Class, OWL.NamedIndividual]

        for term_type in term_type_predicates:
            for subject in graph.subjects(RDF.type, term_type):
                if isinstance(subject, URIRef) and str(subject) not in seen_uris:
                    # Check for excluded types
                    is_excluded = False
                    for rdf_type in graph.objects(subject, RDF.type):
                        if rdf_type in excluded_types:
                            is_excluded = True
                            break

                    if not is_excluded:
                        term = self.extract(graph, subject, context)
                        if term:
                            terms.append(term)
                            seen_uris.add(str(subject))

        logger.info(f"Extracted {len(terms)} glossary terms")
        return terms

    # --- Private extraction methods ---

    def _extract_name(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """
        Extract name from label properties.

        Per specification: skos:prefLabel → rdfs:label
        """
        # Priority order per specification: skos:prefLabel first, then rdfs:label
        label_properties = [SKOS.prefLabel, RDFS.label]

        for prop in label_properties:
            for obj in graph.objects(uri, prop):
                if isinstance(obj, Literal):
                    name = str(obj).strip()
                    if name:
                        return name

        return None

    def _extract_definition(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """
        Extract definition from SKOS or RDFS properties.

        Per specification: skos:definition → rdfs:comment
        """
        # Priority order per specification: skos:definition first, then rdfs:comment
        definition_properties = [SKOS.definition, RDFS.comment]

        for prop in definition_properties:
            for obj in graph.objects(uri, prop):
                if isinstance(obj, Literal):
                    definition = str(obj).strip()
                    if definition:
                        return definition

        return None

    def _extract_source(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract source reference."""
        source_properties = [DCTERMS.source, DC.source, DCTERMS.creator]

        for prop in source_properties:
            for obj in graph.objects(uri, prop):
                if obj:
                    return str(obj)

        return None

    def _extract_relationship_uris(
        self, graph: Graph, uri: URIRef
    ) -> Dict[str, List[str]]:
        """
        Extract relationship URIs for a glossary term.

        Only extracts skos:broader and skos:narrower.
        Returns dict with 'broader' and 'narrower' keys containing lists of target URIs.
        """
        relationships = {"broader": [], "narrower": []}

        # Extract broader relationships
        for obj in graph.objects(uri, SKOS.broader):
            if isinstance(obj, URIRef):
                relationships["broader"].append(str(obj))

        # Extract narrower relationships
        for obj in graph.objects(uri, SKOS.narrower):
            if isinstance(obj, URIRef):
                relationships["narrower"].append(str(obj))

        return relationships

    def _convert_relationship_uris_to_urns(
        self, relationship_uris: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        """
        Convert relationship URIs to URNs.

        Args:
            relationship_uris: Dict with 'broader' and 'narrower' keys containing lists of URIs

        Returns:
            Dict with 'broader' and 'narrower' keys containing lists of URNs
        """
        relationships = {"broader": [], "narrower": []}

        for rel_type, uris in relationship_uris.items():
            if rel_type in relationships:
                for uri in uris:
                    try:
                        target_urn = self.urn_generator.generate_glossary_term_urn(uri)
                        relationships[rel_type].append(target_urn)
                    except Exception as e:
                        logger.warning(
                            f"Failed to convert relationship URI {uri} to URN: {e}"
                        )

        return relationships

    def _extract_custom_properties(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """Extract custom properties, including dialect-specific ones."""
        properties = {}

        # Check for FIBO dialect
        dialect = context.get("dialect") if context else self.dialect
        is_fibo = (
            dialect
            and hasattr(dialect, "dialect_type")
            and str(dialect.dialect_type) == "RDFDialect.FIBO"
        )

        if is_fibo:
            properties.update(self._extract_fibo_properties(graph, uri))

        return properties

    def _extract_fibo_properties(self, graph: Graph, uri: URIRef) -> Dict[str, Any]:
        """Extract FIBO-specific properties."""
        properties = {}

        # FIBO namespaces
        CMNS_AV = "https://www.omg.org/spec/Commons/AnnotationVocabulary/"

        fibo_predicates = {
            f"{CMNS_AV}adaptedFrom": "fibo:adaptedFrom",
            f"{CMNS_AV}explanatoryNote": "fibo:explanatoryNote",
            str(OWL.versionInfo): "version",
        }

        for predicate_uri, prop_name in fibo_predicates.items():
            predicate = URIRef(predicate_uri)
            for obj in graph.objects(uri, predicate):
                if obj:
                    properties[prop_name] = str(obj)

        return properties

    def _extract_shacl_constraints_description(  # noqa: C901
        self, graph: Graph, term_uri: URIRef
    ) -> Optional[str]:
        """
        Extract SHACL constraints from a term and generate a human-readable description.

        Per spec Section 3.8, only extracts constraints from terms that are dual-typed
        as both skos:Concept and sh:PropertyShape (Hybrid Term-Constraint Pattern).
        """
        from rdflib import Namespace
        from rdflib.namespace import SKOS

        SH = Namespace("http://www.w3.org/ns/shacl#")

        # Per spec Section 3.8: Only extract from terms that ARE PropertyShapes (dual-typed)
        if (term_uri, RDF.type, SH.PropertyShape) not in graph:
            return None

        # Get term name for context
        term_name = None
        for label in graph.objects(term_uri, SKOS.prefLabel):
            if isinstance(label, Literal):
                term_name = str(label)
                break

        # Extract datatype from the term (which is a PropertyShape)
        datatype = None
        for dt in graph.objects(term_uri, SH.datatype):
            if isinstance(dt, URIRef):
                dt_str = str(dt)
                if "string" in dt_str.lower():
                    datatype = "string"
                elif "integer" in dt_str.lower() or "int" in dt_str.lower():
                    datatype = "integer"
                elif (
                    "decimal" in dt_str.lower()
                    or "float" in dt_str.lower()
                    or "double" in dt_str.lower()
                ):
                    datatype = "decimal"
                elif "date" in dt_str.lower():
                    datatype = "date"
                elif "boolean" in dt_str.lower() or "bool" in dt_str.lower():
                    datatype = "boolean"
                else:
                    datatype = dt_str.split("#")[-1].split("/")[-1]
                break

        # Extract numeric range constraints from the term
        min_inclusive = None
        max_inclusive = None
        for min_val in graph.objects(term_uri, SH.minInclusive):
            if isinstance(min_val, Literal):
                min_inclusive = str(min_val)
        for max_val in graph.objects(term_uri, SH.maxInclusive):
            if isinstance(max_val, Literal):
                max_inclusive = str(max_val)

        # Extract string length constraints from the term
        min_length = None
        max_length = None
        for min_len in graph.objects(term_uri, SH.minLength):
            if isinstance(min_len, Literal):
                min_length = int(min_len)
        for max_len in graph.objects(term_uri, SH.maxLength):
            if isinstance(max_len, Literal):
                max_length = int(max_len)

        # Extract pattern from the term
        pattern = None
        for pat in graph.objects(term_uri, SH.pattern):
            if isinstance(pat, Literal):
                pattern = str(pat)

        # Build description
        parts = []

        if datatype:
            parts.append(f"must be {datatype}")

        if min_inclusive is not None and max_inclusive is not None:
            parts.append(f"between {min_inclusive} and {max_inclusive}")
        elif min_inclusive is not None:
            parts.append(f"at least {min_inclusive}")
        elif max_inclusive is not None:
            parts.append(f"at most {max_inclusive}")

        if min_length is not None and max_length is not None:
            if min_length == max_length:
                parts.append(f"exactly {min_length} characters")
            else:
                parts.append(f"between {min_length} and {max_length} characters")
        elif min_length is not None:
            parts.append(f"at least {min_length} characters")
        elif max_length is not None:
            parts.append(f"at most {max_length} characters")

        if pattern:
            parts.append(f"matching pattern: {pattern}")

        if not parts:
            return None

        # Combine parts
        description = ", ".join(parts)
        if term_name:
            return f"{term_name} {description}"
        else:
            return description.capitalize()

    def _extract_alternative_labels(self, graph: Graph, uri: URIRef) -> List[str]:
        """Extract alternative labels (skos:altLabel)."""
        labels = []
        for obj in graph.objects(uri, SKOS.altLabel):
            if isinstance(obj, Literal):
                labels.append(str(obj))
        return labels

    def _extract_hidden_labels(self, graph: Graph, uri: URIRef) -> List[str]:
        """Extract hidden labels (skos:hiddenLabel)."""
        labels = []
        for obj in graph.objects(uri, SKOS.hiddenLabel):
            if isinstance(obj, Literal):
                labels.append(str(obj))
        return labels

    def _extract_notation(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract notation (skos:notation)."""
        for obj in graph.objects(uri, SKOS.notation):
            if isinstance(obj, Literal):
                return str(obj)
        return None

    def _extract_scope_note(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract scope note (skos:scopeNote)."""
        for obj in graph.objects(uri, SKOS.scopeNote):
            if isinstance(obj, Literal):
                return str(obj)
        return None
