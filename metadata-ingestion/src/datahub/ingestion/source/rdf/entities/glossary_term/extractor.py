"""
Glossary Term Extractor

Extracts glossary terms from RDF graphs and creates DataHub AST objects directly.
Supports SKOS Concepts, OWL Classes, and other glossary-like entities.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from rdflib import RDF, RDFS, Graph, Literal, URIRef
from rdflib.namespace import DC, DCTERMS, OWL, SKOS

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.glossary_term.ast import DataHubGlossaryTerm
from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
    GlossaryTermUrnGenerator,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.rdf.dialects.base import RDFDialectInterface

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
        self,
        dialect: Any = None,
        urn_generator: Optional[GlossaryTermUrnGenerator] = None,
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

        # Dialect is always provided by RDF source (or defaults to GenericDialect for tests)
        # Let dialect decide - it knows what types it supports (SKOS.Concept, OWL.Class, etc.)
        dialect = self._get_dialect()
        return dialect.looks_like_glossary_term(graph, uri)

    def extract(
        self, graph: Graph, uri: URIRef, context: Optional[Dict[str, Any]] = None
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

            # LLM-oriented properties: rdf:definition, types, hierarchy, provenance, SHACL, etc.
            self._extract_llm_properties(graph, uri, custom_properties)

            # Generate DataHub URN
            term_urn = self.urn_generator.generate_glossary_term_urn(source_uri)

            # Extract path segments for domain hierarchy
            path_segments = list(
                self.urn_generator.derive_path_from_iri(source_uri, include_last=True)
            )

            return DataHubGlossaryTerm(
                urn=term_urn,
                name=name,
                definition=definition,
                source=source_uri,
                custom_properties=custom_properties,
                path_segments=path_segments,
            )

        except (ValueError, RuntimeError, KeyError) as e:
            logger.warning(f"Error extracting glossary term from {uri}: {e}")
            return None

    def extract_all(
        self, graph: Graph, context: Optional[Dict[str, Any]] = None
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

        # Get dialect to use its filtering logic
        # Dialect is always provided by RDF source via context
        # (or defaults to GenericDialect for tests)
        dialect = self._get_dialect(context)

        # Use dialect to decide what's a glossary term
        # Each dialect knows what types it supports:
        # - FIBO: only OWL.Class
        # - Default: only SKOS.Concept
        # - Generic: SKOS.Concept and OWL.Class
        # Check all potential types and let dialect filter
        potential_types = [SKOS.Concept, OWL.Class, OWL.NamedIndividual]

        for term_type in potential_types:
            for subject in graph.subjects(RDF.type, term_type):
                if isinstance(subject, URIRef) and str(subject) not in seen_uris:
                    # Check for excluded types
                    is_excluded = False
                    for rdf_type in graph.objects(subject, RDF.type):
                        if rdf_type in excluded_types:
                            is_excluded = True
                            break

                    if not is_excluded:
                        # Let dialect decide if this is a glossary term
                        if dialect.looks_like_glossary_term(graph, subject):
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

    def _get_dialect(
        self, context: Optional[Dict[str, Any]] = None
    ) -> "RDFDialectInterface":
        """
        Get the dialect instance from context or self.dialect.

        If no dialect is provided, returns GenericDialect as a default.
        In production, RDF source always provides dialect via context.

        Returns:
            Dialect instance (never None)
        """
        # Dialect comes from context (preferred) or self.dialect (fallback)
        # RDF source always provides dialect in context
        dialect = (
            context.get("dialect") if context and isinstance(context, dict) else None
        ) or self.dialect

        # Provide default dialect if none is available (for tests and flexibility)
        if dialect is None:
            from datahub.ingestion.source.rdf.dialects.generic import GenericDialect

            dialect = GenericDialect()

        return dialect

    def _extract_custom_properties(
        self, graph: Graph, uri: URIRef, context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Extract custom properties, including dialect-specific ones."""
        # Dialect is always provided by RDF source (or defaults to GenericDialect for tests)
        dialect = self._get_dialect(context)
        # Let dialect extract its own custom properties
        return dialect.extract_custom_properties(graph, uri, context)

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

    # --- LLM-oriented properties: RDF definition, SHACL, types, hierarchy, provenance ---

    _MAX_DEFINITION_TURTLE_CHARS = 50000

    def _serialize_subgraph_to_turtle(
        self,
        graph: Graph,
        uri: URIRef,
        as_subject: bool,
    ) -> Optional[str]:
        """
        Build a subgraph of triples where uri is subject (or object) and serialize to Turtle.

        Copies namespace bindings from the source graph so output uses prefixes.
        Returns None if the subgraph is empty. Truncates at _MAX_DEFINITION_TURTLE_CHARS.
        """
        from io import BytesIO

        if as_subject:
            triples = list(graph.triples((uri, None, None)))
        else:
            triples = list(graph.triples((None, None, uri)))
        if not triples:
            return None

        subgraph = Graph()
        for prefix, ns in graph.namespaces():
            subgraph.bind(prefix, ns)
        for t in triples:
            subgraph.add(t)

        out = BytesIO()
        try:
            subgraph.serialize(destination=out, format="turtle", encoding="utf-8")
            s = out.getvalue().decode("utf-8")
        except Exception as e:
            logger.debug("Failed to serialize subgraph to Turtle: %s", e)
            return None

        if not s or not s.strip():
            return None
        if len(s) > self._MAX_DEFINITION_TURTLE_CHARS:
            s = s[: self._MAX_DEFINITION_TURTLE_CHARS] + "\n# ... (truncated)"
        return s

    def _extract_rdf_definitions(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract RDF definition triples (outgoing and incoming)."""
        rdf_def = self._serialize_subgraph_to_turtle(graph, uri, as_subject=True)
        if rdf_def:
            custom_properties["rdf:definition"] = rdf_def

        rdf_incoming = self._serialize_subgraph_to_turtle(graph, uri, as_subject=False)
        if rdf_incoming:
            custom_properties["rdf:definitionIncoming"] = rdf_incoming

    def _extract_types_and_hierarchy(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract RDF types and SKOS hierarchy (broader/narrower)."""
        types = [str(o) for o in graph.objects(uri, RDF.type) if o]
        if types:
            custom_properties["rdf:types"] = ", ".join(types)

        broader = [
            str(o) for o in graph.objects(uri, SKOS.broader) if isinstance(o, URIRef)
        ]
        narrower = [
            str(o) for o in graph.objects(uri, SKOS.narrower) if isinstance(o, URIRef)
        ]
        if broader:
            custom_properties["skos:broaderIRIs"] = ", ".join(broader)
        if narrower:
            custom_properties["skos:narrowerIRIs"] = ", ".join(narrower)

    def _extract_equivalence_mappings(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract equivalence and mapping relationships."""
        exact = [
            str(o) for o in graph.objects(uri, SKOS.exactMatch) if isinstance(o, URIRef)
        ]
        close = [
            str(o) for o in graph.objects(uri, SKOS.closeMatch) if isinstance(o, URIRef)
        ]
        equiv = [
            str(o)
            for o in graph.objects(uri, OWL.equivalentClass)
            if isinstance(o, URIRef)
        ]
        same_as = [
            str(o) for o in graph.objects(uri, OWL.sameAs) if isinstance(o, URIRef)
        ]
        sub_class = [
            str(o) for o in graph.objects(uri, RDFS.subClassOf) if isinstance(o, URIRef)
        ]

        if exact:
            custom_properties["skos:exactMatch"] = ", ".join(exact)
        if close:
            custom_properties["skos:closeMatch"] = ", ".join(close)
        if equiv:
            custom_properties["owl:equivalentClass"] = ", ".join(equiv)
        if same_as:
            custom_properties["owl:sameAs"] = ", ".join(same_as)
        if sub_class:
            custom_properties["rdfs:subClassOf"] = ", ".join(sub_class)

    def _extract_ontology_reference(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract ontology/vocabulary reference (isDefinedBy)."""
        if (
            "rdf:isDefinedBy" not in custom_properties
            and "fibo:isDefinedBy" not in custom_properties
        ):
            defined_by = list(graph.objects(uri, RDFS.isDefinedBy))
            if defined_by:
                custom_properties["rdf:isDefinedBy"] = str(defined_by[0])

    def _extract_provenance(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract provenance information (source, version, created, modified)."""
        parts = []
        for obj in graph.objects(uri, DCTERMS.source):
            if obj:
                parts.append(f"source: {obj}")
        for obj in graph.objects(uri, OWL.versionInfo):
            if obj:
                parts.append(f"version: {obj}")
        for obj in graph.objects(uri, DCTERMS.created):
            if obj:
                parts.append(f"created: {obj}")
        for obj in graph.objects(uri, DCTERMS.modified):
            if obj:
                parts.append(f"modified: {obj}")
        if parts:
            custom_properties["rdf:provenance"] = "; ".join(parts)

    def _extract_owl_property_info(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract OWL domain and range for OWL properties."""
        is_property = (uri, RDF.type, OWL.ObjectProperty) in graph or (
            uri,
            RDF.type,
            OWL.DatatypeProperty,
        ) in graph
        if not is_property:
            return

        domains = [
            str(o) for o in graph.objects(uri, RDFS.domain) if isinstance(o, URIRef)
        ]
        ranges = [
            str(o) for o in graph.objects(uri, RDFS.range) if isinstance(o, URIRef)
        ]
        if domains:
            custom_properties["owl:domain"] = ", ".join(domains)
        if ranges:
            custom_properties["owl:range"] = ", ".join(ranges)

    def _extract_shacl_definition(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract raw SHACL definition when term is a PropertyShape."""
        from rdflib import Namespace

        SH = Namespace("http://www.w3.org/ns/shacl#")
        if (uri, RDF.type, SH.PropertyShape) in graph:
            shacl_turtle = self._serialize_subgraph_to_turtle(
                graph, uri, as_subject=True
            )
            if shacl_turtle:
                custom_properties["shacl:definition"] = shacl_turtle

    def _extract_lexical_summary(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """Extract lexical summary for quick LLM scanning."""
        name = self._extract_name(graph, uri) or ""
        definition = self._extract_definition(graph, uri) or ""
        alt = self._extract_alternative_labels(graph, uri)
        scope = self._extract_scope_note(graph, uri) or ""

        summary_parts = [f"Preferred label: {name}."]
        if definition:
            summary_parts.append(f"Definition: {definition}.")
        if alt:
            summary_parts.append(f"Alternative labels: {', '.join(alt)}.")
        if scope:
            summary_parts.append(f"Scope note: {scope}.")
        custom_properties["rdf:lexicalSummary"] = " ".join(summary_parts)

    def _extract_llm_properties(
        self, graph: Graph, uri: URIRef, custom_properties: Dict[str, Any]
    ) -> None:
        """
        Add properties that help an LLM reason about and use the term:
        rdf:definition, rdf:types, hierarchy IRIs, equivalence/mapping, provenance,
        raw SHACL when applicable, OWL domain/range for properties, lexical summary.
        """
        self._extract_rdf_definitions(graph, uri, custom_properties)
        self._extract_types_and_hierarchy(graph, uri, custom_properties)
        self._extract_equivalence_mappings(graph, uri, custom_properties)
        self._extract_ontology_reference(graph, uri, custom_properties)
        self._extract_provenance(graph, uri, custom_properties)
        self._extract_owl_property_info(graph, uri, custom_properties)
        self._extract_shacl_definition(graph, uri, custom_properties)
        self._extract_lexical_summary(graph, uri, custom_properties)
