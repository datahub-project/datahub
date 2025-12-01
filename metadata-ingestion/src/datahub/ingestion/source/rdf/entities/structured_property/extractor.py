"""
Structured Property Extractor

Extracts structured property definitions and value assignments from RDF graphs.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.structured_property.ast import (
    RDFStructuredProperty,
    RDFStructuredPropertyValue,
)

logger = logging.getLogger(__name__)

# Namespaces
DH = Namespace("urn:li:")
SCHEMA = Namespace("http://schema.org/")
VOID = Namespace("http://rdfs.org/ns/void#")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCTERMS = Namespace("http://purl.org/dc/terms/")


class StructuredPropertyExtractor(EntityExtractor[RDFStructuredProperty]):
    """
    Extracts structured property definitions from RDF graphs.

    Identifies structured properties using (per old implementation):
    - owl:ObjectProperty (primary identifier)
    - owl:DatatypeProperty
    - rdf:Property
    - dh:StructuredProperty (DataHub-specific)
    """

    # Property type indicators in priority order
    PROPERTY_INDICATORS = [OWL.ObjectProperty, OWL.DatatypeProperty, RDF.Property]

    @property
    def entity_type(self) -> str:
        return "structured_property"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI is a structured property definition."""
        # Check for dh:StructuredProperty type (DataHub-specific)
        for _ in graph.triples((uri, RDF.type, DH.StructuredProperty)):
            return True

        # Check for OWL/RDF property types (per old implementation)
        for indicator in self.PROPERTY_INDICATORS:
            if (uri, RDF.type, indicator) in graph:
                return True

        return False

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None
    ) -> Optional[RDFStructuredProperty]:
        """Extract a single structured property definition."""
        try:
            # Get name
            name = None
            for label in graph.objects(uri, RDFS.label):
                if isinstance(label, Literal):
                    name = str(label)
                    break

            if not name:
                name = str(uri).split("/")[-1].split("#")[-1]

            # Get description
            description = None
            for desc in graph.objects(uri, RDFS.comment):
                if isinstance(desc, Literal):
                    description = str(desc)
                    break

            # Get value type
            value_type = "string"
            for vtype in graph.objects(uri, DH.valueType):
                if isinstance(vtype, Literal):
                    value_type = str(vtype)
                    break

            # Get allowed values
            allowed_values = []
            for av in graph.objects(uri, DH.allowedValues):
                if isinstance(av, Literal):
                    allowed_values.append(str(av))

            # Get entity types
            entity_types = []
            for et in graph.objects(uri, DH.entityTypes):
                if isinstance(et, Literal):
                    entity_types.append(str(et))

            # Get cardinality
            cardinality = None
            for card in graph.objects(uri, DH.cardinality):
                if isinstance(card, Literal):
                    cardinality = str(card)
                    break

            return RDFStructuredProperty(
                uri=str(uri),
                name=name,
                description=description,
                value_type=value_type,
                allowed_values=allowed_values,
                entity_types=entity_types,
                cardinality=cardinality,
                properties={},
            )

        except Exception as e:
            logger.warning(f"Error extracting structured property from {uri}: {e}")
            return None

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFStructuredProperty]:
        """Extract all structured property definitions from the RDF graph."""
        properties = []
        seen_uris = set()

        # Find all dh:StructuredProperty entities (DataHub-specific)
        for prop_uri in graph.subjects(RDF.type, DH.StructuredProperty):
            if isinstance(prop_uri, URIRef) and str(prop_uri) not in seen_uris:
                prop = self.extract(graph, prop_uri, context)
                if prop:
                    properties.append(prop)
                    seen_uris.add(str(prop_uri))

        # Find all OWL/RDF property types (per old implementation)
        for indicator in self.PROPERTY_INDICATORS:
            for prop_uri in graph.subjects(RDF.type, indicator):
                if isinstance(prop_uri, URIRef) and str(prop_uri) not in seen_uris:
                    prop = self._extract_owl_rdf_property(graph, prop_uri, context)
                    if prop:
                        properties.append(prop)
                        seen_uris.add(str(prop_uri))

        logger.info(f"Extracted {len(properties)} structured properties")
        return properties

    def _extract_owl_rdf_property(  # noqa: C901
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None
    ) -> Optional[RDFStructuredProperty]:
        """Extract a structured property from owl:ObjectProperty, owl:DatatypeProperty, or rdf:Property."""
        try:
            # Get name
            name = None
            for label in graph.objects(uri, RDFS.label):
                if isinstance(label, Literal):
                    name = str(label)
                    break

            if not name:
                name = str(uri).split("/")[-1].split("#")[-1]

            # Get description
            description = None
            for desc in graph.objects(uri, RDFS.comment):
                if isinstance(desc, Literal):
                    description = str(desc)
                    break

            # Get value type and allowed values from rdfs:range
            value_type = "string"
            allowed_values = []
            range_class_uri = None

            for range_val in graph.objects(uri, RDFS.range):
                if isinstance(range_val, URIRef):
                    range_str = str(range_val)
                    # Check if it's a datatype (xsd:*, rdf:*, etc.)
                    if "string" in range_str.lower() or "xsd:string" in range_str:
                        value_type = "string"
                    elif (
                        "integer" in range_str.lower()
                        or "xsd:integer" in range_str
                        or "decimal" in range_str.lower()
                        or "float" in range_str.lower()
                        or "xsd:decimal" in range_str
                    ):
                        value_type = "number"
                    elif "date" in range_str.lower() or "xsd:date" in range_str:
                        value_type = "date"
                    elif "boolean" in range_str.lower() or "xsd:boolean" in range_str:
                        value_type = "boolean"
                    else:
                        # Not a datatype - might be an enumeration class
                        # Check if it's a class with instances (enumeration pattern)
                        if (range_val, RDF.type, RDFS.Class) in graph or (
                            range_val,
                            RDF.type,
                            OWL.Class,
                        ) in graph:
                            range_class_uri = range_val
                            value_type = (
                                "string"  # Enum values are typically strings in DataHub
                            )
                    break

            # Extract allowed values from enumeration class instances
            if range_class_uri:
                # Find all instances of the range class (enumeration values)
                for instance in graph.subjects(RDF.type, range_class_uri):
                    if isinstance(instance, URIRef):
                        # Get the label of the instance
                        instance_label = None
                        for label in graph.objects(instance, RDFS.label):
                            if isinstance(label, Literal):
                                instance_label = str(label).strip()
                                break

                        # If no label, use the local name
                        if not instance_label:
                            instance_label = str(instance).split("/")[-1].split("#")[-1]

                        if instance_label:
                            allowed_values.append(instance_label)

            # If no enum class found but description contains enum pattern, try to extract from comment
            # Pattern: "value1, value2, value3" or "(value1, value2, value3)" in comment
            if not allowed_values and description:
                import re

                # Look for patterns like "(HIGH, MEDIUM, LOW)" or "HIGH, MEDIUM, LOW"
                enum_pattern = r"\(([A-Z][A-Z\s,]+)\)|([A-Z][A-Z\s,]+)"
                matches = re.findall(enum_pattern, description)
                if matches:
                    # Take the first match and split by comma
                    enum_str = matches[0][0] if matches[0][0] else matches[0][1]
                    if enum_str:
                        # Split by comma and clean up
                        potential_values = [v.strip() for v in enum_str.split(",")]
                        # Only use if we have 2+ values and they look like enum values (all caps, short)
                        if len(potential_values) >= 2 and all(
                            len(v) < 20 and v.isupper() or v[0].isupper()
                            for v in potential_values
                        ):
                            allowed_values = potential_values
                            logger.debug(
                                f"Extracted enum values from comment for {uri}: {allowed_values}"
                            )

            # Get entity types from rdfs:domain (per spec section 7.2)
            entity_types = []
            domain_type_mapping = {
                str(DCAT.Dataset): "dataset",
                "http://www.w3.org/2004/02/skos/core#Concept": "glossaryTerm",
                str(SCHEMA.Person): "user",
                str(SCHEMA.Organization): "corpGroup",
                str(SCHEMA.DataCatalog): "dataPlatform",
            }

            for domain in graph.objects(uri, RDFS.domain):
                if isinstance(domain, URIRef):
                    domain_str = str(domain)
                    # owl:Thing means the property can apply to any entity type
                    # Don't add it to entity_types - let converter handle it
                    if "Thing" in domain_str and "owl" in domain_str.lower():
                        # Skip - means universal domain
                        continue
                    elif domain_str in domain_type_mapping:
                        entity_types.append(domain_type_mapping[domain_str])
                    else:
                        # Use generic name
                        entity_types.append(domain_str.split("/")[-1].split("#")[-1])

            return RDFStructuredProperty(
                uri=str(uri),
                name=name,
                description=description,
                value_type=value_type,
                allowed_values=allowed_values,
                entity_types=entity_types,
                cardinality=None,
                properties={},
            )

        except Exception as e:
            logger.warning(f"Error extracting OWL/RDF property {uri}: {e}")
            return None

    def extract_values(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFStructuredPropertyValue]:
        """
        Extract structured property value assignments from the graph.

        Supports two patterns:
        1. Blank node pattern: entity dh:hasStructuredPropertyValue [ dh:property prop ; dh:value value ]
        2. Direct assignment: entity prop_uri value (where prop_uri is a structured property)
        """
        values = []
        environment = context.get("environment", "PROD") if context else "PROD"

        # Get all structured property definitions first
        property_defs = {}
        for prop in self.extract_all(graph, context):
            property_defs[prop.uri] = prop

        # Pattern 1: Blank node pattern (dh:hasStructuredPropertyValue)
        for entity in graph.subjects(DH.hasStructuredPropertyValue, None):
            if isinstance(entity, URIRef):
                # Get entity type - skip if cannot be determined
                entity_type = self._get_entity_type(graph, entity)
                if not entity_type:
                    logger.debug(
                        f"Skipping structured property value assignment for {entity}: "
                        f"entity type cannot be determined"
                    )
                    continue

                platform = self._extract_platform(graph, entity)

                for bnode in graph.objects(entity, DH.hasStructuredPropertyValue):
                    prop_uri = None
                    value = None

                    for p in graph.objects(bnode, DH.property):
                        prop_uri = str(p) if isinstance(p, URIRef) else None

                    for v in graph.objects(bnode, DH.value):
                        value = str(v) if isinstance(v, Literal) else None

                    if prop_uri and value:
                        prop_name = property_defs.get(prop_uri, {})
                        prop_name = (
                            prop_name.name
                            if hasattr(prop_name, "name")
                            else prop_uri.split("/")[-1]
                        )

                        values.append(
                            RDFStructuredPropertyValue(
                                entity_uri=str(entity),
                                property_uri=prop_uri,
                                property_name=prop_name,
                                value=value,
                                entity_type=entity_type,
                                platform=platform,
                                environment=environment,
                            )
                        )

        # Pattern 2: Direct property assignments
        # For each structured property, find all entities that have it assigned
        for prop_uri, prop_def in property_defs.items():
            prop_uri_ref = URIRef(prop_uri)

            # Find all entities that have this property assigned
            for entity, value_obj in graph.subject_objects(prop_uri_ref):
                if not isinstance(entity, URIRef):
                    continue

                # Get entity type - skip if cannot be determined
                entity_type = self._get_entity_type(graph, entity)
                if not entity_type:
                    logger.debug(
                        f"Skipping structured property value assignment for {entity}: "
                        f"entity type cannot be determined"
                    )
                    continue

                platform = self._extract_platform(graph, entity)

                # Extract value - handle both URIRef (ObjectProperty) and Literal (DatatypeProperty)
                if isinstance(value_obj, URIRef):
                    # For ObjectProperty, use the URI's label or local name
                    value = None
                    for label in graph.objects(value_obj, RDFS.label):
                        if isinstance(label, Literal):
                            value = str(label)
                            break
                    if not value:
                        value = str(value_obj).split("/")[-1].split("#")[-1]
                elif isinstance(value_obj, Literal):
                    value = str(value_obj)
                else:
                    continue

                if value:
                    prop_name = (
                        prop_def.name
                        if hasattr(prop_def, "name")
                        else prop_uri.split("/")[-1]
                    )

                    values.append(
                        RDFStructuredPropertyValue(
                            entity_uri=str(entity),
                            property_uri=prop_uri,
                            property_name=prop_name,
                            value=value,
                            entity_type=entity_type,
                            platform=platform,
                            environment=environment,
                        )
                    )

        logger.info(f"Extracted {len(values)} structured property value assignments")
        return values

    def _get_entity_type(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Determine the entity type from RDF types.

        Returns None if entity type cannot be determined.
        """

        SKOS_NS = Namespace("http://www.w3.org/2004/02/skos/core#")
        DPROD = Namespace("https://ekgf.github.io/dprod/")

        for rdf_type in graph.objects(uri, RDF.type):
            type_str = str(rdf_type)
            if (
                "Dataset" in type_str
                or type_str == str(VOID.Dataset)
                or type_str == str(DCAT.Dataset)
            ):
                return "dataset"
            if "Concept" in type_str or type_str == str(SKOS_NS.Concept):
                return "glossaryTerm"
            if "DataProduct" in type_str or type_str == str(DPROD.DataProduct):
                return "dataProduct"

        # Return None if entity type cannot be determined - no defaulting
        return None

    def _extract_platform(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract platform from dcat:accessService."""
        for service in graph.objects(uri, DCAT.accessService):
            for title in graph.objects(service, DCTERMS.title):
                if isinstance(title, Literal):
                    return str(title).strip()
            if isinstance(service, URIRef):
                return str(service).split("/")[-1].split("#")[-1].lower()
        return None
