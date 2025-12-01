"""
Dataset Extractor

Extracts datasets from RDF graphs and creates RDF AST objects.
Supports void:Dataset, dcat:Dataset, and schema:Dataset patterns.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS

from datahub.ingestion.source.rdf.entities.base import EntityExtractor
from datahub.ingestion.source.rdf.entities.dataset.ast import (
    RDFDataset,
    RDFSchemaField,
)

logger = logging.getLogger(__name__)

# Namespaces
VOID = Namespace("http://rdfs.org/ns/void#")
SCHEMA = Namespace("http://schema.org/")
SH = Namespace("http://www.w3.org/ns/shacl#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")


class DatasetExtractor(EntityExtractor[RDFDataset]):
    """
    Extracts datasets from RDF graphs.

    Identifies entities as datasets if they:
    - Have type void:Dataset, dcat:Dataset, or schema:Dataset
    - Or have dataset-like properties (dcat:accessService, etc.)

    Extracts:
    - Basic properties (name, description, platform)
    - Schema fields from SHACL NodeShapes
    - Custom properties including original IRI
    """

    def __init__(self, dialect=None):
        """
        Initialize the extractor.

        Args:
            dialect: Optional dialect for dialect-specific extraction
        """
        self.dialect = dialect

    @property
    def entity_type(self) -> str:
        return "dataset"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI represents a dataset."""
        # Exclude schema definitions - these should be part of the main dataset (per old implementation)
        if "#schema_def" in str(uri):
            return False

        dataset_types = {VOID.Dataset, DCAT.Dataset, SCHEMA.Dataset}

        for rdf_type in graph.objects(uri, RDF.type):
            if rdf_type in dataset_types:
                return True

        # Also check for dataset-like properties
        return self._looks_like_dataset(graph, uri)

    def _looks_like_dataset(self, graph: Graph, uri: URIRef) -> bool:
        """Check if a URI looks like a dataset based on properties."""
        # Exclude schema definitions (per old implementation)
        if "#schema_def" in str(uri):
            return False

        dataset_properties = [
            DCAT.accessService,
            DCAT.distribution,
            VOID.sparqlEndpoint,
            VOID.triples,  # Added per old implementation
            DCTERMS.publisher,
        ]

        return any(any(graph.objects(uri, prop)) for prop in dataset_properties)

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None
    ) -> Optional[RDFDataset]:
        """
        Extract a single dataset from the RDF graph.

        Args:
            graph: The RDF graph
            uri: The URI of the dataset to extract
            context: Optional context with extraction settings
        """
        try:
            # Extract basic properties
            name = self._extract_name(graph, uri)
            if not name:
                return None

            description = self._extract_description(graph, uri)
            platform = self._extract_platform(graph, uri)

            # Extract custom properties
            custom_properties = self._extract_custom_properties(graph, uri)
            custom_properties["rdf:originalIRI"] = str(uri)

            # Create dataset first (schema fields need reference to it)
            dataset = RDFDataset(
                uri=str(uri),
                name=name,
                platform=platform,
                description=description,
                environment=None,  # Set by caller
                schema_fields=[],
                properties=custom_properties,
                custom_properties=custom_properties,
            )

            # Extract schema fields
            schema_fields = self._extract_schema_fields(graph, uri, dataset)
            dataset.schema_fields = schema_fields

            return dataset

        except Exception as e:
            logger.warning(f"Error extracting dataset from {uri}: {e}")
            return None

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFDataset]:
        """Extract all datasets from the RDF graph."""
        datasets = []
        seen_uris = set()

        # Find datasets by type
        dataset_types = [VOID.Dataset, DCAT.Dataset, SCHEMA.Dataset]

        for dataset_type in dataset_types:
            for subject in graph.subjects(RDF.type, dataset_type):
                if isinstance(subject, URIRef) and str(subject) not in seen_uris:
                    dataset = self.extract(graph, subject, context)
                    if dataset:
                        datasets.append(dataset)
                        seen_uris.add(str(subject))

        # Also find by properties
        for subject in graph.subjects():
            if isinstance(subject, URIRef) and str(subject) not in seen_uris:
                if self._looks_like_dataset(graph, subject):
                    dataset = self.extract(graph, subject, context)
                    if dataset:
                        datasets.append(dataset)
                        seen_uris.add(str(subject))

        logger.info(f"Extracted {len(datasets)} datasets")
        return datasets

    # --- Private extraction methods ---

    def _extract_name(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """
        Extract name from dcterms:title property.

        Per specification, dcterms:title is the primary property for dataset names.
        Falls back to local name from URI if dcterms:title is not found.
        """
        # Per specification, dcterms:title is the primary property
        for obj in graph.objects(uri, DCTERMS.title):
            if isinstance(obj, Literal):
                name = str(obj).strip()
                if name:
                    return name

        # Fallback: use local name from URI
        local_name = str(uri).split("/")[-1].split("#")[-1]
        if local_name:
            return local_name.replace("_", " ")

        return None

    def _extract_description(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """
        Extract description from dataset properties.

        Per specification: dcterms:description → schema:description → rdfs:comment
        """
        # Priority order per specification: dcterms:description → schema:description → rdfs:comment
        description_properties = [DCTERMS.description, SCHEMA.description, RDFS.comment]

        for prop in description_properties:
            for obj in graph.objects(uri, prop):
                if isinstance(obj, Literal):
                    description = str(obj).strip()
                    if description:
                        return description

        return None

    def _extract_platform(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract platform from dcat:accessService.

        Requires dcat:accessService pointing to a service with dcterms:title.
        Returns None if platform cannot be determined - no fallback to URI parsing.
        """
        # Check dcat:accessService
        for service in graph.objects(uri, DCAT.accessService):
            # Get the title of the service
            for title in graph.objects(service, DCTERMS.title):
                if isinstance(title, Literal):
                    return str(title).strip()

        return None

    def _extract_custom_properties(self, graph: Graph, uri: URIRef) -> Dict[str, Any]:
        """Extract custom properties."""
        properties = {}

        # Extract common metadata properties
        metadata_properties = [
            (DCTERMS.created, "created"),
            (DCTERMS.modified, "modified"),
            (DCTERMS.publisher, "publisher"),
            (DCTERMS.creator, "creator"),
        ]

        for prop, name in metadata_properties:
            for obj in graph.objects(uri, prop):
                if obj:
                    properties[name] = str(obj)

        return properties

    def _extract_schema_fields(
        self, graph: Graph, uri: URIRef, dataset: RDFDataset
    ) -> List[RDFSchemaField]:
        """Extract schema fields from SHACL NodeShape via dcterms:conformsTo.

        This is the only supported method per RDF-lite specification.
        Datasets must link to their schema via dcterms:conformsTo pointing to a sh:NodeShape.
        """
        fields = []

        # Look for dcterms:conformsTo pointing to a NodeShape
        # This is the proper RDF pattern per specification
        schema_refs = list(graph.objects(uri, DCTERMS.conformsTo))

        if not schema_refs:
            logger.warning(
                f"Dataset {uri} has no dcterms:conformsTo property. "
                f"Schema fields cannot be extracted. Add dcterms:conformsTo pointing to a sh:NodeShape."
            )
            return fields

        for schema_ref in schema_refs:
            if not isinstance(schema_ref, URIRef):
                logger.warning(
                    f"Dataset {uri} has dcterms:conformsTo with non-URI value: {schema_ref}. "
                    f"Expected a URI reference to a sh:NodeShape."
                )
                continue

            # Check if this is a NodeShape
            if (schema_ref, RDF.type, SH.NodeShape) not in graph:
                logger.warning(
                    f"Dataset {uri} references {schema_ref} via dcterms:conformsTo, "
                    f"but {schema_ref} is not a sh:NodeShape. Schema fields cannot be extracted."
                )
                continue

            fields.extend(
                self._extract_fields_from_nodeshape(graph, schema_ref, dataset)
            )

        return fields

    def _extract_fields_from_nodeshape(
        self, graph: Graph, nodeshape: URIRef, dataset: RDFDataset
    ) -> List[RDFSchemaField]:
        """Extract fields from a SHACL NodeShape."""
        fields = []

        for prop_shape in graph.objects(nodeshape, SH.property):
            field = self._create_field_from_property_shape(graph, prop_shape, dataset)
            if field:
                fields.append(field)

        return fields

    def _create_field_from_property_shape(  # noqa: C901
        self, graph: Graph, prop_shape, dataset: RDFDataset
    ) -> Optional[RDFSchemaField]:
        """Create a schema field from a SHACL property shape."""
        try:
            # Collect sources for field properties - check both the property shape
            # and any referenced sh:node (bcbs239 pattern)
            sources = [prop_shape]
            node_ref = None

            for node in graph.objects(prop_shape, SH.node):
                if isinstance(node, URIRef):
                    sources.append(node)
                    node_ref = node
                    break

            # Get field name from sh:name or sh:path (check all sources)
            name = None
            for source in sources:
                for name_obj in graph.objects(source, SH.name):
                    if isinstance(name_obj, Literal):
                        name = str(name_obj)
                        break
                if name:
                    break

            if not name:
                for source in sources:
                    for path_obj in graph.objects(source, SH.path):
                        if isinstance(path_obj, URIRef):
                            name = str(path_obj).split("/")[-1].split("#")[-1]
                            break
                    if name:
                        break

            # If still no name, try to get from the node reference URI (bcbs239 pattern)
            if not name and node_ref:
                name = str(node_ref).split("/")[-1].split("#")[-1].replace("_", " ")

            if not name:
                return None

            # Get field type from sh:datatype (check all sources)
            field_type = "string"  # Default
            for source in sources:
                for datatype in graph.objects(source, SH.datatype):
                    if isinstance(datatype, URIRef):
                        type_name = str(datatype).split("#")[-1]
                        field_type = self._map_xsd_type(type_name)
                        break
                if field_type != "string":
                    break

            # Get description (check all sources)
            description = None
            for source in sources:
                for desc in graph.objects(source, SH.description):
                    if isinstance(desc, Literal):
                        description = str(desc)
                        break
                if description:
                    break

            # Check for glossary term association
            glossary_term_urns = []
            for source in sources:
                for class_obj in graph.objects(source, SH["class"]):
                    if isinstance(class_obj, URIRef):
                        # Check if this is a SKOS Concept
                        if (class_obj, RDF.type, SKOS.Concept) in graph:
                            # Convert to URN
                            from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
                                GlossaryTermUrnGenerator,
                            )

                            urn_gen = GlossaryTermUrnGenerator()
                            glossary_term_urns.append(
                                urn_gen.generate_glossary_term_urn(str(class_obj))
                            )

            # Also check if the sh:node reference itself is a SKOS Concept (bcbs239 pattern)
            if node_ref and (node_ref, RDF.type, SKOS.Concept) in graph:
                from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
                    GlossaryTermUrnGenerator,
                )

                urn_gen = GlossaryTermUrnGenerator()
                term_urn = urn_gen.generate_glossary_term_urn(str(node_ref))
                if term_urn not in glossary_term_urns:
                    glossary_term_urns.append(term_urn)

            # Extract minCount/maxCount for nullable field calculation (schema metadata)
            # This is always extracted regardless of assertion configuration
            min_count_val = None
            max_count_val = None
            for source in sources:
                for min_count in graph.objects(source, SH.minCount):
                    if isinstance(min_count, Literal):
                        min_count_val = int(min_count)
                        break
                if min_count_val is not None:
                    break

            for source in sources:
                for max_count in graph.objects(source, SH.maxCount):
                    if isinstance(max_count, Literal):
                        max_count_val = int(max_count)
                        break
                if max_count_val is not None:
                    break

            # Set nullable based on minCount: minCount >= 1 means field is required (not nullable)
            # minCount = 0 or None means field is optional (nullable)
            nullable = True  # Default to nullable
            if min_count_val is not None and min_count_val >= 1:
                nullable = False

            # Store cardinality constraints in contextual_constraints for potential assertion creation
            contextual_constraints = {}
            if min_count_val is not None:
                contextual_constraints["minCount"] = min_count_val
            if max_count_val is not None:
                contextual_constraints["maxCount"] = max_count_val

            return RDFSchemaField(
                name=name,
                field_type=field_type,
                description=description,
                nullable=nullable,
                glossary_term_urns=glossary_term_urns,
                dataset=dataset,
                property_shape_uri=str(prop_shape)
                if isinstance(prop_shape, URIRef)
                else None,
                contextual_constraints=contextual_constraints,
            )

        except Exception as e:
            logger.warning(f"Error creating field from property shape: {e}")
            return None

    def _map_xsd_type(self, xsd_type: str) -> str:
        """Map XSD type to DataHub field type."""
        type_mapping = {
            "string": "string",
            "integer": "number",
            "int": "number",
            "long": "number",
            "decimal": "number",
            "float": "number",
            "double": "number",
            "boolean": "boolean",
            "date": "date",
            "dateTime": "datetime",
            "time": "time",
        }
        return type_mapping.get(xsd_type, "string")
