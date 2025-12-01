"""
Assertion Extractor

Extracts data quality assertions from RDF graphs using SHACL constraints.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import RDF, Graph, Literal, Namespace, URIRef

from datahub.ingestion.source.rdf.entities.assertion.ast import RDFAssertion
from datahub.ingestion.source.rdf.entities.base import EntityExtractor

logger = logging.getLogger(__name__)

# Namespaces
SH = Namespace("http://www.w3.org/ns/shacl#")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
VOID = Namespace("http://rdfs.org/ns/void#")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCTERMS = Namespace("http://purl.org/dc/terms/")


class AssertionExtractor(EntityExtractor[RDFAssertion]):
    """
    Extracts data quality assertions from RDF graphs.

    Identifies assertions from:
    - SHACL property constraints (sh:minCount, sh:maxCount, sh:minLength, etc.)
    - SHACL node shapes with validation rules
    """

    @property
    def entity_type(self) -> str:
        return "assertion"

    def can_extract(self, graph: Graph, uri: URIRef) -> bool:
        """Check if this URI has SHACL constraints that can be assertions."""
        # Check if it's a NodeShape
        for _ in graph.triples((uri, RDF.type, SH.NodeShape)):
            return True
        return False

    def extract(
        self, graph: Graph, uri: URIRef, context: Dict[str, Any] = None
    ) -> Optional[RDFAssertion]:
        """Extract a single assertion - not applicable for SHACL."""
        return None  # Assertions are extracted in bulk from SHACL

    def extract_all(
        self, graph: Graph, context: Dict[str, Any] = None
    ) -> List[RDFAssertion]:
        """Extract all assertions from the RDF graph.

        Assertions are only created if explicitly enabled via context configuration:
        - create_assertions: bool = False (main flag, default False)
        - assertion_types: dict with sub-flags:
          - required_fields: bool = False (for minCount/maxCount → NOT_NULL)
          - field_size: bool = False (for minLength/maxLength)
          - value_checks: bool = False (for minInclusive/maxInclusive, pattern)
        """
        # Check if assertions are enabled
        if not self._should_create_assertions(context):
            logger.debug(
                "Assertions are disabled. Set create_assertions=True in context to enable."
            )
            return []

        assertions = []
        environment = context.get("environment", "PROD") if context else "PROD"

        # Find all datasets and their SHACL constraints (inline)
        datasets = self._get_datasets_with_shapes(graph, environment)

        for dataset_info in datasets:
            dataset_urn = dataset_info["urn"]
            shape_uri = dataset_info["shape_uri"]

            # Extract property constraints as assertions
            shape_assertions = self._extract_shape_assertions(
                graph, shape_uri, dataset_urn, context
            )
            assertions.extend(shape_assertions)

        # Also find standalone NodeShapes (only if they have a platform/dataset)
        # Skip standalone shapes without platforms - they can't create valid assertions
        standalone_assertions = self._extract_standalone_shapes(
            graph, environment, context
        )
        assertions.extend(standalone_assertions)

        logger.info(f"Extracted {len(assertions)} assertions")
        return assertions

    def _should_create_assertions(self, context: Dict[str, Any] = None) -> bool:
        """Check if assertions should be created based on context configuration."""
        if not context:
            return False

        # Main flag: create_assertions must be True
        create_assertions = context.get("create_assertions", False)
        if not create_assertions:
            return False

        # If create_assertions is True, check if any assertion type is enabled
        assertion_types = context.get("assertion_types", {})
        if isinstance(assertion_types, dict):
            # If assertion_types dict is empty, default to enabling all types
            if not assertion_types:
                return True
            # Otherwise, at least one assertion type must be explicitly enabled
            return any(
                [
                    assertion_types.get("required_fields", False),
                    assertion_types.get("field_size", False),
                    assertion_types.get("value_checks", False),
                ]
            )

        # If assertion_types is not a dict, default to True when create_assertions=True
        return True

    def _should_create_required_field_assertions(
        self, context: Dict[str, Any] = None
    ) -> bool:
        """Check if required field assertions (minCount/maxCount) should be created."""
        if not self._should_create_assertions(context):
            return False
        assertion_types = context.get("assertion_types", {})
        # Default to True if assertion_types is empty (all types enabled)
        if not assertion_types or not isinstance(assertion_types, dict):
            return True
        return assertion_types.get(
            "required_fields", True
        )  # Default True when create_assertions=True

    def _should_create_field_size_assertions(
        self, context: Dict[str, Any] = None
    ) -> bool:
        """Check if field size assertions (minLength/maxLength) should be created."""
        if not self._should_create_assertions(context):
            return False
        assertion_types = context.get("assertion_types", {})
        # Default to True if assertion_types is empty (all types enabled)
        if not assertion_types or not isinstance(assertion_types, dict):
            return True
        return assertion_types.get(
            "field_size", True
        )  # Default True when create_assertions=True

    def _should_create_value_check_assertions(
        self, context: Dict[str, Any] = None
    ) -> bool:
        """Check if value check assertions (minInclusive/maxInclusive, pattern) should be created."""
        if not self._should_create_assertions(context):
            return False
        assertion_types = context.get("assertion_types", {})
        # Default to True if assertion_types is empty (all types enabled)
        if not assertion_types or not isinstance(assertion_types, dict):
            return True
        return assertion_types.get(
            "value_checks", True
        )  # Default True when create_assertions=True

    def _extract_standalone_shapes(
        self, graph: Graph, environment: str, context: Dict[str, Any] = None
    ) -> List[RDFAssertion]:
        """Extract assertions from standalone NodeShapes.

        Only processes NodeShapes that have a platform (linked to a dataset).
        Skips standalone shapes without platforms - they can't create valid assertions.
        """
        from datahub.ingestion.source.rdf.entities.dataset.urn_generator import (
            DatasetUrnGenerator,
        )

        assertions = []
        dataset_urn_generator = DatasetUrnGenerator()

        # Find all NodeShapes
        # Only process shapes that have a platform (linked to a dataset)
        for shape_uri in graph.subjects(RDF.type, SH.NodeShape):
            if isinstance(shape_uri, URIRef):
                # Check if this shape has a platform (linked to a dataset)
                platform = self._extract_platform(graph, shape_uri)
                if not platform:
                    # Skip standalone shapes without platforms - they need to be linked to a dataset
                    logger.debug(
                        f"Skipping standalone NodeShape {shape_uri} - no platform found. Link to a dataset with dcat:accessService to create assertions."
                    )
                    continue

                # Use shape URI as dataset identifier
                shape_str = str(shape_uri)
                dataset_urn = dataset_urn_generator.generate_dataset_urn(
                    shape_str, platform, environment
                )

                # Extract property constraints
                shape_assertions = self._extract_shape_assertions(
                    graph, shape_uri, dataset_urn, context
                )
                assertions.extend(shape_assertions)

        return assertions

    def _get_datasets_with_shapes(
        self, graph: Graph, environment: str
    ) -> List[Dict[str, Any]]:
        """Find datasets that have SHACL shapes."""
        from datahub.ingestion.source.rdf.entities.dataset.urn_generator import (
            DatasetUrnGenerator,
        )

        datasets = []
        dataset_urn_generator = DatasetUrnGenerator()

        # Look for datasets with sh:property
        dataset_types = [VOID.Dataset, DCAT.Dataset]

        for dtype in dataset_types:
            for dataset_uri in graph.subjects(RDF.type, dtype):
                if isinstance(dataset_uri, URIRef):
                    # Check if dataset has SHACL properties
                    has_shape = False
                    for _ in graph.objects(dataset_uri, SH.property):
                        has_shape = True
                        break

                    if has_shape:
                        # Get platform (will default to "logical" if None via URN generator)
                        platform = self._extract_platform(graph, dataset_uri)
                        dataset_urn = dataset_urn_generator.generate_dataset_urn(
                            str(dataset_uri), platform, environment
                        )

                        datasets.append(
                            {
                                "uri": str(dataset_uri),
                                "urn": dataset_urn,
                                "shape_uri": dataset_uri,  # Dataset itself has the properties
                            }
                        )

        # Look for datasets that reference NodeShapes via dcterms:conformsTo (proper RDF pattern)
        for dtype in dataset_types:
            for dataset_uri in graph.subjects(RDF.type, dtype):
                if isinstance(dataset_uri, URIRef):
                    # Check if dataset has dcterms:conformsTo pointing to a NodeShape
                    for shape_ref in graph.objects(dataset_uri, DCTERMS.conformsTo):
                        if isinstance(shape_ref, URIRef):
                            # Check if it's a NodeShape
                            if (shape_ref, RDF.type, SH.NodeShape) in graph:
                                # Get platform (will default to "logical" if None via URN generator)
                                platform = self._extract_platform(graph, dataset_uri)
                                dataset_urn = (
                                    dataset_urn_generator.generate_dataset_urn(
                                        str(dataset_uri), platform, environment
                                    )
                                )

                                # Don't add duplicates
                                if not any(
                                    d["uri"] == str(dataset_uri)
                                    and d["shape_uri"] == shape_ref
                                    for d in datasets
                                ):
                                    datasets.append(
                                        {
                                            "uri": str(dataset_uri),
                                            "urn": dataset_urn,
                                            "shape_uri": shape_ref,
                                        }
                                    )

        # Also look for standalone NodeShapes that target datasets via sh:targetClass
        for shape_uri in graph.subjects(RDF.type, SH.NodeShape):
            if isinstance(shape_uri, URIRef):
                # Check if it targets a dataset class
                for _target_class in graph.objects(shape_uri, SH.targetClass):
                    # Try to match this to a dataset
                    for dtype in dataset_types:
                        for dataset_uri in graph.subjects(RDF.type, dtype):
                            if isinstance(dataset_uri, URIRef):
                                # Get platform (will default to "logical" if None via URN generator)
                                platform = self._extract_platform(graph, dataset_uri)
                                dataset_urn = (
                                    dataset_urn_generator.generate_dataset_urn(
                                        str(dataset_uri), platform, environment
                                    )
                                )

                                # Don't add duplicates
                                if not any(
                                    d["uri"] == str(dataset_uri)
                                    and d["shape_uri"] == shape_uri
                                    for d in datasets
                                ):
                                    datasets.append(
                                        {
                                            "uri": str(dataset_uri),
                                            "urn": dataset_urn,
                                            "shape_uri": shape_uri,
                                        }
                                    )

        return datasets

    def _extract_shape_assertions(
        self,
        graph: Graph,
        shape_uri: URIRef,
        dataset_urn: str,
        context: Dict[str, Any] = None,
    ) -> List[RDFAssertion]:
        """Extract assertions from a SHACL shape."""
        assertions = []

        # Process each sh:property
        for prop_shape in graph.objects(shape_uri, SH.property):
            prop_assertions = self._extract_property_assertions(
                graph, prop_shape, dataset_urn, context
            )
            assertions.extend(prop_assertions)

        return assertions

    def _extract_property_assertions(  # noqa: C901
        self, graph: Graph, prop_shape, dataset_urn: str, context: Dict[str, Any] = None
    ) -> List[RDFAssertion]:
        """Extract assertions from a SHACL property shape."""
        assertions = []

        # Get field name/path - try multiple patterns
        field_name = None

        # Try sh:path first
        for path in graph.objects(prop_shape, SH.path):
            if isinstance(path, URIRef):
                field_name = str(path).split("/")[-1].split("#")[-1]
            elif isinstance(path, Literal):
                field_name = str(path)
            break

        # Try sh:node (bcbs239 pattern - node points to a term URI)
        if not field_name:
            for node in graph.objects(prop_shape, SH.node):
                if isinstance(node, URIRef):
                    field_name = str(node).split("/")[-1].split("#")[-1]
                break

        # Also try sh:name
        if not field_name:
            for name in graph.objects(prop_shape, SH.name):
                if isinstance(name, Literal):
                    field_name = str(name)
                    break

        if not field_name:
            return assertions

        # Extract cardinality constraints together for semantic interpretation
        min_count_val = None
        max_count_val = None

        for min_count in graph.objects(prop_shape, SH.minCount):
            if isinstance(min_count, Literal):
                min_count_val = int(min_count)
                break

        for max_count in graph.objects(prop_shape, SH.maxCount):
            if isinstance(max_count, Literal):
                max_count_val = int(max_count)
                break

        # Interpret cardinality semantically:
        # - minCount=1, maxCount=1 → required field (not null)
        # - minCount=0, maxCount=1 → optional field (no assertion needed)
        # - minCount=0, maxCount=N → optional multi-value (no assertion needed)
        # - minCount>1 or maxCount>1 with minCount>0 → actual cardinality constraint

        # Only create required field assertions if enabled
        if (
            self._should_create_required_field_assertions(context)
            and min_count_val is not None
            and min_count_val >= 1
        ):
            if max_count_val == 1:
                # Required single-value field (not null)
                assertions.append(
                    RDFAssertion(
                        assertion_key=f"{dataset_urn}_{field_name}_not_null",
                        assertion_type="FIELD_METRIC",
                        dataset_urn=dataset_urn,
                        field_name=field_name,
                        description=f"Field {field_name} is required",
                        operator="NOT_NULL",
                        parameters={
                            "minCount": min_count_val,
                            "maxCount": max_count_val,
                        },
                    )
                )
            elif max_count_val is None or max_count_val > 1:
                # Required with potential multiple values - create a "required" assertion
                assertions.append(
                    RDFAssertion(
                        assertion_key=f"{dataset_urn}_{field_name}_required",
                        assertion_type="FIELD_METRIC",
                        dataset_urn=dataset_urn,
                        field_name=field_name,
                        description=f"Field {field_name} requires at least {min_count_val} value(s)",
                        operator="GREATER_THAN_OR_EQUAL",
                        parameters={"minCount": min_count_val},
                    )
                )
                # If maxCount > 1, also add cardinality constraint
                if max_count_val is not None and max_count_val > 1:
                    assertions.append(
                        RDFAssertion(
                            assertion_key=f"{dataset_urn}_{field_name}_cardinality",
                            assertion_type="FIELD_METRIC",
                            dataset_urn=dataset_urn,
                            field_name=field_name,
                            description=f"Field {field_name} allows {min_count_val} to {max_count_val} values",
                            operator="BETWEEN",
                            parameters={
                                "minCount": min_count_val,
                                "maxCount": max_count_val,
                            },
                        )
                    )
        # minCount=0 with maxCount=1 is just "optional" - no assertion needed
        # minCount=0 with maxCount>1 is "optional multi-value" - no assertion needed

        # In bcbs239 pattern, constraints may be on the referenced sh:node rather than
        # the property shape itself. Follow the reference to get additional constraints.
        constraint_sources = [prop_shape]
        for node_ref in graph.objects(prop_shape, SH.node):
            if isinstance(node_ref, URIRef):
                constraint_sources.append(node_ref)

        # Track which constraints we've already added to avoid duplicates
        seen_constraints = set()

        # Extract constraints from all sources (property shape and referenced nodes)
        # Only create assertions if the corresponding flag is enabled
        for source in constraint_sources:
            # Extract minLength constraint (field_size)
            if self._should_create_field_size_assertions(context):
                for min_len in graph.objects(source, SH.minLength):
                    if isinstance(min_len, Literal):
                        key = f"{field_name}_min_length"
                        if key not in seen_constraints:
                            seen_constraints.add(key)
                            length = int(min_len)
                            assertions.append(
                                RDFAssertion(
                                    assertion_key=f"{dataset_urn}_{field_name}_min_length",
                                    assertion_type="FIELD_VALUES",
                                    dataset_urn=dataset_urn,
                                    field_name=field_name,
                                    description=f"Field {field_name} minimum length: {length}",
                                    operator="GREATER_THAN_OR_EQUAL",
                                    parameters={"minLength": length},
                                )
                            )

            # Extract maxLength constraint (field_size)
            if self._should_create_field_size_assertions(context):
                for max_len in graph.objects(source, SH.maxLength):
                    if isinstance(max_len, Literal):
                        key = f"{field_name}_max_length"
                        if key not in seen_constraints:
                            seen_constraints.add(key)
                            length = int(max_len)
                            assertions.append(
                                RDFAssertion(
                                    assertion_key=f"{dataset_urn}_{field_name}_max_length",
                                    assertion_type="FIELD_VALUES",
                                    dataset_urn=dataset_urn,
                                    field_name=field_name,
                                    description=f"Field {field_name} maximum length: {length}",
                                    operator="LESS_THAN_OR_EQUAL",
                                    parameters={"maxLength": length},
                                )
                            )

            # Extract pattern constraint (value_checks)
            if self._should_create_value_check_assertions(context):
                for pattern in graph.objects(source, SH.pattern):
                    if isinstance(pattern, Literal):
                        key = f"{field_name}_pattern_{str(pattern)}"
                        if key not in seen_constraints:
                            seen_constraints.add(key)
                            assertions.append(
                                RDFAssertion(
                                    assertion_key=f"{dataset_urn}_{field_name}_pattern",
                                    assertion_type="FIELD_VALUES",
                                    dataset_urn=dataset_urn,
                                    field_name=field_name,
                                    description=f"Field {field_name} must match pattern: {str(pattern)}",
                                    operator="MATCHES",
                                    parameters={"pattern": str(pattern)},
                                )
                            )

            # Extract minInclusive constraint (value_checks)
            if self._should_create_value_check_assertions(context):
                for min_val in graph.objects(source, SH.minInclusive):
                    if isinstance(min_val, Literal):
                        key = f"{field_name}_min_value"
                        if key not in seen_constraints:
                            seen_constraints.add(key)
                            assertions.append(
                                RDFAssertion(
                                    assertion_key=f"{dataset_urn}_{field_name}_min_value",
                                    assertion_type="FIELD_METRIC",
                                    dataset_urn=dataset_urn,
                                    field_name=field_name,
                                    description=f"Field {field_name} minimum value: {min_val}",
                                    operator="GREATER_THAN_OR_EQUAL",
                                    parameters={"minValue": float(min_val)},
                                )
                            )

            # Extract maxInclusive constraint (value_checks)
            if self._should_create_value_check_assertions(context):
                for max_val in graph.objects(source, SH.maxInclusive):
                    if isinstance(max_val, Literal):
                        key = f"{field_name}_max_value"
                        if key not in seen_constraints:
                            seen_constraints.add(key)
                            assertions.append(
                                RDFAssertion(
                                    assertion_key=f"{dataset_urn}_{field_name}_max_value",
                                    assertion_type="FIELD_METRIC",
                                    dataset_urn=dataset_urn,
                                    field_name=field_name,
                                    description=f"Field {field_name} maximum value: {max_val}",
                                    operator="LESS_THAN_OR_EQUAL",
                                    parameters={"maxValue": float(max_val)},
                                )
                            )

            # Skip datatype constraints - these are schema information, not data quality assertions
            # Datatype is handled during schema field creation, not as assertions

        return assertions

    def _extract_platform(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract platform from dcat:accessService."""
        for service in graph.objects(uri, DCAT.accessService):
            for title in graph.objects(service, DCTERMS.title):
                if isinstance(title, Literal):
                    return str(title).strip()
            if isinstance(service, URIRef):
                return str(service).split("/")[-1].split("#")[-1].lower()
        return None
