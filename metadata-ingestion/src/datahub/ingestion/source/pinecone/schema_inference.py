"""Schema inference from Pinecone vector metadata."""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from datahub.ingestion.source.pinecone.pinecone_client import VectorRecord
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


class MetadataSchemaInferrer:
    """
    Infers DataHub schema from Pinecone vector metadata.

    Analyzes metadata from sampled vectors to determine:
    - Field names
    - Field types (string, number, boolean, array)
    - Field frequency (how often each field appears)
    """

    # Field path format version
    FIELD_PATH_VERSION = "[version=2.0]"

    def __init__(self, max_fields: int = 100):
        """
        Initialize the schema inferrer.

        Args:
            max_fields: Maximum number of fields to include in schema
        """
        self.max_fields = max_fields

    def infer_schema(
        self,
        vectors: List[VectorRecord],
        dataset_name: str,
        platform: str = "pinecone",
    ) -> Optional[SchemaMetadataClass]:
        """
        Infer schema from a list of vector records.

        Args:
            vectors: List of vector records with metadata
            dataset_name: Name of the dataset (for logging)
            platform: Platform name

        Returns:
            SchemaMetadataClass or None if no metadata found
        """
        if not vectors:
            logger.warning(f"No vectors provided for schema inference: {dataset_name}")
            return None

        # Collect metadata statistics
        field_stats, total_vectors = self._collect_field_statistics(vectors)

        if not field_stats:
            logger.info(f"No metadata fields found in vectors: {dataset_name}")
            return None

        # Generate schema fields
        schema_fields = self._generate_schema_fields(field_stats, total_vectors)

        if not schema_fields:
            logger.warning(f"Failed to generate schema fields: {dataset_name}")
            return None

        # Create schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=schema_fields,
        )

        logger.info(
            f"Inferred schema with {len(schema_fields)} fields for {dataset_name}"
        )
        return schema_metadata

    def _collect_field_statistics(
        self, vectors: List[VectorRecord]
    ) -> tuple[Dict[str, Dict[str, Any]], int]:
        """
        Collect statistics about metadata fields across vectors.

        Args:
            vectors: List of vector records

        Returns:
            Tuple of (field statistics dictionary, total vector count)
        """
        field_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "count": 0,
                "types": set(),
                "sample_values": [],
            }
        )

        for vector in vectors:
            if not vector.metadata:
                continue

            for field_name, field_value in vector.metadata.items():
                stats = field_stats[field_name]
                stats["count"] += 1

                # Determine type
                field_type = self._infer_field_type(field_value)
                stats["types"].add(field_type)

                # Store sample values (up to 5)
                if len(stats["sample_values"]) < 5:
                    stats["sample_values"].append(field_value)

        return dict(field_stats), len(vectors)

    def _infer_field_type(self, value: Any) -> str:
        """
        Infer the type of a field value.

        Args:
            value: Field value

        Returns:
            Type name as string
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            # Check bool before int because bool is subclass of int
            return "boolean"
        elif isinstance(value, (int, float)):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "string"  # Default to string

    def _generate_schema_fields(
        self, field_stats: Dict[str, Dict[str, Any]], total_vectors: int
    ) -> List[SchemaFieldClass]:
        """
        Generate SchemaField objects from field statistics.

        Args:
            field_stats: Field statistics dictionary
            total_vectors: Total number of vectors analyzed

        Returns:
            List of SchemaFieldClass objects
        """
        if not field_stats:
            logger.warning("No field statistics to generate schema from")
            return []

        schema_fields = []

        # Sort fields by frequency (most common first)
        sorted_fields = sorted(
            field_stats.items(),
            key=lambda x: x[1]["count"],
            reverse=True,
        )

        # Limit to max_fields
        sorted_fields = sorted_fields[: self.max_fields]

        for field_name, stats in sorted_fields:
            # Determine the primary type (most common)
            types = stats["types"]
            if not types:
                continue

            # If multiple types, prefer in order: string, number, boolean, array
            primary_type = self._select_primary_type(types)

            # Create field type
            field_type = self._create_field_type(primary_type)

            # Create field path
            field_path = f"{self.FIELD_PATH_VERSION}.{field_name}"

            # Create description with accurate frequency
            frequency_pct = (
                (stats["count"] / total_vectors * 100) if total_vectors > 0 else 0
            )

            description_parts = [f"Appears in {frequency_pct:.1f}% of vectors"]

            if len(types) > 1:
                description_parts.append(f"Multiple types: {', '.join(sorted(types))}")

            # Add sample values for context
            if stats["sample_values"]:
                sample_str = ", ".join(str(v)[:50] for v in stats["sample_values"][:3])
                description_parts.append(f"Examples: {sample_str}")

            description = ". ".join(description_parts)

            # Create schema field
            schema_field = SchemaFieldClass(
                fieldPath=field_path,
                type=field_type,
                nativeDataType=primary_type,
                description=description,
                nullable=True,  # Metadata fields are optional
                recursive=False,
            )

            schema_fields.append(schema_field)

        return schema_fields

    def _select_primary_type(self, types: Set[str]) -> str:
        """
        Select the primary type when multiple types are present.

        Args:
            types: Set of type names

        Returns:
            Primary type name
        """
        # Priority order
        type_priority = ["string", "number", "boolean", "array", "object", "null"]

        for preferred_type in type_priority:
            if preferred_type in types:
                return preferred_type

        # Fallback
        return "string"

    def _create_field_type(self, type_name: str) -> SchemaFieldDataTypeClass:
        """
        Create a SchemaFieldDataType from a type name.

        Args:
            type_name: Type name (string, number, boolean, array)

        Returns:
            SchemaFieldDataTypeClass
        """
        type_mapping = {
            "string": StringTypeClass,
            "number": NumberTypeClass,
            "boolean": BooleanTypeClass,
            "array": ArrayTypeClass,
        }

        type_class = type_mapping.get(type_name, StringTypeClass)
        return SchemaFieldDataTypeClass(type=type_class())
