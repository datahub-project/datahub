"""
Dataset Converter

Converts RDF AST datasets to DataHub AST format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import EntityConverter
from datahub.ingestion.source.rdf.entities.dataset.ast import (
    DataHubDataset,
    RDFDataset,
    RDFSchemaField,
)
from datahub.ingestion.source.rdf.entities.dataset.urn_generator import (
    DatasetUrnGenerator,
)

logger = logging.getLogger(__name__)


class DatasetConverter(EntityConverter[RDFDataset, DataHubDataset]):
    """
    Converts RDF datasets to DataHub datasets.

    Handles:
    - URN generation from IRIs
    - Platform and environment assignment
    - Schema field conversion
    - Field-to-glossary-term relationships
    - Path segment extraction for domain hierarchy
    """

    def __init__(self, urn_generator: DatasetUrnGenerator = None):
        """
        Initialize the converter.

        Args:
            urn_generator: URN generator for creating DataHub URNs
        """
        self.urn_generator = urn_generator or DatasetUrnGenerator()

    @property
    def entity_type(self) -> str:
        return "dataset"

    def convert(
        self, rdf_dataset: RDFDataset, context: Dict[str, Any] = None
    ) -> Optional[DataHubDataset]:
        """
        Convert an RDF dataset to DataHub format.

        Args:
            rdf_dataset: The RDF dataset to convert
            context: Optional context with 'environment' setting
        """
        try:
            environment = context.get("environment", "PROD") if context else "PROD"

            # Generate DataHub URN
            dataset_urn = self.urn_generator.generate_dataset_urn(
                rdf_dataset.uri, rdf_dataset.platform, environment
            )

            # Convert schema fields
            schema_fields = self._convert_schema_fields(rdf_dataset.schema_fields)

            # Extract field-to-glossary-term relationships
            field_glossary_relationships = self._extract_field_glossary_relationships(
                rdf_dataset.schema_fields
            )

            # Parse IRI path into segments for domain hierarchy (as tuple)
            path_segments = tuple(
                self.urn_generator.derive_path_from_iri(
                    rdf_dataset.uri, include_last=True
                )
            )

            # Build custom properties
            custom_props = dict(rdf_dataset.custom_properties or {})

            # Ensure original IRI is preserved
            if "rdf:originalIRI" not in custom_props:
                custom_props["rdf:originalIRI"] = rdf_dataset.uri

            # Add properties (convert dates to strings)
            for key, value in (rdf_dataset.properties or {}).items():
                if key not in ["title", "description"]:
                    if hasattr(value, "isoformat"):
                        custom_props[key] = value.isoformat()
                    else:
                        custom_props[key] = str(value)

            return DataHubDataset(
                urn=dataset_urn,
                name=rdf_dataset.name,
                description=rdf_dataset.description,
                platform=rdf_dataset.platform,
                environment=environment,
                schema_fields=schema_fields,
                structured_properties=[],
                custom_properties=custom_props,
                path_segments=path_segments,
                field_glossary_relationships=field_glossary_relationships,
            )

        except Exception as e:
            logger.warning(f"Error converting dataset {rdf_dataset.name}: {e}")
            return None

    def convert_all(
        self, rdf_datasets: List[RDFDataset], context: Dict[str, Any] = None
    ) -> List[DataHubDataset]:
        """Convert all RDF datasets to DataHub format."""
        datahub_datasets = []

        for rdf_dataset in rdf_datasets:
            datahub_dataset = self.convert(rdf_dataset, context)
            if datahub_dataset:
                datahub_datasets.append(datahub_dataset)
                logger.debug(f"Converted dataset: {datahub_dataset.name}")

        logger.info(f"Converted {len(datahub_datasets)} datasets")
        return datahub_datasets

    def _convert_schema_fields(self, rdf_fields: List[RDFSchemaField]) -> List:
        """Convert RDF schema fields to DataHub format."""
        from datahub.metadata.schema_classes import SchemaFieldClass

        datahub_fields = []

        for field in rdf_fields:
            native_type = self._map_field_type_to_native(field.field_type)

            schema_field = SchemaFieldClass(
                fieldPath=field.name,
                nativeDataType=native_type,
                type=self._get_schema_field_data_type(field.field_type),
                description=field.description,
                nullable=field.nullable,
            )
            datahub_fields.append(schema_field)

        return datahub_fields

    def _map_field_type_to_native(self, field_type: str) -> str:
        """Map generic field type to native database type."""
        type_mapping = {
            "string": "VARCHAR",
            "number": "NUMERIC",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "time": "TIME",
        }
        return type_mapping.get(field_type, "VARCHAR")

    def _get_schema_field_data_type(self, field_type: str):
        """Get DataHub SchemaFieldDataType from field type string."""
        from datahub.metadata.schema_classes import (
            BooleanTypeClass,
            DateTypeClass,
            NumberTypeClass,
            SchemaFieldDataTypeClass,
            StringTypeClass,
            TimeTypeClass,
        )

        type_mapping = {
            "string": SchemaFieldDataTypeClass(type=StringTypeClass()),
            "number": SchemaFieldDataTypeClass(type=NumberTypeClass()),
            "boolean": SchemaFieldDataTypeClass(type=BooleanTypeClass()),
            "date": SchemaFieldDataTypeClass(type=DateTypeClass()),
            "datetime": SchemaFieldDataTypeClass(type=TimeTypeClass()),
            "time": SchemaFieldDataTypeClass(type=TimeTypeClass()),
        }

        return type_mapping.get(
            field_type, SchemaFieldDataTypeClass(type=StringTypeClass())
        )

    def _extract_field_glossary_relationships(
        self, schema_fields: List[RDFSchemaField]
    ) -> Dict[str, List[str]]:
        """Extract field-to-glossary-term relationships."""
        relationships = {}

        for field in schema_fields:
            if field.glossary_term_urns:
                relationships[field.name] = field.glossary_term_urns

        return relationships
