"""Metadata mapping for Unstructured.io output."""

import logging
from typing import Any, Dict, List

import jsonpath_ng

from datahub.ingestion.source.unstructured.config import MetadataConfig

logger = logging.getLogger(__name__)


class MetadataMapper:
    """Maps Unstructured.io metadata to Document customProperties."""

    def __init__(self, config: MetadataConfig):
        self.config = config

    def extract_custom_properties(
        self, elements: List[Dict[str, Any]], metadata: Dict[str, Any]
    ) -> Dict[str, str]:
        """Extract custom properties from Unstructured output."""
        properties: Dict[str, str] = {}

        # Wrap in a document structure for JSONPath
        doc = {"elements": elements, "metadata": metadata}

        # Apply custom property mappings
        for mapping in self.config.custom_properties:
            try:
                # Parse JSONPath
                jsonpath_expr = jsonpath_ng.parse(mapping.path)
                matches = jsonpath_expr.find(doc)

                if matches:
                    value = matches[0].value
                    # Convert to string
                    properties[mapping.name] = str(value)

            except Exception as e:
                logger.warning(
                    f"Failed to extract {mapping.name} from {mapping.path}: {e}"
                )

        # Add standard metadata if enabled
        if self.config.capture.file_metadata:
            self._add_file_metadata(properties, metadata)

        if self.config.capture.document_properties:
            self._add_document_properties(properties, metadata)

        if self.config.capture.element_statistics:
            self._add_element_statistics(properties, elements)

        if self.config.capture.processing_metadata:
            self._add_processing_metadata(properties, metadata)

        if self.config.capture.source_metadata:
            self._add_source_metadata(properties, metadata)

        return properties

    def _add_file_metadata(
        self, properties: Dict[str, str], metadata: Dict[str, Any]
    ) -> None:
        """Add file metadata to properties."""
        file_fields = {
            "filetype": "file_type",
            "filename": "source_filename",
            "file_directory": "source_directory",
            "last_modified": "last_modified",
            "file_size": "file_size_bytes",
        }

        for meta_field, prop_name in file_fields.items():
            value = metadata.get(meta_field)
            if value is not None:
                properties[prop_name] = str(value)

    def _add_document_properties(
        self, properties: Dict[str, str], metadata: Dict[str, Any]
    ) -> None:
        """Add document properties to properties."""
        doc_fields = {
            "page_count": "page_count",
            "languages": "languages",
        }

        for meta_field, prop_name in doc_fields.items():
            value = metadata.get(meta_field)
            if value is not None:
                if isinstance(value, list):
                    # Join list values
                    properties[prop_name] = ",".join(str(v) for v in value)
                    # Also add primary language
                    if meta_field == "languages" and value:
                        properties["primary_language"] = str(value[0])
                else:
                    properties[prop_name] = str(value)

    def _add_element_statistics(
        self, properties: Dict[str, str], elements: List[Dict[str, Any]]
    ) -> None:
        """Add element count statistics to properties."""
        counts = self._compute_element_statistics(elements)

        properties["element_count_total"] = str(counts.get("Total", 0))

        # Add counts by type
        for elem_type, count in counts.items():
            if elem_type != "Total":
                prop_name = f"element_count_{elem_type.lower()}"
                properties[prop_name] = str(count)

    def _add_processing_metadata(
        self, properties: Dict[str, str], metadata: Dict[str, Any]
    ) -> None:
        """Add processing metadata to properties."""
        # Add processing strategy if available
        # Note: This would need to be added by the source during processing
        pass

    def _add_source_metadata(
        self, properties: Dict[str, str], metadata: Dict[str, Any]
    ) -> None:
        """Add source system metadata to properties."""
        data_source = metadata.get("data_source", {})

        if "url" in data_source:
            properties["source_url"] = str(data_source["url"])

        if "record_locator" in data_source:
            # Convert record locator to string representation
            locator = data_source["record_locator"]
            if isinstance(locator, dict):
                # Format as key=value pairs
                locator_str = ", ".join(f"{k}={v}" for k, v in locator.items())
                properties["source_record_locator"] = locator_str
            else:
                properties["source_record_locator"] = str(locator)

    def _compute_element_statistics(
        self, elements: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Compute element count statistics."""
        counts: Dict[str, int] = {}
        total = 0

        for elem in elements:
            elem_type = elem.get("type", "Unknown")
            counts[elem_type] = counts.get(elem_type, 0) + 1
            total += 1

        counts["Total"] = total
        return counts
