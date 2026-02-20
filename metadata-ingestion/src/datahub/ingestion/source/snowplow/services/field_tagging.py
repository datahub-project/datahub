"""
Field tagging and structured properties for Snowplow schemas.

Auto-generates tags and/or structured properties for schema fields based on:
- Schema version
- Event type
- Data classification (PII, Sensitive)
- Authorship
"""

import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.constants import DataClassification
from datahub.ingestion.source.snowplow.snowplow_config import FieldTaggingConfig
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    SchemaFieldClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)


@dataclass
class FieldTagContext:
    """Context for generating field tags and structured properties."""

    schema_version: str
    vendor: str
    name: str
    field_name: str
    field_type: Optional[str]
    field_description: Optional[str]
    deployment_initiator: Optional[str]  # Who deployed this version
    deployment_timestamp: Optional[str]  # ISO 8601 timestamp when deployed
    pii_fields: Set[str]  # PII fields from PII enrichment config
    skip_version_tag: bool = False  # Skip version tag for initial version fields
    event_type: Optional[str] = None  # Type: "self_describing", "atomic", "context"


class FieldTagger:
    """Generates tags for schema fields."""

    def __init__(self, config: FieldTaggingConfig):
        self.config = config

    def generate_tags(self, context: FieldTagContext) -> Optional[GlobalTagsClass]:
        """
        Generate tags for a field based on context.

        Returns:
            GlobalTagsClass with tags, or None if no tags to add
        """
        if not self.config.enabled:
            return None

        # Skip tag generation if using only structured properties
        if (
            self.config.use_structured_properties
            and not self.config.emit_tags_and_structured_properties
        ):
            return None

        tags: Set[str] = set()

        # If pii_tags_only is enabled, skip version and authorship tags
        # but still emit data classification tags
        skip_non_pii_tags = (
            self.config.pii_tags_only
            and self.config.use_structured_properties
            and self.config.emit_tags_and_structured_properties
        )

        # Schema version tag (skip for initial version fields)
        if (
            self.config.tag_schema_version
            and not context.skip_version_tag
            and not skip_non_pii_tags
        ):
            version_tag = self._make_version_tag(context.schema_version)
            tags.add(version_tag)

        # Note: Event type tag is applied at dataset level, not field level
        # See _process_schema() where dataset-level tags are added

        # Data classification tags (always emitted when enabled, even with pii_tags_only)
        if self.config.tag_data_class:
            class_tags = self._classify_field(context.field_name, context.pii_fields)
            tags.update(class_tags)

        # Authorship tag (skip for initial version fields, same as version tag)
        if (
            self.config.tag_authorship
            and context.deployment_initiator
            and not context.skip_version_tag
            and not skip_non_pii_tags
        ):
            author_tag = self._make_authorship_tag(context.deployment_initiator)
            tags.add(author_tag)

        if not tags:
            return None

        # Convert to TagAssociationClass
        tag_associations = [
            TagAssociationClass(tag=f"urn:li:tag:{tag}") for tag in sorted(tags)
        ]

        return GlobalTagsClass(tags=tag_associations)

    def _make_version_tag(self, version: str) -> str:
        """Create schema version tag."""
        # Convert 1-1-0 to v1-1-0
        clean_version = version.replace(".", "-")
        return self.config.schema_version_pattern.format(version=clean_version)

    def _make_event_type_tag(self, name: str) -> str:
        """Create event type tag."""
        # Convert checkout_started to checkout
        event_name = name.split("_")[0] if "_" in name else name
        return self.config.event_type_pattern.format(name=event_name)

    def _make_authorship_tag(self, author: str) -> str:
        """Create authorship tag."""
        # Convert "Jane Doe" to "jane_doe" or extract email username
        # Handle email addresses (jane.doe@company.com -> jane_doe)
        if "@" in author:
            author = author.split("@")[0]

        # Convert to lowercase and replace spaces/dots with underscores
        author_slug = author.lower().replace(" ", "_").replace(".", "_")
        return self.config.authorship_pattern.format(author=author_slug)

    def _classify_field(self, field_name: str, pii_fields: Set[str]) -> Set[str]:
        """
        Classify field as PII, Sensitive, etc.

        Args:
            field_name: Name of the field
            pii_fields: Set of PII field names from enrichment config

        Returns:
            Set of classification tags
        """
        tags = set()

        field_lower = field_name.lower()

        # Check PII from enrichment config first (most accurate)
        if self.config.use_pii_enrichment and field_name in pii_fields:
            tags.add("PII")
            return tags  # Don't check patterns if we have explicit PII config

        # Fall back to pattern matching if enrichment not available
        # Check PII patterns
        for pattern in self.config.pii_field_patterns:
            if pattern in field_lower:
                tags.add("PII")
                break

        # Check Sensitive patterns
        for pattern in self.config.sensitive_field_patterns:
            if pattern in field_lower:
                tags.add("Sensitive")
                break

        return tags

    def generate_field_structured_properties(
        self,
        dataset_urn: str,
        field: SchemaFieldClass,
        context: FieldTagContext,
        field_properties_cache: Optional[
            dict[str, List[StructuredPropertyValueAssignmentClass]]
        ] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate structured properties for a schema field.

        Args:
            dataset_urn: URN of the parent dataset
            field: The schema field
            context: Field tagging context with authorship and classification info
            field_properties_cache: Optional dict to store properties by field_path
                for later propagation to event_spec datasets

        Yields:
            MetadataWorkUnit containing structured properties for the field
        """
        if not self.config.use_structured_properties:
            return

        # Only emit structured properties if we have deployment metadata
        # (author, version, or timestamp). If we only have data_class/event_type
        # but no deployment info, skip emission - those can be represented as tags.
        has_deployment_info = bool(
            context.deployment_initiator
            or context.schema_version
            or context.deployment_timestamp
        )
        if not has_deployment_info:
            return

        properties: List[StructuredPropertyValueAssignmentClass] = []

        # Author - only add if we have a meaningful value
        if self.config.tag_authorship and context.deployment_initiator:
            author_slug = self._normalize_author_name(context.deployment_initiator)
            # Validate: not None, not empty, not just whitespace
            if author_slug and author_slug.strip():
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAuthor",
                        values=[author_slug.strip()],  # Ensure trimmed value
                    )
                )

        # Version
        if self.config.tag_schema_version and context.schema_version:
            # Keep version format consistent: 1-0-2
            version_clean = context.schema_version.replace(".", "-")
            # Only add if not empty after normalization
            if version_clean and version_clean.strip():
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldVersionAdded",
                        values=[version_clean],
                    )
                )

        # Timestamp
        if context.deployment_timestamp:
            # Only add if not empty
            if context.deployment_timestamp.strip():
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldAddedTimestamp",
                        values=[context.deployment_timestamp],
                    )
                )

        # Data classification
        if self.config.tag_data_class:
            data_class = self._get_data_classification(
                context.field_name, context.pii_fields
            )
            # Only add if not empty/None
            if data_class and data_class.strip():
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldDataClass",
                        values=[data_class],
                    )
                )

        # Event type
        if context.event_type:
            # Only add if not empty
            if context.event_type.strip():
                properties.append(
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:io.acryl.snowplow.fieldEventType",
                        values=[context.event_type],
                    )
                )

        if not properties:
            return

        # Store properties in cache for later propagation to event_spec datasets
        if field_properties_cache is not None:
            field_properties_cache[field.fieldPath] = properties

        # Create SchemaField URN
        schema_field_urn = make_schema_field_urn(dataset_urn, field.fieldPath)

        # Emit structured properties aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=schema_field_urn,
            aspect=StructuredPropertiesClass(properties=properties),
        ).as_workunit()

    def _normalize_author_name(self, author: str) -> str:
        """
        Normalize author name for structured property storage.

        Args:
            author: Author name or email

        Returns:
            Normalized author name with proper capitalization (e.g., "Jane Doe")
        """
        # Handle email addresses (jane.doe@company.com -> Jane Doe)
        if "@" in author:
            author = author.split("@")[0]

        # Replace dots and underscores with spaces, then title case
        # jane.doe -> Jane Doe
        # john_smith -> John Smith
        # JANE DOE -> Jane Doe
        author = author.replace(".", " ").replace("_", " ")
        return author.title()

    def _get_data_classification(
        self, field_name: str, pii_fields: Set[str]
    ) -> Optional[str]:
        """
        Get data classification for a field.

        Args:
            field_name: Name of the field
            pii_fields: Set of PII field names from enrichment config

        Returns:
            Classification string from DataClassification enum
        """
        field_lower = field_name.lower()

        # Check PII from enrichment config first (most accurate)
        if self.config.use_pii_enrichment and field_name in pii_fields:
            logger.info(
                f"✅ Field '{field_name}' classified as PII from enrichment config"
            )
            return DataClassification.PII.value

        # Fall back to pattern matching if enrichment not available
        # Check PII patterns
        for pattern in self.config.pii_field_patterns:
            if pattern in field_lower:
                logger.info(
                    f"✅ Field '{field_name}' classified as PII by pattern match: '{pattern}'"
                )
                return DataClassification.PII.value

        # Check Sensitive patterns
        for pattern in self.config.sensitive_field_patterns:
            if pattern in field_lower:
                logger.info(
                    f"✅ Field '{field_name}' classified as sensitive by pattern match: '{pattern}'"
                )
                return DataClassification.SENSITIVE.value

        # Default to internal for most fields
        return DataClassification.INTERNAL.value
