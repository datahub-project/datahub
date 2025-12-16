"""
Field tagging for Snowplow schemas.

Auto-generates tags for schema fields based on:
- Schema version
- Event type
- Data classification (PII, Sensitive)
- Authorship
"""

from dataclasses import dataclass
from typing import Optional, Set

from datahub.ingestion.source.snowplow.snowplow_config import FieldTaggingConfig
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass


@dataclass
class FieldTagContext:
    """Context for generating field tags."""

    schema_version: str
    vendor: str
    name: str
    field_name: str
    field_type: Optional[str]
    field_description: Optional[str]
    deployment_initiator: Optional[str]  # Who deployed this version
    pii_fields: Set[str]  # PII fields from PII enrichment config
    skip_version_tag: bool = False  # Skip version tag for initial version fields


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

        tags: Set[str] = set()

        # Schema version tag (skip for initial version fields)
        if self.config.tag_schema_version and not context.skip_version_tag:
            version_tag = self._make_version_tag(context.schema_version)
            tags.add(version_tag)

        # Note: Event type tag is applied at dataset level, not field level
        # See _process_schema() where dataset-level tags are added

        # Data classification tags
        if self.config.tag_data_class:
            class_tags = self._classify_field(context.field_name, context.pii_fields)
            tags.update(class_tags)

        # Authorship tag (skip for initial version fields, same as version tag)
        if (
            self.config.tag_authorship
            and context.deployment_initiator
            and not context.skip_version_tag
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
