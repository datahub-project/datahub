"""
Unit tests for Snowplow field tagging.

Tests the field tagging infrastructure including:
- Tag generation
- Field classification (PII, Sensitive)
- Tag pattern customization
"""

from datahub.ingestion.source.snowplow.services.field_tagging import (
    FieldTagContext,
    FieldTagger,
)
from datahub.ingestion.source.snowplow.snowplow_config import FieldTaggingConfig


class TestFieldTagger:
    """Test the FieldTagger class."""

    def test_generate_all_tags(self):
        """Test generating all tag types for an incremental field."""
        config = FieldTaggingConfig(use_structured_properties=False)
        tagger = FieldTagger(config)

        # Incremental field (skip_version_tag=False, which is default)
        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="user_id",
            field_type="string",
            field_description="User identifier",
            deployment_initiator="ryan@company.com",
            deployment_timestamp=None,
            pii_fields={"user_id"},
            skip_version_tag=False,  # Explicit: this is an incremental field
        )

        tags = tagger.generate_tags(context)

        assert tags is not None
        tag_values = [t.tag for t in tags.tags]

        # Should have 3 tags: version, PII, authorship (event type is now dataset-level)
        assert len(tag_values) == 3
        assert "urn:li:tag:snowplow_schema_v1-0-0" in tag_values
        assert "urn:li:tag:PII" in tag_values
        assert "urn:li:tag:added_by_ryan" in tag_values

    def test_disabled_tagging(self):
        """Test that tagging can be disabled."""
        config = FieldTaggingConfig(enabled=False)
        tagger = FieldTagger(config)

        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="user_id",
            field_type="string",
            field_description=None,
            deployment_initiator="ryan@company.com",
            deployment_timestamp=None,
            pii_fields=set(),
        )

        tags = tagger.generate_tags(context)
        assert tags is None

    def test_selective_tag_types(self):
        """Test enabling only specific tag types."""
        config = FieldTaggingConfig(
            use_structured_properties=False,  # Test tags only
            tag_schema_version=True,
            tag_event_type=False,
            tag_data_class=False,
            tag_authorship=False,
        )
        tagger = FieldTagger(config)

        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="user_id",
            field_type="string",
            field_description=None,
            deployment_initiator="ryan@company.com",
            deployment_timestamp=None,
            pii_fields={"user_id"},
        )

        tags = tagger.generate_tags(context)

        assert tags is not None
        tag_values = [t.tag for t in tags.tags]

        # Only version tag should be present
        assert len(tag_values) == 1
        assert "urn:li:tag:snowplow_schema_v1-0-0" in tag_values

    def test_version_tag_formatting(self):
        """Test schema version tag formatting."""
        config = FieldTaggingConfig()
        tagger = FieldTagger(config)

        # Test default pattern
        assert tagger._make_version_tag("1-0-0") == "snowplow_schema_v1-0-0"
        assert tagger._make_version_tag("2-1-3") == "snowplow_schema_v2-1-3"

        # Test custom pattern
        config.schema_version_pattern = "v{version}"
        tagger = FieldTagger(config)
        assert tagger._make_version_tag("1-0-0") == "v1-0-0"

    def test_authorship_tag_formatting(self):
        """Test authorship tag formatting."""
        config = FieldTaggingConfig()
        tagger = FieldTagger(config)

        # Test email addresses
        assert tagger._make_authorship_tag("ryan@company.com") == "added_by_ryan"
        assert (
            tagger._make_authorship_tag("jane.doe@company.com") == "added_by_jane_doe"
        )

        # Test names (includes full name)
        assert tagger._make_authorship_tag("Ryan Smith") == "added_by_ryan_smith"
        assert tagger._make_authorship_tag("Jane") == "added_by_jane"

        # Test custom pattern
        config.authorship_pattern = "author_{author}"
        tagger = FieldTagger(config)
        assert tagger._make_authorship_tag("ryan@company.com") == "author_ryan"

    def test_pii_classification_from_enrichment(self):
        """Test PII classification from enrichment config."""
        config = FieldTaggingConfig(use_pii_enrichment=True)
        tagger = FieldTagger(config)

        # Field is in PII enrichment config
        pii_fields = {"user_id", "user_ipaddress", "user_fingerprint"}
        tags = tagger._classify_field("user_id", pii_fields)

        assert tags == {"PII"}

    def test_pii_classification_fallback_to_patterns(self):
        """Test PII classification falls back to patterns when enrichment not configured."""
        config = FieldTaggingConfig(use_pii_enrichment=False)
        tagger = FieldTagger(config)

        # Should match pattern even though not in enrichment config
        tags = tagger._classify_field("email", set())
        assert "PII" in tags

        tags = tagger._classify_field("user_id", set())
        assert "PII" in tags

        tags = tagger._classify_field("ip_address", set())
        assert "PII" in tags

    def test_sensitive_classification(self):
        """Test Sensitive field classification."""
        config = FieldTaggingConfig()
        tagger = FieldTagger(config)

        # Test sensitive patterns
        tags = tagger._classify_field("password", set())
        assert "Sensitive" in tags

        tags = tagger._classify_field("api_token", set())
        assert "Sensitive" in tags

        tags = tagger._classify_field("secret_key", set())
        assert "Sensitive" in tags

    def test_no_classification_for_normal_fields(self):
        """Test that normal fields get no classification tags."""
        config = FieldTaggingConfig()
        tagger = FieldTagger(config)

        tags = tagger._classify_field("product_id", set())
        assert len(tags) == 0

        tags = tagger._classify_field("category", set())
        assert len(tags) == 0

    def test_custom_pii_patterns(self):
        """Test custom PII field patterns."""
        config = FieldTaggingConfig(
            use_pii_enrichment=False,
            pii_field_patterns=["custom_id", "tracking_id"],
        )
        tagger = FieldTagger(config)

        tags = tagger._classify_field("custom_id", set())
        assert "PII" in tags

        tags = tagger._classify_field("tracking_id", set())
        assert "PII" in tags

        # Standard patterns shouldn't match
        tags = tagger._classify_field("user_id", set())
        assert len(tags) == 0

    def test_custom_sensitive_patterns(self):
        """Test custom Sensitive field patterns."""
        config = FieldTaggingConfig(
            sensitive_field_patterns=["internal", "confidential"]
        )
        tagger = FieldTagger(config)

        tags = tagger._classify_field("internal_notes", set())
        assert "Sensitive" in tags

        tags = tagger._classify_field("confidential_data", set())
        assert "Sensitive" in tags

    def test_no_authorship_tag_when_initiator_missing(self):
        """Test that authorship tag is not added when initiator is None."""
        config = FieldTaggingConfig(use_structured_properties=False)
        tagger = FieldTagger(config)

        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="user_id",
            field_type="string",
            field_description=None,
            deployment_initiator=None,  # No initiator
            deployment_timestamp=None,
            pii_fields=set(),
        )

        tags = tagger.generate_tags(context)

        assert tags is not None
        tag_values = [t.tag for t in tags.tags]

        # Should not have authorship tag
        assert not any("added_by" in tag for tag in tag_values)

    def test_tags_are_sorted(self):
        """Test that tags are returned in sorted order."""
        config = FieldTaggingConfig(use_structured_properties=False)
        tagger = FieldTagger(config)

        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="user_id",
            field_type="string",
            field_description=None,
            deployment_initiator="ryan@company.com",
            deployment_timestamp=None,
            pii_fields={"user_id"},
        )

        tags = tagger.generate_tags(context)

        assert tags is not None
        tag_values = [t.tag for t in tags.tags]

        # Tags should be sorted
        assert tag_values == sorted(tag_values)

    def test_skip_version_tag_for_initial_version_fields(self):
        """Test that initial version fields skip version tags."""
        config = FieldTaggingConfig(
            tag_schema_version=True,
            tag_event_type=True,
            tag_data_class=False,
            tag_authorship=False,
        )
        tagger = FieldTagger(config)

        # Field from initial version - should skip version tag
        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="items",
            field_type="array",
            field_description="Items in cart",
            deployment_initiator=None,
            deployment_timestamp=None,
            pii_fields=set(),
            skip_version_tag=True,  # Skip version tag for initial version
        )

        tags = tagger.generate_tags(context)

        # Should have NO tags (event type is dataset-level now, version skipped)
        assert tags is None

    def test_skip_authorship_tag_for_initial_version_fields(self):
        """Test that initial version fields skip authorship tags, same as version tags."""
        config = FieldTaggingConfig(
            tag_schema_version=True,
            tag_event_type=True,
            tag_data_class=False,
            tag_authorship=True,
        )
        tagger = FieldTagger(config)

        # Field from initial version with deployment_initiator - should skip authorship tag
        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="items",
            field_type="array",
            field_description="Items in cart",
            deployment_initiator="ryan@company.com",  # Has initiator
            deployment_timestamp=None,
            pii_fields=set(),
            skip_version_tag=True,  # Skip version/authorship tags for initial version
        )

        tags = tagger.generate_tags(context)

        # Should have NO tags (event type is dataset-level, version/authorship skipped)
        assert tags is None

    def test_include_version_tag_for_incremental_fields(self):
        """Test that incrementally added fields get version tags."""
        config = FieldTaggingConfig(
            use_structured_properties=False,  # Test tags only
            tag_schema_version=True,
            tag_event_type=True,
            tag_data_class=False,
            tag_authorship=True,
        )
        tagger = FieldTagger(config)

        # Field added in later version - should include version tag
        context = FieldTagContext(
            schema_version="1-1-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="discount_code",
            field_type="string",
            field_description="Discount code applied (Added in version 1-1-0)",
            deployment_initiator="ryan@company.com",
            deployment_timestamp=None,
            pii_fields=set(),
            skip_version_tag=False,  # Include version tag for incremental field
        )

        tags = tagger.generate_tags(context)

        assert tags is not None
        tag_values = [t.tag for t in tags.tags]

        # Should have version tag and authorship tag (event type is dataset-level)
        assert "urn:li:tag:snowplow_schema_v1-1-0" in tag_values
        assert "urn:li:tag:added_by_ryan" in tag_values
        assert len(tag_values) == 2

    def test_skip_version_tag_default_value(self):
        """Test that skip_version_tag defaults to False."""
        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="amount",
            field_type="number",
            field_description=None,
            deployment_initiator=None,
            deployment_timestamp=None,
            pii_fields=set(),
            # skip_version_tag not specified - should default to False
        )

        assert context.skip_version_tag is False

    def test_version_tag_with_skip_flag_false(self):
        """Test that version tag is included when skip_version_tag is explicitly False."""
        config = FieldTaggingConfig(
            use_structured_properties=False,  # Test tags only
            tag_schema_version=True,
            tag_event_type=False,
            tag_data_class=False,
            tag_authorship=False,
        )
        tagger = FieldTagger(config)

        context = FieldTagContext(
            schema_version="2-0-0",
            vendor="com.acme",
            name="product_viewed",
            field_name="new_field",
            field_type="string",
            field_description=None,
            deployment_initiator=None,
            deployment_timestamp=None,
            pii_fields=set(),
            skip_version_tag=False,
        )

        tags = tagger.generate_tags(context)

        assert tags is not None
        tag_values = [t.tag for t in tags.tags]

        # Should have version tag
        assert "urn:li:tag:snowplow_schema_v2-0-0" in tag_values
        assert len(tag_values) == 1


class TestFieldTagContext:
    """Test the FieldTagContext dataclass."""

    def test_field_tag_context_creation(self):
        """Test creating a FieldTagContext object."""
        context = FieldTagContext(
            schema_version="1-0-0",
            vendor="com.acme",
            name="checkout_started",
            field_name="user_id",
            field_type="string",
            field_description="User identifier",
            deployment_initiator="ryan@company.com",
            deployment_timestamp=None,
            pii_fields={"user_id"},
        )

        assert context.schema_version == "1-0-0"
        assert context.vendor == "com.acme"
        assert context.name == "checkout_started"
        assert context.field_name == "user_id"
        assert context.field_type == "string"
        assert context.field_description == "User identifier"
        assert context.deployment_initiator == "ryan@company.com"
        assert context.pii_fields == {"user_id"}
