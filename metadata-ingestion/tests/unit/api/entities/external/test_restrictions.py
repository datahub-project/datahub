"""
Tests for ExternalTag and UnityCatalogTag classes.
"""

from datahub.api.entities.external.external_tag import (
    ExternalTag,
)
from datahub.api.entities.external.lake_formation_external_entites import (
    LakeFormationTag,
    LakeFormationTagKeyText,
    LakeFormationTagValueText,
)
from datahub.api.entities.external.restricted_text import RestrictedText
from datahub.api.entities.external.unity_catalog_external_entites import (
    UnityCatalogTag,
    UnityCatalogTagKeyText,
    UnityCatalogTagValueText,
)
from datahub.metadata.urns import TagUrn


class TestRestrictedText:
    """Tests for RestrictedText base functionality."""

    def test_basic_functionality(self):
        """Test basic RestrictedText creation and processing."""
        text = RestrictedText(text="Hello World! This is a test.")
        assert str(text) == "Hello_World!_This_is_a_test."
        assert text.text == "Hello World! This is a test."
        assert text.processed == "Hello_World!_This_is_a_test."

    def test_truncation(self):
        """Test text truncation with default settings."""
        long_text = "A" * 60  # Longer than default 50 chars
        text = RestrictedText(text=long_text)
        assert len(str(text)) == 50
        assert str(text).endswith("...")
        assert text.text == long_text

    def test_custom_configuration(self):
        """Test RestrictedText with custom configuration."""
        text = RestrictedText(
            text="hello-world.test",
            max_length=10,
            forbidden_chars={"-", "."},
            replacement_char="_",
            truncation_suffix="...",
        )
        assert str(text) == "hello_w..."
        assert text.text == "hello-world.test"


class TestExternalTag:
    """Tests for ExternalTag class."""

    def test_from_urn_with_value(self):
        """Test creating ExternalTag from URN with key:value."""
        tag = ExternalTag.from_urn("urn:li:tag:environment:production")
        assert tag.key.text == "environment"
        assert tag.value is not None
        assert tag.value.text == "production"
        assert str(tag.key) == "environment"
        assert str(tag.value) == "production"

    def test_from_urn_key_only(self):
        """Test creating ExternalTag from URN with key only."""
        tag = ExternalTag.from_urn("urn:li:tag:critical")
        assert tag.key.text == "critical"
        assert tag.value is None
        assert str(tag.key) == "critical"

    def test_from_urn_multiple_colons(self):
        """Test URN parsing with multiple colons (only splits on first)."""
        tag = ExternalTag.from_urn("urn:li:tag:database:mysql:version:8.0")
        assert tag.key.text == "database"
        assert tag.value is not None
        assert tag.value.text == "mysql:version:8.0"

    def test_from_key_value(self):
        """Test creating ExternalTag from explicit key/value."""
        tag = ExternalTag.from_key_value("team", "data-engineering")
        assert tag.key.text == "team"
        assert tag.value is not None
        assert tag.value.text == "data-engineering"

    def test_from_key_only(self):
        """Test creating ExternalTag from key only."""
        tag = ExternalTag.from_key_value("critical")
        assert tag.key.text == "critical"
        assert tag.value is None

    def test_to_datahub_tag_urn_with_value(self):
        """Test generating DataHub URN with value."""
        tag = ExternalTag.from_key_value("environment", "production")
        urn = tag.to_datahub_tag_urn()
        assert str(urn) == "urn:li:tag:environment:production"

    def test_to_datahub_tag_urn_key_only(self):
        """Test generating DataHub URN with key only."""
        tag = ExternalTag.from_key_value("critical")
        urn = tag.to_datahub_tag_urn()
        assert str(urn) == "urn:li:tag:critical"

    def test_string_representation(self):
        """Test string representation of ExternalTag."""
        tag_with_value = ExternalTag.from_key_value("env", "prod")
        tag_key_only = ExternalTag.from_key_value("critical")

        assert str(tag_with_value) == "env:prod"
        assert str(tag_key_only) == "critical"

    def test_repr(self):
        """Test repr representation of ExternalTag."""
        tag = ExternalTag.from_key_value("env", "prod")
        repr_str = repr(tag)
        assert "ExternalTag" in repr_str
        assert "env" in repr_str
        assert "prod" in repr_str

    def test_restricted_text_processing(self):
        """Test that RestrictedText processing works in ExternalTag."""
        tag = ExternalTag.from_key_value(
            "very long key with spaces and special chars!",
            "very long value with \n newlines and \t tabs",
        )

        # Keys and values should be processed by RestrictedText
        assert " " not in str(tag.key)  # Spaces replaced
        assert tag.value is not None
        assert "\n" not in str(tag.value)  # Newlines replaced
        assert "\t" not in str(tag.value)  # Tabs replaced

        # But originals should be preserved
        assert " " in tag.key.text
        assert "!" in tag.key.text
        assert "\n" in tag.value.text
        assert "\t" in tag.value.text

    def test_get_datahub_tag_fallback(self):
        """Test get_datahub_tag fallback when DataHub is not available."""
        tag = ExternalTag.from_key_value("environment", "production")

        # Since DataHub is not available in test environment, should return string
        result = tag.to_datahub_tag_urn()
        assert isinstance(result, TagUrn)
        assert str(result) == "urn:li:tag:environment:production"

    def test_parse_tag_name_static_method(self):
        """Test the static _parse_tag_name method."""
        # With colon
        key, value = ExternalTag._parse_tag_name("environment:production")
        assert key == "environment"
        assert value == "production"

        # Without colon
        key, value = ExternalTag._parse_tag_name("critical")
        assert key == "critical"
        assert value is None

        # Multiple colons (only split on first)
        key, value = ExternalTag._parse_tag_name("db:mysql:version:8.0")
        assert key == "db"
        assert value == "mysql:version:8.0"


class TestLakeFormationTagKeyText:
    """Tests for LakeFormationTagKeyText."""

    def test_key_restrictions(self):
        """Test Lake Formation key restrictions."""
        key_text = LakeFormationTagKeyText(text="data-source with spaces!")

        # Should replace spaces and other characters
        processed = str(key_text)
        assert " " not in processed
        assert "_" in processed  # Replacement character

        # Should preserve original
        assert key_text.text == "data-source with spaces!"

    def test_key_length_limit(self):
        """Test Lake Formation key length limit (50 chars)."""
        long_key = "a" * 60  # Longer than 50 chars
        key_text = LakeFormationTagKeyText(text=long_key)

        assert len(str(key_text)) <= 50
        assert key_text.text == long_key

    def test_valid_key_characters(self):
        """Test that valid characters are preserved."""
        valid_key = "environment_prod_v1_2"
        key_text = LakeFormationTagKeyText(text=valid_key)

        # These should be preserved (valid characters)
        assert str(key_text) == valid_key

    def test_no_truncation_suffix(self):
        """Test that Lake Formation keys don't use truncation suffix."""
        long_key = "a" * 60
        key_text = LakeFormationTagKeyText(text=long_key)
        processed = str(key_text)

        # Should not end with dots since DEFAULT_TRUNCATION_SUFFIX is ""
        assert not processed.endswith("...")
        assert len(processed) == 50


class TestLakeFormationTagValueText:
    """Tests for LakeFormationTagValueText."""

    def test_value_restrictions(self):
        """Test Lake Formation value restrictions."""
        value_text = LakeFormationTagValueText(
            text="Database Instance\nWith control chars\t"
        )

        # Should replace control characters with spaces
        processed = str(value_text)
        assert "\n" not in processed
        assert "\t" not in processed
        assert " " in processed  # Replacement character

        # Should preserve original
        assert "\n" in value_text.text
        assert "\t" in value_text.text

    def test_value_length_limit(self):
        """Test Lake Formation value length limit (50 chars)."""
        long_value = "a" * 60  # Longer than 50 chars
        value_text = LakeFormationTagValueText(text=long_value)

        assert len(str(value_text)) <= 50
        assert str(value_text).endswith("...")
        assert value_text.text == long_value

    def test_permissive_characters(self):
        """Test that most characters are allowed in values."""
        complex_value = "MySQL: 8.0 (Primary) - Special chars: @#$%^&*"
        value_text = LakeFormationTagValueText(text=complex_value)

        # Most characters should be preserved (more permissive than keys)
        processed = str(value_text)
        assert ":" in processed
        assert "(" in processed
        assert "@" in processed
        assert "#" in processed


class TestLakeFormationTag:
    """Tests for LakeFormationTag class."""

    def test_from_key_value(self):
        """Test creating LakeFormationTag from key/value."""
        tag = LakeFormationTag.from_key_value("environment", "production")
        assert tag.key.text == "environment"
        assert tag.value is not None
        assert tag.value.text == "production"

    def test_from_dict(self):
        """Test creating LakeFormationTag from dictionary."""
        tag_dict = {"key": "team owner", "value": "data engineering"}
        tag = LakeFormationTag.from_dict(tag_dict)

        assert tag.key.text == "team owner"
        assert tag.value is not None
        assert tag.value.text == "data engineering"

    def test_to_dict(self):
        """Test converting LakeFormationTag to dictionary."""
        tag = LakeFormationTag.from_key_value("environment", "production")
        result = tag.to_dict()

        expected = {"key": "environment", "value": "production"}
        assert result == expected

    def test_to_display_dict(self):
        """Test converting LakeFormationTag to display dictionary."""
        tag = LakeFormationTag.from_key_value("data source type", "MySQL: 8.0")
        result = tag.to_display_dict()

        # Should show processed values
        assert result["key"] != "data source type"  # Should be processed
        assert " " not in result["key"]  # Spaces replaced
        assert "_" in result["key"]  # Replacement character

    def test_key_only_tag(self):
        """Test LakeFormationTag with key only."""
        tag = LakeFormationTag.from_key_value("critical")
        assert tag.key.text == "critical"
        assert tag.value is None

        result = tag.to_dict()
        expected = {"key": "critical"}
        assert result == expected

    def test_direct_initialization(self):
        """Test direct initialization with strings (uses validators)."""
        tag = LakeFormationTag(key="environment", value="production")
        assert tag.key.text == "environment"
        assert tag.value is not None
        assert tag.value.text == "production"

    def test_direct_initialization_with_objects(self):
        """Test direct initialization with RestrictedText objects."""
        key_obj = LakeFormationTagKeyText(text="team")
        value_obj = LakeFormationTagValueText(text="engineering")
        tag = LakeFormationTag(key=key_obj, value=value_obj)

        assert tag.key.text == "team"
        assert tag.value is not None
        assert tag.value.text == "engineering"

    def test_truncation_detection(self):
        """Test truncation detection properties."""
        # Long key (over 50 chars)
        long_key = "a" * 60
        tag1 = LakeFormationTag.from_key_value(long_key, "short_value")
        assert tag1.key.text == long_key  # Original preserved
        assert len(str(tag1.key)) == 50  # Processed truncated

        # Long value (over 50 chars)
        long_value = "b" * 60
        tag2 = LakeFormationTag.from_key_value("short_key", long_value)
        assert tag2.value is not None
        assert tag2.value.text == long_value  # Original preserved
        assert len(str(tag2.value)) == 50  # Processed truncated

        # No truncation
        tag3 = LakeFormationTag.from_key_value("short", "short")
        assert tag3.value is not None
        assert str(tag3.value) == "short"
        assert str(tag3.key) == "short"

    def test_string_representation(self):
        """Test string representation of LakeFormationTag."""
        tag_with_value = LakeFormationTag.from_key_value("env", "prod")
        tag_key_only = LakeFormationTag.from_key_value("critical")

        assert str(tag_with_value) == "env:prod"
        assert str(tag_key_only) == "critical"

    def test_character_sanitization(self):
        """Test that invalid characters are properly sanitized."""
        # Test key sanitization (spaces replaced with underscores)
        tag = LakeFormationTag.from_key_value("data source main", "value")
        processed_key = str(tag.key)
        assert " " not in processed_key
        assert "_" in processed_key  # Replacement char

        # Test value sanitization (control chars replaced with spaces)
        tag2 = LakeFormationTag.from_key_value("key", "line1\nline2\tcolumn")
        assert tag2.value is not None
        processed_value = str(tag2.value)
        assert "\n" not in processed_value
        assert "\t" not in processed_value
        assert " " in processed_value  # Replacement char

    def test_api_compatibility(self):
        """Test compatibility with Lake Formation API format."""
        # Simulate API response format
        api_data = {"key": "data source type", "value": "PostgreSQL DB"}
        tag = LakeFormationTag.from_dict(api_data)

        # Should be able to convert back to API format
        api_output = tag.to_dict()
        assert api_output["key"] == "data source type"  # Original preserved
        assert api_output["value"] == "PostgreSQL DB"  # Original preserved

        # Display format should show processed values
        display_output = tag.to_display_dict()
        assert " " not in display_output["key"]  # Should be sanitized

    def test_empty_value_handling(self):
        """Test that empty values are handled correctly."""
        # Empty string value should become None
        tag = LakeFormationTag.from_key_value("key", "")
        assert tag.value is None

        result = tag.to_dict()
        assert "value" not in result or result.get("value") is None

    def test_equality_and_hashing(self):
        """Test equality and hashing of LakeFormationTag objects."""
        tag1 = LakeFormationTag.from_key_value("environment", "production")
        tag2 = LakeFormationTag.from_key_value("environment", "production")
        tag3 = LakeFormationTag.from_key_value("environment", "staging")

        # Same tags should be equal
        assert tag1 == tag2
        assert hash(tag1) == hash(tag2)

        # Different tags should not be equal
        assert tag1 != tag3
        assert hash(tag1) != hash(tag3)

    def test_repr(self):
        """Test repr representation of LakeFormationTag."""
        tag = LakeFormationTag.from_key_value("env", "prod")
        repr_str = repr(tag)
        assert "LakeFormationTag" in repr_str
        assert "env" in repr_str
        assert "prod" in repr_str


class TestUnityCatalogTagKeyText:
    """Tests for UnityCatalogTagKeyText."""

    def test_key_restrictions(self):
        """Test Unity Catalog key restrictions."""
        key_text = UnityCatalogTagKeyText(text="data-source/type@main!")

        # Should replace invalid characters
        processed = str(key_text)
        assert "/" not in processed
        assert "=" not in processed
        assert "@" in processed  # Replacement character

        # Should preserve original
        assert key_text.text == "data-source/type@main!"

    def test_key_length_limit(self):
        """Test Unity Catalog key length limit (127 chars)."""
        long_key = "a" * 260  # Longer than 127 chars
        key_text = UnityCatalogTagKeyText(text=long_key)

        assert len(str(key_text)) <= 255
        assert key_text.text == long_key

    def test_valid_key_characters(self):
        """Test that valid characters are preserved."""
        valid_key = "environment_prod_v1_2"
        key_text = UnityCatalogTagKeyText(text=valid_key)

        # These should be preserved (valid UC characters)
        assert str(key_text) == valid_key


class TestUnityCatalogTagValueText:
    """Tests for UnityCatalogTagValueText."""

    def test_value_restrictions(self):
        """Test Unity Catalog value restrictions."""
        value_text = UnityCatalogTagValueText(
            text="MySQL Database: 8.0 (Primary)\nProduction Instance"
        )

        # Should replace control characters
        processed = str(value_text)
        assert "\n" not in processed

        # Should preserve original
        assert "\n" in value_text.text

    def test_value_length_limit(self):
        """Test Unity Catalog value length limit (1000 chars)."""
        long_value = "a" * 1010  # Longer than 1000 chars
        value_text = UnityCatalogTagValueText(text=long_value)

        assert len(str(value_text)) <= 1000
        assert str(value_text).endswith("...")
        assert value_text.text == long_value

    def test_permissive_characters(self):
        """Test that most characters are allowed in values."""
        complex_value = "MySQL: 8.0 (Primary) - Special chars: @#$%^&*"
        value_text = UnityCatalogTagValueText(text=complex_value)

        # Most characters should be preserved (more permissive than keys)
        processed = str(value_text)
        assert ":" in processed
        assert "(" in processed
        assert "@" in processed
        assert "#" in processed


class TestUnityCatalogTag:
    """Tests for UnityCatalogTag class."""

    def test_from_key_value(self):
        """Test creating UnityCatalogTag from key/value."""
        tag = UnityCatalogTag.from_key_value("environment", "production")
        assert tag.key.text == "environment"
        assert tag.value is not None
        assert tag.value.text == "production"

    def test_from_dict(self):
        """Test creating UnityCatalogTag from dictionary."""
        tag_dict = {"key": "team/owner", "value": "data-engineering@company.com"}
        tag = UnityCatalogTag.from_dict(tag_dict)

        assert tag.key.text == "team/owner"
        assert tag.value is not None
        assert tag.value.text == "data-engineering@company.com"

    def test_to_dict(self):
        """Test converting UnityCatalogTag to dictionary."""
        tag = UnityCatalogTag.from_key_value("environment", "production")
        result = tag.to_dict()

        expected = {"key": "environment", "value": "production"}
        assert result == expected

    def test_to_display_dict(self):
        """Test converting UnityCatalogTag to display dictionary."""
        tag = UnityCatalogTag.from_key_value("data-source/type!", "MySQL: 8.0")
        result = tag.to_display_dict()

        # Should show processed values
        assert result["key"] != "data-source/type!"  # Should be processed
        assert "/" not in result["key"]  # Invalid chars replaced
        assert "!" in result["key"]  # Invalid chars replaced

    def test_key_only_tag(self):
        """Test UnityCatalogTag with key only."""
        tag = UnityCatalogTag.from_key_value("critical")
        assert tag.key.text == "critical"
        assert tag.value is None

        result = tag.to_dict()
        expected = {"key": "critical"}
        assert result == expected

    def test_truncation_detection(self):
        """Test truncation detection properties."""
        # Long key (over 127 chars)
        long_key = "a" * 256
        tag1 = UnityCatalogTag.from_key_value(long_key, "short_value")
        assert tag1.key != long_key
        assert len(str(tag1.key)) == 255

        # Long value (over 256 chars)
        long_value = "b" * 1001
        tag2 = UnityCatalogTag.from_key_value("short_key", long_value)
        assert tag2.value is not None
        assert tag2.value != long_value
        assert len(str(tag2.value)) == 1000

        # No truncation
        tag3 = UnityCatalogTag.from_key_value("short", "short")
        assert tag3.value is not None
        assert str(tag3.value) == "short"
        assert str(tag3.key) == "short"

    def test_string_representation(self):
        """Test string representation of UnityCatalogTag."""
        tag_with_value = UnityCatalogTag.from_key_value("env", "prod")
        tag_key_only = UnityCatalogTag.from_key_value("critical")

        assert str(tag_with_value) == "env:prod"
        assert str(tag_key_only) == "critical"

    def test_character_sanitization(self):
        """Test that invalid characters are properly sanitized."""
        # Test key sanitization
        tag = UnityCatalogTag.from_key_value("data/source@main!", "value")
        processed_key = str(tag.key)
        assert "/" not in processed_key
        assert "@" in processed_key
        assert "!" in processed_key
        assert "_" in processed_key  # Replacement char

        # Test value sanitization
        tag2 = UnityCatalogTag.from_key_value("key", "line1\nline2\tcolumn")
        assert tag2.value is not None
        processed_value = str(tag2.value)
        assert "\n" not in processed_value
        assert "\t" not in processed_value

    def test_api_compatibility(self):
        """Test compatibility with Unity Catalog API format."""
        # Simulate API response format
        api_data = {"key": "data-source/type", "value": "PostgreSQL DB"}
        tag = UnityCatalogTag.from_dict(api_data)

        # Should be able to convert back to API format
        api_output = tag.to_dict()
        assert api_output["key"] == "data-source/type"  # Original preserved
        assert api_output["value"] == "PostgreSQL DB"  # Original preserved

        # Display format should show processed values
        display_output = tag.to_display_dict()
        assert "/" not in display_output["key"]  # Should be sanitized


class TestIntegration:
    """Integration tests for the tag classes."""

    def test_external_tag_to_unity_catalog_conversion(self):
        """Test converting ExternalTag concept to UnityCatalogTag."""
        # Create an ExternalTag
        external_tag = ExternalTag.from_key_value("data-source/type!", "MySQL: 8.0")

        # Convert to UnityCatalogTag
        uc_tag = UnityCatalogTag.from_key_value(
            external_tag.key.text,
            external_tag.value.text if external_tag.value is not None else None,
        )

        # Should have same original values
        assert uc_tag.key.text == external_tag.key.text
        if external_tag.value is not None and uc_tag.value is not None:
            assert uc_tag.value.text == external_tag.value.text

        # But different processing rules
        assert str(uc_tag.key) != str(external_tag.key)  # Different sanitization

    def test_round_trip_urn_parsing(self):
        """Test round-trip URN parsing and generation."""
        original_urn = "urn:li:tag:environment:production"

        # Parse URN to ExternalTag
        tag = ExternalTag.from_urn(original_urn)

        # Generate URN back
        generated_urn = tag.to_datahub_tag_urn()

        # Should be identical
        assert str(generated_urn) == original_urn

    def test_complex_tag_scenarios(self):
        """Test complex real-world tag scenarios."""
        test_cases = [
            # Simple tags
            ("environment", "production"),
            ("critical", None),
            # Tags with special characters
            ("data-source", "postgresql://user:pass@host:5432/db"),
            ("team/project", "data-eng/analytics-v2"),
            # Tags with multiple colons
            ("version", "app:v1.2.3:stable"),
            # Very long tags
            ("description", "This is a very long description " * 10),
        ]

        for key, value in test_cases:
            # Test ExternalTag
            ext_tag = ExternalTag.from_key_value(key, value)
            assert ext_tag.key.text == key
            if value:
                assert ext_tag.value is not None
                assert ext_tag.value.text == value

            # Test round-trip through URN
            urn = ext_tag.to_datahub_tag_urn()
            parsed_tag = ExternalTag.from_urn(urn)
            assert parsed_tag.key.text == key
            if value:
                assert parsed_tag.value is not None
                assert parsed_tag.value.text == value

            # Test UnityCatalogTag
            uc_tag = UnityCatalogTag.from_key_value(key, value)
            assert uc_tag.key.text == key
            if value:
                assert uc_tag.value is not None
                assert uc_tag.value.text == value
