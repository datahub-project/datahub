"""Unit tests for LineageBuilder."""

from unittest.mock import MagicMock

from datahub.ingestion.source.snowplow.builders.lineage_builder import LineageBuilder
from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentFieldInfo,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment


def _create_mock_enrichment(name: str = "test_enrichment") -> Enrichment:
    """Create a mock Enrichment object for testing."""
    enrichment = MagicMock(spec=Enrichment)
    enrichment.filename = f"{name}.json"
    # Create nested content.data.name structure
    enrichment.content = MagicMock()
    enrichment.content.data = MagicMock()
    enrichment.content.data.name = name
    return enrichment


def _create_mock_registry(field_info: EnrichmentFieldInfo = None) -> MagicMock:
    """Create a mock EnrichmentLineageRegistry that returns specified field info."""
    registry = MagicMock()
    registry.get_field_info.return_value = field_info
    return registry


class TestLineageBuilder:
    """Test LineageBuilder enrichment description building."""

    def test_build_enrichment_description_no_field_info(self):
        """Test building description when no field info is available."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("ip_lookups")
        registry = _create_mock_registry(field_info=None)

        result = builder.build_enrichment_description(enrichment, registry)

        assert result == "ip_lookups enrichment"

    def test_build_enrichment_description_empty_field_info(self):
        """Test building description with empty field info."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("ip_lookups")
        field_info = EnrichmentFieldInfo(input_fields=[], output_fields=[])
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        assert result == "ip_lookups enrichment"

    def test_build_enrichment_description_with_output_fields(self):
        """Test building description with output fields."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("ip_lookups")
        field_info = EnrichmentFieldInfo(
            input_fields=[],
            output_fields=["geo_country", "geo_city"],
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        assert "## ip_lookups enrichment" in result
        assert "**Adds fields:**" in result
        assert "- `geo_city`" in result
        assert "- `geo_country`" in result

    def test_build_enrichment_description_with_input_fields(self):
        """Test building description with input fields."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("ip_lookups")
        field_info = EnrichmentFieldInfo(
            input_fields=["user_ipaddress"],
            output_fields=["geo_country"],
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        assert "## ip_lookups enrichment" in result
        assert "**From source fields:**" in result
        assert "- `user_ipaddress`" in result
        assert "**Adds fields:**" in result
        assert "- `geo_country`" in result

    def test_build_enrichment_description_with_transformation_description(self):
        """Test building description with transformation description."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("ip_lookups")
        field_info = EnrichmentFieldInfo(
            input_fields=["user_ipaddress"],
            output_fields=["geo_country", "geo_city"],
            transformation_description="IP geolocation lookup via MaxMind",
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        assert "## ip_lookups enrichment" in result
        assert "*IP geolocation lookup via MaxMind*" in result
        assert "**Adds fields:**" in result
        assert "- `geo_city`" in result
        assert "- `geo_country`" in result

    def test_build_enrichment_description_deduplicates_fields(self):
        """Test that duplicate fields are deduplicated."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("ip_lookups")
        # Same field appears twice
        field_info = EnrichmentFieldInfo(
            input_fields=[],
            output_fields=["geo_country", "geo_country", "geo_city"],
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        # Should only appear once
        assert result.count("- `geo_country`") == 1

    def test_build_enrichment_description_sorts_fields(self):
        """Test that fields are sorted alphabetically."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("test")
        field_info = EnrichmentFieldInfo(
            input_fields=[],
            output_fields=["zebra_field", "alpha_field", "middle_field"],
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        # Find positions of fields in result
        alpha_pos = result.index("alpha_field")
        middle_pos = result.index("middle_field")
        zebra_pos = result.index("zebra_field")

        # Should be in alphabetical order
        assert alpha_pos < middle_pos < zebra_pos

    def test_build_enrichment_description_uses_filename_if_no_content(self):
        """Test that filename is used if content is not available."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("test")
        enrichment.content = None  # No content available
        enrichment.filename = "my_enrichment.json"
        field_info = EnrichmentFieldInfo(
            input_fields=["source"],
            output_fields=["output"],
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        assert "## my_enrichment.json enrichment" in result

    def test_build_enrichment_description_markdown_formatting(self):
        """Test markdown formatting of output."""
        builder = LineageBuilder()
        enrichment = _create_mock_enrichment("test_enrichment")
        field_info = EnrichmentFieldInfo(
            input_fields=["source_field"],
            output_fields=["output_field"],
        )
        registry = _create_mock_registry(field_info=field_info)

        result = builder.build_enrichment_description(enrichment, registry)

        # Verify markdown structure
        assert result.startswith("## test_enrichment enrichment")
        assert "**Adds fields:**" in result
        assert "**From source fields:**" in result
        assert "- `output_field`" in result
        assert "- `source_field`" in result
        # Should have newlines between sections
        lines = result.split("\n")
        assert "" in lines  # Empty line for spacing
