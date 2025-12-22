"""Unit tests for LineageBuilder."""

from datahub.ingestion.source.snowplow.builders.lineage_builder import LineageBuilder
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)


class TestLineageBuilder:
    """Test LineageBuilder enrichment description building."""

    def test_build_enrichment_description_no_lineages(self):
        """Test building description with no lineages."""
        builder = LineageBuilder()

        result = builder.build_enrichment_description("ip_lookups", [])

        assert result == "ip_lookups enrichment"

    def test_build_enrichment_description_with_downstream_fields(self):
        """Test building description with downstream fields."""
        builder = LineageBuilder()

        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_country)",
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_city)",
            ],
        )

        result = builder.build_enrichment_description("ip_lookups", [lineage])

        assert "## ip_lookups enrichment" in result
        assert "**Adds fields:**" in result
        assert "- `geo_city`" in result
        assert "- `geo_country`" in result

    def test_build_enrichment_description_with_upstream_fields(self):
        """Test building description with upstream fields."""
        builder = LineageBuilder()

        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.atomic,PROD),user_ipaddress)"
            ],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_country)"
            ],
        )

        result = builder.build_enrichment_description("ip_lookups", [lineage])

        assert "## ip_lookups enrichment" in result
        assert "**From source fields:**" in result
        assert "- `user_ipaddress`" in result
        assert "**Adds fields:**" in result
        assert "- `geo_country`" in result

    def test_build_enrichment_description_multiple_lineages(self):
        """Test building description with multiple lineage entries."""
        builder = LineageBuilder()

        lineage1 = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.atomic,PROD),user_ipaddress)"
            ],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_country)"
            ],
        )

        lineage2 = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.atomic,PROD),user_ipaddress)"
            ],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_city)"
            ],
        )

        result = builder.build_enrichment_description(
            "ip_lookups", [lineage1, lineage2]
        )

        # Should aggregate all fields
        assert "- `geo_country`" in result
        assert "- `geo_city`" in result
        assert "- `user_ipaddress`" in result

    def test_build_enrichment_description_deduplicates_fields(self):
        """Test that duplicate fields are deduplicated."""
        builder = LineageBuilder()

        # Same downstream field appears twice
        lineage1 = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_country)"
            ],
        )

        lineage2 = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),geo_country)"
            ],
        )

        result = builder.build_enrichment_description(
            "ip_lookups", [lineage1, lineage2]
        )

        # Should only appear once
        assert result.count("- `geo_country`") == 1

    def test_build_enrichment_description_sorts_fields(self):
        """Test that fields are sorted alphabetically."""
        builder = LineageBuilder()

        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),zebra_field)",
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),alpha_field)",
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),middle_field)",
            ],
        )

        result = builder.build_enrichment_description("test", [lineage])

        # Find positions of fields in result
        alpha_pos = result.index("alpha_field")
        middle_pos = result.index("middle_field")
        zebra_pos = result.index("zebra_field")

        # Should be in alphabetical order
        assert alpha_pos < middle_pos < zebra_pos

    def test_build_enrichment_description_handles_none_upstreams_downstreams(self):
        """Test handling of None upstreams/downstreams."""
        builder = LineageBuilder()

        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=None,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=None,
        )

        result = builder.build_enrichment_description("test", [lineage])

        # Should not crash, just return basic description
        assert "## test enrichment" in result

    def test_build_enrichment_description_urn_parsing_edge_cases(self):
        """Test URN parsing with various formats."""
        builder = LineageBuilder()

        # URN without comma (shouldn't be extracted)
        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:invalid_format_no_comma",
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),valid_field)",
            ],
        )

        result = builder.build_enrichment_description("test", [lineage])

        # Should only extract valid field
        assert "- `valid_field`" in result
        assert "invalid_format_no_comma" not in result

    def test_build_enrichment_description_markdown_formatting(self):
        """Test markdown formatting of output."""
        builder = LineageBuilder()

        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.atomic,PROD),source_field)"
            ],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD),output_field)"
            ],
        )

        result = builder.build_enrichment_description("test_enrichment", [lineage])

        # Verify markdown structure
        assert result.startswith("## test_enrichment enrichment")
        assert "**Adds fields:**" in result
        assert "**From source fields:**" in result
        assert "- `output_field`" in result
        assert "- `source_field`" in result
        # Should have newlines between sections
        lines = result.split("\n")
        assert "" in lines  # Empty line for spacing
