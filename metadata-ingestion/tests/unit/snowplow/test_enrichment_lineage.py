"""
Unit tests for Snowplow enrichment field-level lineage extraction.

Tests the enrichment lineage infrastructure including:
- Base classes and registry
- Individual enrichment extractors
- Field lineage generation
"""

import pytest

from datahub.ingestion.source.snowplow.enrichment_lineage import (
    CurrencyConversionLineageExtractor,
    EnrichmentLineageRegistry,
    FieldLineage,
    IpLookupLineageExtractor,
    RefererParserLineageExtractor,
    UaParserLineageExtractor,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.utils import (
    camel_to_snake,
    make_field_urn,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    Enrichment,
    EnrichmentContent,
    EnrichmentContentData,
)


class TestEnrichmentLineageUtils:
    """Test utility functions for enrichment lineage."""

    def test_camel_to_snake(self):
        """Test camelCase to snake_case conversion."""
        assert camel_to_snake("mktMedium") == "mkt_medium"
        assert camel_to_snake("deviceClass") == "device_class"
        assert camel_to_snake("IPAddress") == "ip_address"
        assert camel_to_snake("simpleword") == "simpleword"
        assert camel_to_snake("HTTPRequest") == "http_request"

    def test_make_field_urn(self):
        """Test field URN construction."""
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started.1-0-0,PROD)"
        field_name = "user_ipaddress"

        expected = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started.1-0-0,PROD),user_ipaddress)"
        assert make_field_urn(dataset_urn, field_name) == expected

    def test_make_field_urn_validation(self):
        """Test field URN construction requires valid dataset URN."""
        dataset_urn = "custom_urn"  # Invalid URN format
        field_name = "geo_country"

        # Should raise AssertionError for invalid URN format
        with pytest.raises(
            AssertionError, match="Schema field's parent must be an urn"
        ):
            make_field_urn(dataset_urn, field_name)


class TestEnrichmentLineageRegistry:
    """Test the enrichment lineage registry."""

    def test_registry_initialization(self):
        """Test registry can be initialized."""
        registry = EnrichmentLineageRegistry()
        assert registry.get_extractor_count() == 0

    def test_register_extractor(self):
        """Test registering an extractor."""
        registry = EnrichmentLineageRegistry()
        extractor = IpLookupLineageExtractor()

        registry.register(extractor)
        assert registry.get_extractor_count() == 1

    def test_register_multiple_extractors(self):
        """Test registering multiple extractors."""
        registry = EnrichmentLineageRegistry()

        registry.register(IpLookupLineageExtractor())
        registry.register(RefererParserLineageExtractor())
        registry.register(UaParserLineageExtractor())
        registry.register(CurrencyConversionLineageExtractor())

        assert registry.get_extractor_count() == 4

    def test_get_extractor_by_schema(self):
        """Test looking up extractor by enrichment schema."""
        registry = EnrichmentLineageRegistry()
        registry.register(IpLookupLineageExtractor())

        # Create mock enrichment
        enrichment = Enrichment(
            id="test-id",
            filename="ip_lookups.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="IP Lookup",
                    vendor="com.snowplowanalytics.snowplow",
                    parameters={},
                ),
            ),
        )

        extractor = registry.get_extractor(enrichment)
        assert extractor is not None
        assert isinstance(extractor, IpLookupLineageExtractor)

    def test_get_extractor_no_match(self):
        """Test looking up extractor with no match returns None."""
        registry = EnrichmentLineageRegistry()
        registry.register(IpLookupLineageExtractor())

        # Create enrichment with unknown schema
        enrichment = Enrichment(
            id="test-id",
            filename="unknown.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.example/unknown_enrichment/jsonschema/1-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="Unknown",
                    vendor="com.example",
                    parameters={},
                ),
            ),
        )

        extractor = registry.get_extractor(enrichment)
        assert extractor is None


class TestIpLookupLineageExtractor:
    """Test IP Lookup enrichment lineage extractor."""

    def test_supports_enrichment(self):
        """Test that extractor recognizes IP lookup schema."""
        extractor = IpLookupLineageExtractor()

        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0"
        )
        assert not extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0"
        )

    def test_extract_lineage_with_geo_database(self):
        """Test lineage extraction with geo database configured."""
        extractor = IpLookupLineageExtractor()

        enrichment = Enrichment(
            id="test-id",
            filename="ip_lookups.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="IP Lookup",
                    vendor="com.snowplowanalytics.snowplow",
                    parameters={"geo": {"database": "GeoLite2-City.mmdb"}},
                ),
            ),
        )

        event_schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event.1-0-0,PROD)"
        )
        warehouse_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD)"
        )

        lineages = extractor.extract_lineage(
            enrichment, event_schema_urn, warehouse_table_urn
        )

        # Should have 8 geo fields
        assert len(lineages) == 8

        # Check structure of first lineage
        assert lineages[0].upstream_fields == [
            make_field_urn(event_schema_urn, "user_ipaddress")
        ]
        assert lineages[0].downstream_fields == [
            make_field_urn(warehouse_table_urn, "geo_country")
        ]
        assert lineages[0].transformation_type == "DERIVED"

    def test_extract_lineage_no_warehouse(self):
        """Test lineage extraction returns empty list if no warehouse."""
        extractor = IpLookupLineageExtractor()

        enrichment = Enrichment(
            id="test-id",
            filename="ip_lookups.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="IP Lookup",
                    vendor="com.snowplowanalytics.snowplow",
                    parameters={"geo": {"database": "GeoLite2-City.mmdb"}},
                ),
            ),
        )

        event_schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event.1-0-0,PROD)"
        )

        lineages = extractor.extract_lineage(enrichment, event_schema_urn, None)

        assert len(lineages) == 0


class TestRefererParserLineageExtractor:
    """Test Referer Parser enrichment lineage extractor."""

    def test_supports_enrichment(self):
        """Test that extractor recognizes referer parser schema."""
        extractor = RefererParserLineageExtractor()

        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0"
        )
        assert not extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0"
        )

    def test_extract_lineage(self):
        """Test lineage extraction for referer parser."""
        extractor = RefererParserLineageExtractor()

        enrichment = Enrichment(
            id="test-id",
            filename="referer_parser.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="Referer Parser",
                    vendor="com.snowplowanalytics.snowplow",
                    parameters={},
                ),
            ),
        )

        event_schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event.1-0-0,PROD)"
        )
        warehouse_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD)"
        )

        lineages = extractor.extract_lineage(
            enrichment, event_schema_urn, warehouse_table_urn
        )

        # Should have 3 output fields
        assert len(lineages) == 3

        # Check all lineages have correct input
        for lineage in lineages:
            assert lineage.upstream_fields == [
                make_field_urn(event_schema_urn, "page_referrer")
            ]
            assert lineage.transformation_type == "DERIVED"

        # Check output fields
        output_fields = [lineage.downstream_fields[0] for lineage in lineages]
        assert make_field_urn(warehouse_table_urn, "refr_medium") in output_fields
        assert make_field_urn(warehouse_table_urn, "refr_source") in output_fields
        assert make_field_urn(warehouse_table_urn, "refr_term") in output_fields


class TestCurrencyConversionLineageExtractor:
    """Test Currency Conversion enrichment lineage extractor."""

    def test_supports_enrichment(self):
        """Test that extractor recognizes currency conversion schema."""
        extractor = CurrencyConversionLineageExtractor()

        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0"
        )
        assert not extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0"
        )

    def test_extract_lineage(self):
        """Test lineage extraction for currency conversion."""
        extractor = CurrencyConversionLineageExtractor()

        enrichment = Enrichment(
            id="test-id",
            filename="currency_conversion.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="Currency Conversion",
                    vendor="com.snowplowanalytics.snowplow",
                    parameters={"baseCurrency": "USD"},
                ),
            ),
        )

        event_schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event.1-0-0,PROD)"
        )
        warehouse_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD)"
        )

        lineages = extractor.extract_lineage(
            enrichment, event_schema_urn, warehouse_table_urn
        )

        # Should have 4 conversions
        assert len(lineages) == 4

        # Check first conversion (tr_total + tr_currency → tr_total_base)
        assert lineages[0].upstream_fields == [
            make_field_urn(event_schema_urn, "tr_total"),
            make_field_urn(event_schema_urn, "tr_currency"),
        ]
        assert lineages[0].downstream_fields == [
            make_field_urn(warehouse_table_urn, "tr_total_base")
        ]
        assert lineages[0].transformation_type == "DERIVED"

        # Check all conversions have 2 inputs (value + currency)
        for lineage in lineages:
            assert len(lineage.upstream_fields) == 2
            assert lineage.transformation_type == "DERIVED"


class TestUaParserLineageExtractor:
    """Test UA Parser enrichment lineage extractor."""

    def test_supports_enrichment(self):
        """Test that extractor recognizes UA parser schema."""
        extractor = UaParserLineageExtractor()

        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/ua_parser_config/jsonschema/1-0-0"
        )
        assert not extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0"
        )

    def test_extract_lineage_returns_empty(self):
        """Test lineage extraction returns empty (context lineage not yet implemented)."""
        extractor = UaParserLineageExtractor()

        enrichment = Enrichment(
            id="test-id",
            filename="ua_parser.json",
            enabled=True,
            last_update="2025-12-14T00:00:00Z",
            content=EnrichmentContent(
                schema_ref="iglu:com.snowplowanalytics.snowplow/ua_parser_config/jsonschema/1-0-0",
                data=EnrichmentContentData(
                    enabled=True,
                    name="UA Parser",
                    vendor="com.snowplowanalytics.snowplow",
                    parameters={},
                ),
            ),
        )

        event_schema_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.event.1-0-0,PROD)"
        )
        warehouse_table_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.events,PROD)"
        )

        lineages = extractor.extract_lineage(
            enrichment, event_schema_urn, warehouse_table_urn
        )

        # Context lineage not yet implemented
        assert len(lineages) == 0


class TestFieldLineageDataClass:
    """Test the FieldLineage dataclass."""

    def test_field_lineage_creation(self):
        """Test creating a FieldLineage object."""
        lineage = FieldLineage(
            upstream_fields=["urn:li:schemaField:(...,field1)"],
            downstream_fields=["urn:li:schemaField:(...,field2)"],
            transformation_type="DERIVED",
        )

        assert len(lineage.upstream_fields) == 1
        assert len(lineage.downstream_fields) == 1
        assert lineage.transformation_type == "DERIVED"

    def test_field_lineage_multiple_inputs(self):
        """Test FieldLineage with multiple input fields."""
        lineage = FieldLineage(
            upstream_fields=[
                "urn:li:schemaField:(...,field1)",
                "urn:li:schemaField:(...,field2)",
            ],
            downstream_fields=["urn:li:schemaField:(...,output)"],
            transformation_type="DERIVED",
        )

        assert len(lineage.upstream_fields) == 2
        assert len(lineage.downstream_fields) == 1
