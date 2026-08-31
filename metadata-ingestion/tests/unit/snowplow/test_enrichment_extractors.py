"""Unit tests for enrichment lineage extractors."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.enrichment_lineage.base import FieldLineage
from datahub.ingestion.source.snowplow.enrichment_lineage.extractors.campaign_attribution import (
    CampaignAttributionLineageExtractor,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.extractors.ip_lookup import (
    IpLookupLineageExtractor,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.extractors.pii_pseudonymization import (
    PiiPseudonymizationLineageExtractor,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.extractors.referer_parser import (
    RefererParserLineageExtractor,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.extractors.ua_parser import (
    UaParserLineageExtractor,
)


class TestIpLookupLineageExtractor:
    """Tests for IP Lookup enrichment lineage extraction."""

    @pytest.fixture
    def extractor(self):
        """Create IP Lookup extractor instance."""
        return IpLookupLineageExtractor()

    @pytest.fixture
    def mock_enrichment(self):
        """Create mock enrichment with configurable parameters."""
        enrichment = Mock()
        enrichment.parameters = {}
        return enrichment

    def test_supports_ip_lookups_schema(self, extractor):
        """Test extractor recognizes IP lookups schema."""
        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0"
        )
        assert extractor.supports_enrichment(
            "iglu:com.example/ip_lookups/jsonschema/1-0-0"
        )

    def test_does_not_support_other_schemas(self, extractor):
        """Test extractor rejects non-IP lookup schemas."""
        assert not extractor.supports_enrichment(
            "iglu:com.snowplow/ua_parser_config/jsonschema/1-0-0"
        )
        assert not extractor.supports_enrichment(
            "iglu:com.snowplow/campaign_attribution/jsonschema/1-0-0"
        )

    def test_returns_empty_when_no_warehouse_urn(self, extractor, mock_enrichment):
        """Test returns empty list when warehouse URN is None."""
        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn=None,
        )
        assert result == []

    def test_extracts_geo_fields_when_geo_configured(self, extractor, mock_enrichment):
        """Test extracts geo fields when geo database is configured."""
        mock_enrichment.parameters = {"geo": {"database": "GeoLite2-City.mmdb"}}

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # Should have all 8 geo fields
        assert len(result) == len(IpLookupLineageExtractor.GEO_FIELDS)
        downstream_fields = [lineage.downstream_fields[0] for lineage in result]
        assert any("geo_country" in f for f in downstream_fields)
        assert any("geo_city" in f for f in downstream_fields)
        assert any("geo_latitude" in f for f in downstream_fields)
        assert any("geo_timezone" in f for f in downstream_fields)

    def test_extracts_isp_fields_when_isp_configured(self, extractor, mock_enrichment):
        """Test extracts ISP fields when ISP database is configured."""
        mock_enrichment.parameters = {"isp": {"database": "GeoIP2-ISP.mmdb"}}

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # Should have 2 ISP fields
        assert len(result) == len(IpLookupLineageExtractor.ISP_FIELDS)
        downstream_fields = [lineage.downstream_fields[0] for lineage in result]
        assert any("ip_isp" in f for f in downstream_fields)
        assert any("ip_organization" in f for f in downstream_fields)

    def test_extracts_domain_field_when_domain_configured(
        self, extractor, mock_enrichment
    ):
        """Test extracts domain field when domain database is configured."""
        mock_enrichment.parameters = {"domain": {"database": "GeoIP2-Domain.mmdb"}}

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # Should have 1 domain field
        assert len(result) == 1
        assert "ip_domain" in result[0].downstream_fields[0]

    def test_extracts_connection_field_when_configured(
        self, extractor, mock_enrichment
    ):
        """Test extracts connection type field when configured."""
        mock_enrichment.parameters = {
            "connectionType": {"database": "GeoIP2-Connection-Type.mmdb"}
        }

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # Should have 1 connection field
        assert len(result) == 1
        assert "ip_netspeed" in result[0].downstream_fields[0]

    def test_extracts_all_fields_when_all_databases_configured(
        self, extractor, mock_enrichment
    ):
        """Test extracts all fields when all databases are configured."""
        mock_enrichment.parameters = {
            "geo": {"database": "GeoIP2-City.mmdb"},
            "isp": {"database": "GeoIP2-ISP.mmdb"},
            "domain": {"database": "GeoIP2-Domain.mmdb"},
            "connectionType": {"database": "GeoIP2-Connection-Type.mmdb"},
        }

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # Should have geo (8) + ISP (2) + domain (1) + connection (1) = 12 fields
        expected_count = (
            len(IpLookupLineageExtractor.GEO_FIELDS)
            + len(IpLookupLineageExtractor.ISP_FIELDS)
            + len(IpLookupLineageExtractor.DOMAIN_FIELDS)
            + len(IpLookupLineageExtractor.CONNECTION_FIELDS)
        )
        assert len(result) == expected_count

    def test_returns_empty_when_no_databases_configured(
        self, extractor, mock_enrichment
    ):
        """Test returns empty when no databases are configured."""
        mock_enrichment.parameters = {}

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        assert result == []

    def test_upstream_field_is_user_ipaddress(self, extractor, mock_enrichment):
        """Test all lineages use user_ipaddress as upstream field."""
        mock_enrichment.parameters = {"geo": {"database": "GeoLite2-City.mmdb"}}

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # All lineages should have user_ipaddress as upstream
        for lineage in result:
            assert len(lineage.upstream_fields) == 1
            assert "user_ipaddress" in lineage.upstream_fields[0]

    def test_transformation_type_is_derived(self, extractor, mock_enrichment):
        """Test all lineages have DERIVED transformation type."""
        mock_enrichment.parameters = {"geo": {"database": "GeoLite2-City.mmdb"}}

        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        for lineage in result:
            assert lineage.transformation_type == "DERIVED"


class TestUaParserLineageExtractor:
    """Tests for UA Parser enrichment lineage extraction."""

    @pytest.fixture
    def extractor(self):
        """Create UA Parser extractor instance."""
        return UaParserLineageExtractor()

    @pytest.fixture
    def mock_enrichment(self):
        """Create mock enrichment."""
        return Mock()

    def test_supports_ua_parser_schema(self, extractor):
        """Test extractor recognizes UA Parser config schema."""
        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/ua_parser_config/jsonschema/1-0-0"
        )

    def test_does_not_support_other_schemas(self, extractor):
        """Test extractor rejects non-UA parser schemas."""
        assert not extractor.supports_enrichment(
            "iglu:com.snowplow/ip_lookups/jsonschema/2-0-0"
        )

    def test_returns_empty_list(self, extractor, mock_enrichment):
        """Test returns empty list (context lineage not yet implemented)."""
        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )

        # Context lineage not yet implemented, should return empty
        assert result == []


class TestRefererParserLineageExtractor:
    """Tests for Referer Parser enrichment lineage extraction."""

    @pytest.fixture
    def extractor(self):
        """Create Referer Parser extractor instance."""
        return RefererParserLineageExtractor()

    def test_supports_referer_parser_schema(self, extractor):
        """Test extractor recognizes Referer Parser schema."""
        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0"
        )

    def test_does_not_support_other_schemas(self, extractor):
        """Test extractor rejects non-Referer parser schemas."""
        assert not extractor.supports_enrichment(
            "iglu:com.snowplow/ip_lookups/jsonschema/2-0-0"
        )


class TestCampaignAttributionLineageExtractor:
    """Tests for Campaign Attribution enrichment lineage extraction."""

    @pytest.fixture
    def extractor(self):
        """Create Campaign Attribution extractor instance."""
        return CampaignAttributionLineageExtractor()

    def test_supports_campaign_attribution_schema(self, extractor):
        """Test extractor recognizes Campaign Attribution schema."""
        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-1"
        )

    def test_does_not_support_other_schemas(self, extractor):
        """Test extractor rejects non-Campaign attribution schemas."""
        assert not extractor.supports_enrichment(
            "iglu:com.snowplow/ip_lookups/jsonschema/2-0-0"
        )


class TestPiiPseudonymizationLineageExtractor:
    """Tests for PII Pseudonymization enrichment lineage extraction."""

    @pytest.fixture
    def extractor(self):
        """Create PII Pseudonymization extractor instance."""
        return PiiPseudonymizationLineageExtractor()

    @pytest.fixture
    def mock_enrichment(self):
        """Create mock enrichment."""
        return Mock()

    def test_supports_pii_enrichment_schema(self, extractor):
        """Test extractor recognizes PII enrichment config schema."""
        assert extractor.supports_enrichment(
            "iglu:com.snowplowanalytics.snowplow.enrichments/pii_enrichment_config/jsonschema/2-0-0"
        )

    def test_does_not_support_other_schemas(self, extractor):
        """Test extractor rejects non-PII schemas."""
        assert not extractor.supports_enrichment(
            "iglu:com.snowplow/ip_lookups/jsonschema/2-0-0"
        )

    def test_returns_empty_when_no_warehouse_urn(self, extractor, mock_enrichment):
        """Test returns empty list when warehouse URN is None."""
        result = extractor.extract_lineage(
            enrichment=mock_enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn=None,
        )
        assert result == []

    def test_returns_empty_when_no_content(self, extractor):
        """Test returns empty list when enrichment has no content."""
        enrichment = Mock()
        enrichment.content = None

        result = extractor.extract_lineage(
            enrichment=enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )
        assert result == []

    def test_returns_empty_when_no_pii_config(self, extractor):
        """Test returns empty list when no PII config in parameters."""
        enrichment = Mock()
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {"other": "value"}

        result = extractor.extract_lineage(
            enrichment=enrichment,
            event_schema_urn="urn:li:dataset:event",
            warehouse_table_urn="urn:li:dataset:warehouse",
        )
        assert result == []

    def test_extracts_pojo_field_lineage(self, extractor):
        """Test extracting lineage for POJO fields."""
        enrichment = Mock()
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {
            "pii": {"pojo": [{"field": "user_id"}, {"field": "user_ipaddress"}]}
        }
        enrichment.filename = "pii.json"

        result = extractor.extract_lineage(
            enrichment=enrichment,
            event_schema_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,event,PROD)",
            warehouse_table_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse,PROD)",
        )

        # Should extract 2 lineages (one per POJO field)
        assert len(result) == 2
        assert result[0].transformation_type == "IN_PLACE"
        assert "user_id" in result[0].upstream_fields[0]
        assert "user_id" in result[0].downstream_fields[0]

    def test_extracts_json_field_lineage(self, extractor):
        """Test extracting lineage for JSON fields."""
        enrichment = Mock()
        enrichment.content = Mock()
        enrichment.content.data = Mock()
        enrichment.content.data.parameters = {
            "pii": {
                "json": [
                    {
                        "field": "contexts",
                        "schemaCriterion": "iglu:com.acme/checkout/jsonschema/1-*-*",
                        "jsonPath": "$.customer.email",
                    }
                ]
            }
        }
        enrichment.filename = "pii.json"

        result = extractor.extract_lineage(
            enrichment=enrichment,
            event_schema_urn="urn:li:dataset:(urn:li:dataPlatform:snowplow,event,PROD)",
            warehouse_table_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse,PROD)",
        )

        # Should extract 1 lineage for the JSON field
        assert len(result) == 1
        assert result[0].transformation_type == "IN_PLACE"
        assert "customer.email" in result[0].upstream_fields[0]
        assert "email" in result[0].downstream_fields[0]


class TestFieldLineageDataclass:
    """Tests for the FieldLineage dataclass."""

    def test_field_lineage_creation(self):
        """Test creating a FieldLineage instance."""
        lineage = FieldLineage(
            upstream_fields=[
                "urn:li:schemaField:(urn:li:dataset:event,user_ipaddress)"
            ],
            downstream_fields=[
                "urn:li:schemaField:(urn:li:dataset:warehouse,geo_country)"
            ],
            transformation_type="DERIVED",
        )

        assert len(lineage.upstream_fields) == 1
        assert len(lineage.downstream_fields) == 1
        assert lineage.transformation_type == "DERIVED"

    def test_field_lineage_multiple_upstreams(self):
        """Test FieldLineage with multiple upstream fields."""
        lineage = FieldLineage(
            upstream_fields=[
                "urn:li:schemaField:(urn:li:dataset:event,field1)",
                "urn:li:schemaField:(urn:li:dataset:event,field2)",
            ],
            downstream_fields=[
                "urn:li:schemaField:(urn:li:dataset:warehouse,combined)"
            ],
            transformation_type="AGGREGATED",
        )

        assert len(lineage.upstream_fields) == 2
        assert lineage.transformation_type == "AGGREGATED"

    def test_field_lineage_in_place_transformation(self):
        """Test FieldLineage for in-place transformations (like PII)."""
        lineage = FieldLineage(
            upstream_fields=["urn:li:schemaField:(urn:li:dataset:event,user_id)"],
            downstream_fields=["urn:li:schemaField:(urn:li:dataset:warehouse,user_id)"],
            transformation_type="IN_PLACE",
        )

        assert lineage.transformation_type == "IN_PLACE"
        # Same field name, different datasets (event → warehouse)
        assert "user_id" in lineage.upstream_fields[0]
        assert "user_id" in lineage.downstream_fields[0]
