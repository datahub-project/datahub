"""Unit tests for EnrichmentRegistryFactory service."""

import pytest

from datahub.ingestion.source.snowplow.enrichment_lineage import (
    CampaignAttributionLineageExtractor,
    CurrencyConversionLineageExtractor,
    EnrichmentLineageRegistry,
    EventFingerprintLineageExtractor,
    IabSpidersRobotsLineageExtractor,
    IpLookupLineageExtractor,
    PiiPseudonymizationLineageExtractor,
    RefererParserLineageExtractor,
    UaParserLineageExtractor,
    YauaaLineageExtractor,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import (
    Enrichment,
    EnrichmentContent,
    EnrichmentContentData,
)
from datahub.ingestion.source.snowplow.services.enrichment_registry_factory import (
    EnrichmentRegistryFactory,
)


def make_enrichment(schema_ref: str, filename: str, name: str) -> Enrichment:
    """Helper to create an Enrichment with required fields."""
    return Enrichment(
        id="test-enrichment-id",
        filename=filename,
        content=EnrichmentContent(
            schema_ref=schema_ref,
            data=EnrichmentContentData(
                enabled=True,
                name=name,
                vendor="com.snowplowanalytics.snowplow",
            ),
        ),
        enabled=True,
        last_update="2024-01-01T00:00:00Z",
    )


class TestEnrichmentRegistryFactoryCreate:
    """Tests for EnrichmentRegistryFactory.create_registry() method."""

    def test_creates_enrichment_lineage_registry(self):
        """Test that factory creates an EnrichmentLineageRegistry instance."""
        registry = EnrichmentRegistryFactory.create_registry()

        assert isinstance(registry, EnrichmentLineageRegistry)

    def test_registers_nine_extractors(self):
        """Test that factory registers all 9 enrichment extractors."""
        registry = EnrichmentRegistryFactory.create_registry()

        assert registry.get_extractor_count() == 9

    def test_registers_ip_lookup_extractor(self):
        """Test that IP Lookup extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
            filename="ip_lookups",
            name="IP Lookups",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, IpLookupLineageExtractor)

    def test_registers_ua_parser_extractor(self):
        """Test that UA Parser extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow/ua_parser_config/jsonschema/1-0-0",
            filename="ua_parser_config",
            name="UA Parser",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, UaParserLineageExtractor)

    def test_registers_yauaa_extractor(self):
        """Test that YAUAA extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow.enrichments/yauaa_enrichment_config/jsonschema/1-0-0",
            filename="yauaa_enrichment_config",
            name="YAUAA",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, YauaaLineageExtractor)

    def test_registers_iab_spiders_robots_extractor(self):
        """Test that IAB Spiders & Robots extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow.enrichments/iab_spiders_and_robots_enrichment/jsonschema/1-0-0",
            filename="iab_spiders_and_robots_enrichment",
            name="IAB Spiders & Robots",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, IabSpidersRobotsLineageExtractor)

    def test_registers_pii_pseudonymization_extractor(self):
        """Test that PII Pseudonymization extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow.enrichments/pii_enrichment_config/jsonschema/2-0-0",
            filename="pii_enrichment_config",
            name="PII Pseudonymization",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, PiiPseudonymizationLineageExtractor)

    def test_registers_referer_parser_extractor(self):
        """Test that Referer Parser extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0",
            filename="referer_parser",
            name="Referer Parser",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, RefererParserLineageExtractor)

    def test_registers_currency_conversion_extractor(self):
        """Test that Currency Conversion extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0",
            filename="currency_conversion_config",
            name="Currency Conversion",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, CurrencyConversionLineageExtractor)

    def test_registers_campaign_attribution_extractor(self):
        """Test that Campaign Attribution extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-1",
            filename="campaign_attribution",
            name="Campaign Attribution",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, CampaignAttributionLineageExtractor)

    def test_registers_event_fingerprint_extractor(self):
        """Test that Event Fingerprint extractor is registered."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.snowplowanalytics.snowplow/event_fingerprint_config/jsonschema/1-0-1",
            filename="event_fingerprint_config",
            name="Event Fingerprint",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is not None
        assert isinstance(extractor, EventFingerprintLineageExtractor)

    def test_returns_none_for_unknown_enrichment(self):
        """Test that registry returns None for unknown enrichment types."""
        registry = EnrichmentRegistryFactory.create_registry()

        enrichment = make_enrichment(
            schema_ref="iglu:com.example/unknown_enrichment/jsonschema/1-0-0",
            filename="unknown_enrichment",
            name="Unknown",
        )

        extractor = registry.get_extractor(enrichment)

        assert extractor is None


class TestEnrichmentRegistryFactoryMultipleCreation:
    """Tests for creating multiple registry instances."""

    def test_creates_independent_registry_instances(self):
        """Test that each call creates an independent registry instance."""
        registry1 = EnrichmentRegistryFactory.create_registry()
        registry2 = EnrichmentRegistryFactory.create_registry()

        assert registry1 is not registry2

    def test_both_instances_have_same_extractors(self):
        """Test that both instances have the same number of extractors."""
        registry1 = EnrichmentRegistryFactory.create_registry()
        registry2 = EnrichmentRegistryFactory.create_registry()

        assert registry1.get_extractor_count() == registry2.get_extractor_count() == 9


class TestEnrichmentRegistryFactoryExtractorTypes:
    """Tests verifying all expected extractor types are registered."""

    @pytest.fixture
    def registry(self):
        """Create registry for testing."""
        return EnrichmentRegistryFactory.create_registry()

    def test_all_extractor_classes_are_correct_types(self, registry):
        """Test that all registered extractors are EnrichmentLineageExtractor subclasses."""
        # Access internal extractors list for type verification
        for extractor in registry._extractors:
            # All extractors should be subclasses of EnrichmentLineageExtractor
            from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
                EnrichmentLineageExtractor,
            )

            assert isinstance(extractor, EnrichmentLineageExtractor)

    def test_expected_extractor_class_names(self, registry):
        """Test that all expected extractor class names are present."""
        extractor_class_names = {type(e).__name__ for e in registry._extractors}

        expected_names = {
            "IpLookupLineageExtractor",
            "UaParserLineageExtractor",
            "YauaaLineageExtractor",
            "IabSpidersRobotsLineageExtractor",
            "PiiPseudonymizationLineageExtractor",
            "RefererParserLineageExtractor",
            "CurrencyConversionLineageExtractor",
            "CampaignAttributionLineageExtractor",
            "EventFingerprintLineageExtractor",
        }

        assert extractor_class_names == expected_names

    def test_no_duplicate_extractors(self, registry):
        """Test that no extractor class is registered multiple times."""
        extractor_classes = [type(e) for e in registry._extractors]
        unique_classes = set(extractor_classes)

        assert len(extractor_classes) == len(unique_classes), (
            "Duplicate extractors found"
        )
