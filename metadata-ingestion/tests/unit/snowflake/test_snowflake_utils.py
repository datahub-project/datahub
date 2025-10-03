import pytest

from datahub.emitter.mcp_builder import DataProductKey
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)


class TestSnowflakeIdentifierBuilderMarketplace:
    """Unit tests for marketplace-related functionality in SnowflakeIdentifierBuilder"""

    @pytest.fixture
    def identifier_config(self):
        return SnowflakeIdentifierConfig(
            convert_urns_to_lowercase=True,
            platform_instance="test_instance",
            env="PROD",
        )

    @pytest.fixture
    def report(self):
        return SnowflakeV2Report()

    @pytest.fixture
    def builder(self, identifier_config, report):
        return SnowflakeIdentifierBuilder(identifier_config, report)

    def test_gen_marketplace_data_product_key(self, builder):
        """Test generation of marketplace data product keys"""
        listing_name = "TEST_LISTING_123"

        key = builder.gen_marketplace_data_product_key(listing_name)

        assert isinstance(key, DataProductKey)
        assert key.platform == "snowflake-marketplace"
        assert key.name == "test_listing_123"  # Should be lowercased
        assert key.instance == "test_instance"
        assert key.env == "PROD"

    def test_gen_marketplace_data_product_key_no_lowercase(self):
        """Test data product key generation without lowercase conversion"""
        config = SnowflakeIdentifierConfig(
            convert_urns_to_lowercase=False,
            platform_instance="test_instance",
            env="DEV",
        )
        report = SnowflakeV2Report()
        builder = SnowflakeIdentifierBuilder(config, report)

        listing_name = "TEST_LISTING_123"
        key = builder.gen_marketplace_data_product_key(listing_name)

        assert key.name == "TEST_LISTING_123"  # Should preserve case
        assert key.env == "DEV"

    def test_gen_marketplace_data_product_urn(self, builder):
        """Test generation of marketplace data product URNs"""
        listing_name = "TEST_LISTING_123"

        urn = builder.gen_marketplace_data_product_urn(listing_name)

        assert urn.startswith("urn:li:dataProduct:")

    def test_marketplace_data_product_key_uniqueness(self, builder):
        """Test that different listing names generate different keys"""
        listing1 = "LISTING_A"
        listing2 = "LISTING_B"

        key1 = builder.gen_marketplace_data_product_key(listing1)
        key2 = builder.gen_marketplace_data_product_key(listing2)

        assert key1.guid() != key2.guid()
        assert key1.as_urn() != key2.as_urn()

    def test_marketplace_data_product_key_consistency(self, builder):
        """Test that the same listing name always generates the same key"""
        listing_name = "TEST_LISTING"

        key1 = builder.gen_marketplace_data_product_key(listing_name)
        key2 = builder.gen_marketplace_data_product_key(listing_name)

        assert key1.guid() == key2.guid()
        assert key1.as_urn() == key2.as_urn()

    def test_marketplace_data_product_key_with_special_characters(self, builder):
        """Test data product key generation with special characters"""
        listing_name = "TEST-LISTING_WITH.SPECIAL@CHARS"

        key = builder.gen_marketplace_data_product_key(listing_name)

        # Should handle special characters gracefully
        assert isinstance(key, DataProductKey)
        assert key.platform == "snowflake-marketplace"
        assert key.name == "test-listing_with.special@chars"

    def test_marketplace_data_product_key_different_configs(self):
        """Test that different configs generate different keys for same listing"""
        listing_name = "TEST_LISTING"

        config1 = SnowflakeIdentifierConfig(
            platform_instance="instance1",
            env="PROD",
        )
        config2 = SnowflakeIdentifierConfig(
            platform_instance="instance2",
            env="PROD",
        )

        report = SnowflakeV2Report()
        builder1 = SnowflakeIdentifierBuilder(config1, report)
        builder2 = SnowflakeIdentifierBuilder(config2, report)

        key1 = builder1.gen_marketplace_data_product_key(listing_name)
        key2 = builder2.gen_marketplace_data_product_key(listing_name)

        # Same listing name but different configs should generate different keys
        assert key1.guid() != key2.guid()
        assert key1.instance != key2.instance

    def test_marketplace_data_product_key_different_envs(self):
        """Test that different environments generate different keys"""
        listing_name = "TEST_LISTING"

        config1 = SnowflakeIdentifierConfig(env="PROD")
        config2 = SnowflakeIdentifierConfig(env="DEV")

        report = SnowflakeV2Report()
        builder1 = SnowflakeIdentifierBuilder(config1, report)
        builder2 = SnowflakeIdentifierBuilder(config2, report)

        key1 = builder1.gen_marketplace_data_product_key(listing_name)
        key2 = builder2.gen_marketplace_data_product_key(listing_name)

        assert key1.guid() != key2.guid()
        assert key1.env != key2.env

    def test_marketplace_platform_separation(self, builder):
        """Test that marketplace uses separate platform from regular snowflake"""
        # Test that marketplace data products use a different platform
        listing_name = "TEST_LISTING"
        key = builder.gen_marketplace_data_product_key(listing_name)

        assert key.platform == "snowflake-marketplace"
        assert key.platform != "snowflake"  # Should be different from regular platform

    def test_marketplace_urn_format(self, builder):
        """Test that marketplace URNs follow the correct format"""
        listing_name = "TEST_LISTING_123"
        urn = builder.gen_marketplace_data_product_urn(listing_name)

        # URN should follow the pattern: urn:li:dataProduct:{guid}
        assert urn.startswith("urn:li:dataProduct:")

        # Extract GUID and verify it's not empty
        guid_part = urn.split(":")[-1]
        assert len(guid_part) > 0
        assert guid_part != "None"
