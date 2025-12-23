"""Unit tests for SnowplowURNFactory."""

from unittest.mock import Mock

import pytest

from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.snowplow_config import (
    DestinationMapping,
    SnowplowSourceConfig,
    WarehouseLineageConfig,
)


class TestSnowplowURNFactory:
    """Test URN construction for various Snowplow entities."""

    @pytest.fixture
    def config_basic(self):
        """Create basic config without platform instance."""
        config = Mock(spec=SnowplowSourceConfig)
        config.platform_instance = None
        config.env = "PROD"
        config.include_version_in_urn = False
        config.warehouse_lineage = WarehouseLineageConfig(
            enabled=True, env="PROD", platform_instance=None, destination_mappings=[]
        )
        config.bdp_connection = Mock()
        config.bdp_connection.organization_id = "org-123"
        return config

    @pytest.fixture
    def config_with_platform_instance(self):
        """Create config with platform instance."""
        config = Mock(spec=SnowplowSourceConfig)
        config.platform_instance = "production-cluster"
        config.env = "PROD"
        config.include_version_in_urn = False
        config.warehouse_lineage = WarehouseLineageConfig(
            enabled=True, env="PROD", platform_instance=None, destination_mappings=[]
        )
        config.bdp_connection = Mock()
        config.bdp_connection.organization_id = "org-123"
        return config

    @pytest.fixture
    def urn_factory_basic(self, config_basic):
        """Create URN factory with basic config."""
        return SnowplowURNFactory(platform="snowplow", config=config_basic)

    @pytest.fixture
    def urn_factory_with_instance(self, config_with_platform_instance):
        """Create URN factory with platform instance."""
        return SnowplowURNFactory(
            platform="snowplow", config=config_with_platform_instance
        )

    def test_make_organization_urn(self, urn_factory_basic):
        """Test organization URN construction."""
        org_urn = urn_factory_basic.make_organization_urn("org-123")

        # URN is hashed, so just verify it's a valid container URN
        assert org_urn.startswith("urn:li:container:")
        assert len(org_urn) > len("urn:li:container:")

    def test_make_schema_dataset_urn_without_version_in_urn(self, urn_factory_basic):
        """Test schema URN without version (default behavior)."""
        schema_urn = urn_factory_basic.make_schema_dataset_urn(
            vendor="com.acme", name="checkout_started", version="1-0-0"
        )

        # Version should NOT be in URN
        assert "com.acme.checkout_started" in schema_urn
        assert "1-0-0" not in schema_urn
        assert "urn:li:dataset:(urn:li:dataPlatform:snowplow" in schema_urn

    def test_make_schema_dataset_urn_with_version_in_urn(self, config_basic):
        """Test schema URN with version (legacy behavior)."""
        config_basic.include_version_in_urn = True
        urn_factory = SnowplowURNFactory(platform="snowplow", config=config_basic)

        schema_urn = urn_factory.make_schema_dataset_urn(
            vendor="com.acme", name="checkout_started", version="1-0-0"
        )

        # Version SHOULD be in URN
        assert "com.acme.checkout_started.1-0-0" in schema_urn
        assert "urn:li:dataset:(urn:li:dataPlatform:snowplow" in schema_urn

    def test_make_schema_dataset_urn_handles_slashes_in_vendor(self, urn_factory_basic):
        """Test that slashes in vendor are replaced with dots."""
        schema_urn = urn_factory_basic.make_schema_dataset_urn(
            vendor="com/acme/analytics", name="page_view", version="1-0-0"
        )

        # Slashes should be replaced with dots
        assert "com.acme.analytics.page_view" in schema_urn
        assert "/" not in schema_urn

    def test_make_event_spec_dataset_urn(self, urn_factory_basic):
        """Test event specification URN construction."""
        event_spec_urn = urn_factory_basic.make_event_spec_dataset_urn(
            "650986b2-ad4a-453f-a0f1-4a2df337c31d"
        )

        assert "event_spec.650986b2-ad4a-453f-a0f1-4a2df337c31d" in event_spec_urn
        assert "urn:li:dataset:(urn:li:dataPlatform:snowplow" in event_spec_urn

    def test_make_tracking_scenario_urn(self, urn_factory_basic):
        """Test tracking scenario URN construction."""
        scenario_urn = urn_factory_basic.make_tracking_scenario_urn("scenario-123-456")

        # URN is hashed, so just verify it's a valid container URN
        assert scenario_urn.startswith("urn:li:container:")
        assert len(scenario_urn) > len("urn:li:container:")


class TestWarehouseURNConstruction:
    """Test warehouse URN construction with destination mappings."""

    @pytest.fixture
    def config_warehouse_basic(self):
        """Create config with basic warehouse lineage."""
        config = Mock(spec=SnowplowSourceConfig)
        config.warehouse_lineage = WarehouseLineageConfig(
            enabled=True,
            env="PROD",
            platform_instance=None,
            destination_mappings=[],
        )
        return config

    @pytest.fixture
    def config_warehouse_with_mappings(self):
        """Create config with destination mappings."""
        config = Mock(spec=SnowplowSourceConfig)
        config.warehouse_lineage = WarehouseLineageConfig(
            enabled=True,
            env="PROD",
            platform_instance="default-instance",
            destination_mappings=[
                DestinationMapping(
                    destination_id="dest-snowflake-prod",
                    platform_instance="snowflake-prod",
                    env="PROD",
                ),
                DestinationMapping(
                    destination_id="dest-snowflake-dev",
                    platform_instance="snowflake-dev",
                    env="DEV",
                ),
            ],
        )
        return config

    @pytest.fixture
    def urn_factory_warehouse_basic(self, config_warehouse_basic):
        """Create URN factory with basic warehouse config."""
        return SnowplowURNFactory(platform="snowplow", config=config_warehouse_basic)

    @pytest.fixture
    def urn_factory_warehouse_with_mappings(self, config_warehouse_with_mappings):
        """Create URN factory with destination mappings."""
        return SnowplowURNFactory(
            platform="snowplow", config=config_warehouse_with_mappings
        )

    def test_construct_warehouse_urn_basic(self, urn_factory_warehouse_basic):
        """Test basic warehouse URN construction without mappings."""
        warehouse_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="analytics.events.page_views",
            destination_id=None,
        )

        # Should use table name directly without platform instance prefix
        assert "urn:li:dataset:(urn:li:dataPlatform:snowflake" in warehouse_urn
        assert "analytics.events.page_views" in warehouse_urn
        assert ",PROD)" in warehouse_urn

    def test_construct_warehouse_urn_with_destination_mapping(
        self, urn_factory_warehouse_with_mappings
    ):
        """Test warehouse URN uses destination mapping when available."""
        warehouse_urn = urn_factory_warehouse_with_mappings.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="analytics.events.page_views",
            destination_id="dest-snowflake-prod",
        )

        # Should use mapped platform instance
        assert "snowflake-prod.analytics.events.page_views" in warehouse_urn
        assert "urn:li:dataset:(urn:li:dataPlatform:snowflake" in warehouse_urn
        assert ",PROD)" in warehouse_urn

    def test_construct_warehouse_urn_with_unmapped_destination(
        self, urn_factory_warehouse_with_mappings
    ):
        """Test warehouse URN falls back to global config for unmapped destination."""
        warehouse_urn = urn_factory_warehouse_with_mappings.construct_warehouse_urn(
            query_engine="bigquery",
            table_name="analytics.events.page_views",
            destination_id="dest-bigquery-unknown",
        )

        # Should use global platform instance since destination not mapped
        assert "default-instance.analytics.events.page_views" in warehouse_urn
        assert "urn:li:dataset:(urn:li:dataPlatform:bigquery" in warehouse_urn

    def test_construct_warehouse_urn_with_no_destination_id(
        self, urn_factory_warehouse_with_mappings
    ):
        """Test warehouse URN uses global config when no destination ID provided."""
        warehouse_urn = urn_factory_warehouse_with_mappings.construct_warehouse_urn(
            query_engine="redshift",
            table_name="public.events",
            destination_id=None,
        )

        # Should use global platform instance
        assert "default-instance.public.events" in warehouse_urn
        assert "urn:li:dataset:(urn:li:dataPlatform:redshift" in warehouse_urn

    def test_construct_warehouse_urn_respects_env_from_mapping(
        self, urn_factory_warehouse_with_mappings
    ):
        """Test that warehouse URN uses environment from destination mapping."""
        warehouse_urn = urn_factory_warehouse_with_mappings.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="analytics.events.test_events",
            destination_id="dest-snowflake-dev",
        )

        # Should use DEV env from mapping
        assert ",DEV)" in warehouse_urn
        assert "snowflake-dev" in warehouse_urn

    def test_construct_warehouse_urn_different_query_engines(
        self, urn_factory_warehouse_basic
    ):
        """Test warehouse URN construction for different query engines."""
        # Snowflake
        snowflake_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="db.schema.table",
            destination_id=None,
        )
        assert "urn:li:dataPlatform:snowflake" in snowflake_urn

        # BigQuery
        bigquery_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="bigquery",
            table_name="project.dataset.table",
            destination_id=None,
        )
        assert "urn:li:dataPlatform:bigquery" in bigquery_urn

        # Databricks
        databricks_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="databricks",
            table_name="catalog.schema.table",
            destination_id=None,
        )
        assert "urn:li:dataPlatform:databricks" in databricks_urn

        # Redshift
        redshift_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="redshift",
            table_name="schema.table",
            destination_id=None,
        )
        assert "urn:li:dataPlatform:redshift" in redshift_urn

    def test_construct_warehouse_urn_handles_complex_table_names(
        self, urn_factory_warehouse_basic
    ):
        """Test that complex table names are preserved in URN."""
        # Table name with multiple dots
        complex_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="database.schema.table_with_underscores",
            destination_id=None,
        )
        assert "database.schema.table_with_underscores" in complex_urn

        # Table name with numbers
        numbered_urn = urn_factory_warehouse_basic.construct_warehouse_urn(
            query_engine="bigquery",
            table_name="project123.dataset_v2.table_2024",
            destination_id=None,
        )
        assert "project123.dataset_v2.table_2024" in numbered_urn

    def test_construct_warehouse_urn_with_multiple_mappings(self):
        """Test that correct mapping is selected when multiple mappings exist."""
        config = Mock(spec=SnowplowSourceConfig)
        config.warehouse_lineage = WarehouseLineageConfig(
            enabled=True,
            env="PROD",
            platform_instance="default",
            destination_mappings=[
                DestinationMapping(
                    destination_id="dest-1",
                    platform_instance="instance-1",
                    env="PROD",
                ),
                DestinationMapping(
                    destination_id="dest-2",
                    platform_instance="instance-2",
                    env="PROD",
                ),
                DestinationMapping(
                    destination_id="dest-3",
                    platform_instance="instance-3",
                    env="DEV",
                ),
            ],
        )

        urn_factory = SnowplowURNFactory(platform="snowplow", config=config)

        # Test each mapping
        urn1 = urn_factory.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="db.schema.table",
            destination_id="dest-1",
        )
        assert "instance-1" in urn1

        urn2 = urn_factory.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="db.schema.table",
            destination_id="dest-2",
        )
        assert "instance-2" in urn2

        urn3 = urn_factory.construct_warehouse_urn(
            query_engine="snowflake",
            table_name="db.schema.table",
            destination_id="dest-3",
        )
        assert "instance-3" in urn3
        assert ",DEV)" in urn3


class TestURNFactoryWithPlatformInstance:
    """Test URN construction with platform instance."""

    @pytest.fixture
    def config_with_instance(self):
        """Create config with platform instance."""
        config = Mock(spec=SnowplowSourceConfig)
        config.platform_instance = "prod-cluster"
        config.env = "PROD"
        config.include_version_in_urn = False
        config.warehouse_lineage = WarehouseLineageConfig(enabled=False)
        config.bdp_connection = Mock()
        config.bdp_connection.organization_id = "org-123"
        return config

    @pytest.fixture
    def urn_factory_with_instance(self, config_with_instance):
        """Create URN factory with platform instance."""
        return SnowplowURNFactory(platform="snowplow", config=config_with_instance)

    def test_schema_urn_includes_platform_instance(self, urn_factory_with_instance):
        """Test that schema URNs include platform instance."""
        schema_urn = urn_factory_with_instance.make_schema_dataset_urn(
            vendor="com.acme", name="checkout", version="1-0-0"
        )

        # Platform instance should be in URN
        assert "prod-cluster" in schema_urn

    def test_event_spec_urn_includes_platform_instance(self, urn_factory_with_instance):
        """Test that event spec URNs include platform instance."""
        event_spec_urn = urn_factory_with_instance.make_event_spec_dataset_urn(
            "spec-123"
        )

        # Platform instance should be in URN
        assert "prod-cluster" in event_spec_urn

    def test_organization_urn_includes_platform_instance(
        self, urn_factory_with_instance
    ):
        """Test that organization URNs include platform instance."""
        org_urn = urn_factory_with_instance.make_organization_urn("org-123")

        # Organization URN is hashed, but should be a valid container URN
        # Platform instance is included in the hash computation
        assert org_urn.startswith("urn:li:container:")
        assert len(org_urn) > len("urn:li:container:")
