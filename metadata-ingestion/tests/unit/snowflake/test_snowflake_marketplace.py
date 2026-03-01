"""Unit tests for Snowflake Marketplace handler."""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast

import pytest

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_marketplace import (
    SnowflakeMarketplaceHandler,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeMarketplacePurchase,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataproduct import (
    DataProductProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    InstitutionalMemoryClass,
)
from tests.unit.snowflake.conftest_marketplace import (  # type: ignore[import-untyped]
    FakeNativeConn as _FakeNativeConn,
)

# Test Fixtures


@pytest.fixture
def base_config() -> Dict[str, Any]:
    """Base configuration for marketplace tests."""
    return {
        "account_id": "test_account",
        "warehouse": "COMPUTE_WH",
        "role": "test_role",
        "username": "test_user",
        "marketplace": {
            "enabled": True,
        },
    }


@pytest.fixture
def mock_listings() -> List[Dict[str, Any]]:
    """Mock marketplace listings data."""
    return [
        {
            "global_name": "ACME.DATA.LISTING",  # Snowflake returns "global_name" not "listing_global_name"
            "title": "Acme Data",
            "uniform_listing_locator": "ORGACME$INTERNAL$ACME_DATA_LISTING",  # Used as fallback for name
            "organization_profile_name": "INTERNAL",  # Used as fallback for provider
            "category": "Sample",
            "description": "Sample listing",
            "created_on": datetime(2024, 1, 1, tzinfo=timezone.utc),
        },
        {
            "global_name": "WEATHER.PUBLIC.GLOBAL_DATA",  # Snowflake returns "global_name"
            "title": "Weather Data",
            "uniform_listing_locator": "ORGWEATHER$INTERNAL$WEATHER_DATA",
            "organization_profile_name": "INTERNAL",
            "category": "Environmental",
            "description": "Global weather data",
            "created_on": datetime(2024, 2, 1, tzinfo=timezone.utc),
        },
    ]


@pytest.fixture
def mock_purchases() -> List[Dict[str, Any]]:
    """Mock marketplace purchases data."""
    return [
        {
            "DATABASE_NAME": "DEMO_DATABASE",
            "PURCHASE_DATE": datetime(2024, 7, 1, tzinfo=timezone.utc),
            "OWNER": "ACCOUNTADMIN",
            "COMMENT": "Sample listing data from ACME.DATA.LISTING",
        },
        {
            "DATABASE_NAME": "WEATHER_DB",
            "PURCHASE_DATE": datetime(2024, 8, 1, tzinfo=timezone.utc),
            "OWNER": "SYSADMIN",
            "COMMENT": "Weather data for analysis",
        },
    ]


@pytest.fixture
def mock_usage_events() -> List[Dict[str, Any]]:
    """Mock marketplace usage events data."""
    return [
        {
            "EVENT_TIMESTAMP": datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
            "QUERY_ID": "query-token-001",
            "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
            "USER_NAME": "analyst_user",
            "SHARE_NAME": "ACME_SHARE",
            "SHARE_OBJECTS_ACCESSED": json.dumps(
                [
                    {
                        "objectName": "DEMO_DATABASE.PUBLIC.CUSTOMERS",
                        "objectDomain": "Table",
                    },
                ]
            ),
        },
        {
            "EVENT_TIMESTAMP": datetime(2024, 6, 1, 14, 0, 0, tzinfo=timezone.utc),
            "QUERY_ID": "query-token-002",
            "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
            "USER_NAME": "data_scientist",
            "SHARE_NAME": "ACME_SHARE",
            "SHARE_OBJECTS_ACCESSED": json.dumps(
                [
                    {
                        "objectName": "DEMO_DATABASE.PUBLIC.CUSTOMERS",
                        "objectDomain": "Table",
                    },
                ]
            ),
        },
    ]


def create_handler(
    config_dict: Dict[str, Any],
    mock_listings: Optional[List[Dict[str, Any]]] = None,
    mock_purchases: Optional[List[Dict[str, Any]]] = None,
    mock_usage: Optional[List[Dict[str, Any]]] = None,
    mock_describe: Optional[List[Dict[str, Any]]] = None,
) -> SnowflakeMarketplaceHandler:
    """Helper to create a SnowflakeMarketplaceHandler with mocked data."""
    config = SnowflakeV2Config.parse_obj(config_dict)
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(config, report)

    fake_native = _FakeNativeConn()

    if mock_listings is not None:
        fake_native.set_mock_response("SHOW AVAILABLE LISTINGS", mock_listings)
    if mock_purchases is not None:
        fake_native.set_mock_response("IMPORTED DATABASE", mock_purchases)
    if mock_usage is not None:
        fake_native.set_mock_response("LISTING_ACCESS_HISTORY", mock_usage)
    if mock_describe is not None:
        fake_native.set_mock_response("DESCRIBE AVAILABLE LISTING", mock_describe)

    connection = SnowflakeConnection(fake_native)  # type: ignore[arg-type]

    return SnowflakeMarketplaceHandler(
        config=config,
        report=report,
        connection=connection,
        identifiers=identifiers,
    )


def _get_data_product_urns(wus: List[MetadataWorkUnit]) -> List[str]:
    """Extract data product URNs from workunits."""
    urns = []
    for wu in wus:
        if hasattr(wu.metadata, "entityUrn"):
            urn = cast(str, wu.metadata.entityUrn)
            if "urn:li:dataProduct:" in urn:
                urns.append(urn)
    return list(set(urns))  # Deduplicate


# Core Functionality Tests


class TestMarketplaceBasicFunctionality:
    """Test basic marketplace listing and purchase functionality."""

    def test_creates_workunits_for_listings_and_purchases(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
    ) -> None:
        """Test that data products and dataset enhancements are created."""
        # Add shares configuration for proper linking
        config_with_shares = base_config.copy()
        config_with_shares["shares"] = {
            "ACME_SHARE": {
                "database": "ACME_DATA",  # Matches ACME.DATA.LISTING
                "platform_instance": None,
                "consumers": [{"database": "DEMO_DATABASE", "platform_instance": None}],
            }
        }
        handler = create_handler(config_with_shares, mock_listings, mock_purchases)
        wus = list(handler.get_marketplace_workunits())

        # Verify data products were created (check for data product URNs)
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 2

        # Verify dataset enhancement for DEMO_DATABASE
        dataset_props_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
            and hasattr(wu.metadata, "entityUrn")
            and "demo_database" in cast(str, wu.metadata.entityUrn).lower()
        ]
        assert len(dataset_props_wus) == 1

        # Verify the enhanced properties have marketplace metadata
        assert hasattr(dataset_props_wus[0].metadata, "aspect")
        demo_db_props = cast(
            DatasetPropertiesClass, dataset_props_wus[0].metadata.aspect
        )
        assert demo_db_props.customProperties["marketplace_purchase"] == "true"
        assert demo_db_props.customProperties["database_type"] == "IMPORTED_DATABASE"
        assert (
            "ACME.DATA.LISTING"
            in demo_db_props.customProperties["marketplace_listing_global_name"]
        )

        # Verify report metrics
        assert handler.report.marketplace_listings_scanned == 2
        assert handler.report.marketplace_purchases_scanned == 2
        assert handler.report.marketplace_data_products_created == 2
        assert handler.report.marketplace_enhanced_datasets == 2

    def test_listing_details_enrichment(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
    ) -> None:
        """Test that DESCRIBE AVAILABLE LISTING enrichment maps to custom properties and externalUrl."""
        config_dict = base_config.copy()
        config_dict["marketplace"]["fetch_internal_marketplace_listing_details"] = True

        # Only use the first listing (ACME)
        listings = [mock_listings[0]]

        describe_rows = [
            {"property": "DOCUMENTATION_URL", "value": "https://docs.acme.example"},
            {"property": "QUICKSTART_URL", "value": "https://quick.acme.example"},
            {"property": "SUPPORT_EMAIL", "value": "support@acme.example"},
            {"property": "CONTACT", "value": "Acme Support"},
            {"property": "REQUEST_APPROVER", "value": "approver@acme.example"},
        ]

        handler = create_handler(
            config_dict,
            listings,
            mock_purchases=[],
            mock_usage=None,
            mock_describe=describe_rows,
        )

        wus = list(handler.get_marketplace_workunits())

        # Find DataProductProperties for the ACME listing via customProperties listing_global_name
        dp_props: List[DataProductProperties] = []
        for wu in wus:
            aspect_any = getattr(wu.metadata, "aspect", None)
            if isinstance(aspect_any, DataProductProperties):
                aspect = cast(DataProductProperties, aspect_any)
                if (
                    aspect.customProperties is not None
                    and aspect.customProperties.get("listing_global_name")
                    == "ACME.DATA.LISTING"
                ):
                    dp_props.append(aspect)

        assert len(dp_props) == 1
        props = dp_props[0]
        # externalUrl should be set to the Snowflake marketplace URL
        assert (
            props.externalUrl
            == "https://app.snowflake.com/marketplace/internal/listing/ACME.DATA.LISTING"
        )
        # Mapped properties should be present
        assert (
            props.customProperties.get("documentation_url")
            == "https://docs.acme.example"
        )
        assert (
            props.customProperties.get("quickstart_url") == "https://quick.acme.example"
        )
        assert props.customProperties.get("support_email") == "support@acme.example"
        assert props.customProperties.get("support_contact") == "Acme Support"
        assert props.customProperties.get("request_approver") == "approver@acme.example"

        # Check InstitutionalMemory aspect for documentation links (marketplace URL is in externalUrl)
        institutional_memory_aspects = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, InstitutionalMemoryClass)
        ]
        assert len(institutional_memory_aspects) == 1
        memory = institutional_memory_aspects[0]
        assert len(memory.elements) >= 2  # At least 2 documentation URLs
        # Check that documentation links have simple "Documentation" description
        assert memory.elements[0].url == "https://docs.acme.example"
        assert memory.elements[0].description == "Documentation"
        assert memory.elements[1].url == "https://quick.acme.example"
        assert memory.elements[1].description == "Documentation"


# Heuristic Matching Tests


class TestListingPurchaseMatching:
    """Test the heuristic matching of purchases to listings."""

    def test_find_listing_with_shares_config(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
    ) -> None:
        """Test matching imported databases to listings using shares configuration."""
        # Add shares configuration
        config_with_shares = base_config.copy()
        config_with_shares["shares"] = {
            "ACME_SHARE": {
                "database": "ACME_DATA",  # Source database name - matches "ACME.DATA.LISTING"
                "platform_instance": None,
                "consumers": [
                    {"database": "IMPORTED_ACME_DB", "platform_instance": None}
                ],
            }
        }

        handler = create_handler(config_with_shares, mock_listings, [])
        handler._load_marketplace_data()

        # Create test purchase for imported database
        purchase = SnowflakeMarketplacePurchase(
            database_name="IMPORTED_ACME_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment=None,
        )

        found_listing = handler._find_listing_for_purchase(purchase)
        # Should match because "ACME_DATA" appears in "ACME.DATA.LISTING"
        assert found_listing == "ACME.DATA.LISTING"

    def test_find_listing_without_shares_config(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
    ) -> None:
        """Test that without shares config, no listing is found."""
        handler = create_handler(base_config, mock_listings, [])
        handler._load_marketplace_data()

        purchase = SnowflakeMarketplacePurchase(
            database_name="TEST_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment="Created from ACME_CORP.CUSTOMER.CUSTOMER_360",
        )

        found_listing = handler._find_listing_for_purchase(purchase)
        # Should return None because no shares config is provided
        assert found_listing is None

    def test_unknown_listing_purchase_status(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that purchases without matching listings get UNKNOWN_LISTING status."""
        unknown_purchase = [
            {
                "DATABASE_NAME": "UNKNOWN_DB",
                "PURCHASE_DATE": datetime(2024, 9, 1, tzinfo=timezone.utc),
                "OWNER": "ACCOUNTADMIN",
                "COMMENT": "Some database with no listing info",
            }
        ]

        handler = create_handler(base_config, mock_listings, unknown_purchase)
        wus = list(handler.get_marketplace_workunits())

        # Find the dataset properties for UNKNOWN_DB
        dataset_props_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
            and hasattr(wu.metadata, "entityUrn")
            and "unknown_db" in cast(str, wu.metadata.entityUrn).lower()
        ]

        assert len(dataset_props_wus) == 1
        props = cast(DatasetPropertiesClass, dataset_props_wus[0].metadata.aspect)  # type: ignore[union-attr]
        assert props.customProperties["purchase_status"] == "UNKNOWN_LISTING"


# Share Objects Parsing Tests


class TestShareObjectsParsing:
    """Test parsing of SHARE_OBJECTS_ACCESSED JSON arrays."""

    @pytest.mark.parametrize(
        "share_objects,expected_databases",
        [
            pytest.param(
                [
                    {"objectName": "DB1.SCHEMA1.TABLE1", "objectDomain": "Table"},
                    {"objectName": "DB1.SCHEMA2.TABLE2", "objectDomain": "Table"},
                ],
                ["DB1"],
                id="single_database",
            ),
            pytest.param(
                [
                    {"objectName": "DB1.SCHEMA1.TABLE1", "objectDomain": "Table"},
                    {"objectName": "DB2.SCHEMA2.TABLE2", "objectDomain": "Table"},
                    {"objectName": "DB1.SCHEMA3.TABLE3", "objectDomain": "Table"},
                ],
                ["DB1", "DB2"],
                id="multiple_databases",
            ),
            pytest.param(
                [
                    {
                        "objectName": '"MY_DB"."MY_SCHEMA"."MY_TABLE"',
                        "objectDomain": "Table",
                    },
                ],
                ["MY_DB"],
                id="quoted_identifiers",
            ),
            pytest.param(
                [
                    {
                        "objectName": "'QUOTED_DB'.'SCHEMA'.'TABLE'",
                        "objectDomain": "Table",
                    },
                ],
                ["QUOTED_DB"],
                id="single_quoted_identifiers",
            ),
            pytest.param(
                [],
                [],
                id="empty_array",
            ),
            pytest.param(
                [
                    {"objectName": "DB.SCHEMA.VIEW", "objectDomain": "View"},
                    {"objectName": "DB.SCHEMA.TABLE", "objectDomain": "Table"},
                ],
                ["DB"],
                id="mixed_object_types",
            ),
        ],
    )
    def test_parse_share_objects(
        self,
        base_config: Dict[str, Any],
        share_objects: List[Dict[str, Any]],
        expected_databases: List[str],
    ) -> None:
        """Test parsing share objects with various inputs."""
        handler = create_handler(base_config)
        share_objects_json = json.dumps(share_objects)
        databases = handler._parse_share_objects(share_objects_json)

        assert sorted(databases) == sorted(expected_databases)

    @pytest.mark.parametrize(
        "invalid_input",
        [
            pytest.param("not valid json", id="invalid_json"),
            pytest.param("{}", id="dict_not_array"),
            pytest.param("[]", id="empty_string_array"),
        ],
    )
    def test_parse_share_objects_invalid_input(
        self, base_config: Dict[str, Any], invalid_input: str
    ) -> None:
        """Test parsing share objects with invalid inputs."""
        handler = create_handler(base_config)
        databases = handler._parse_share_objects(invalid_input)
        assert databases == []

    def test_parse_share_objects_malformed_object_name(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test parsing share objects with malformed object names."""
        handler = create_handler(base_config)

        # Object name without dots (invalid format)
        share_objects_json = json.dumps(
            [
                {"objectName": "JUST_A_NAME", "objectDomain": "Table"},
            ]
        )
        databases = handler._parse_share_objects(share_objects_json)
        assert "JUST_A_NAME" in databases  # Still extracts the name

        # Object with missing objectName
        share_objects_json = json.dumps(
            [
                {"objectDomain": "Table"},
            ]
        )
        databases = handler._parse_share_objects(share_objects_json)
        assert databases == []


# Usage Statistics Tests


class TestMarketplaceUsageStatistics:
    """Test marketplace usage statistics creation."""

    def test_usage_statistics_creation(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
        mock_usage_events: List[Dict[str, Any]],
    ) -> None:
        """Test that usage statistics are created correctly."""
        config_dict = base_config.copy()
        config_dict["email_domain"] = "test.com"

        handler = create_handler(
            config_dict, mock_listings, mock_purchases, mock_usage_events
        )
        wus = list(handler.get_marketplace_workunits())

        # Find usage statistics workunit
        usage_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
        ]

        assert len(usage_wus) >= 1

        # Verify usage statistics
        usage_stats = cast(DatasetUsageStatisticsClass, usage_wus[0].metadata.aspect)  # type: ignore[union-attr]
        assert usage_stats.totalSqlQueries == 2
        assert usage_stats.uniqueUserCount == 2
        user_counts = usage_stats.userCounts
        assert user_counts is not None
        assert len(user_counts) == 2

        # Verify report metrics
        assert handler.report.marketplace_usage_events_processed == 2

    def test_usage_statistics_grouped_by_database(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
    ) -> None:
        """Test that usage events are correctly grouped by database."""
        config_dict = base_config.copy()
        config_dict["email_domain"] = "test.com"

        # Usage events accessing different databases
        usage_with_multiple_dbs = [
            {
                "EVENT_TIMESTAMP": datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
                "QUERY_ID": "query-001",
                "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
                "USER_NAME": "user1",
                "SHARE_NAME": "SHARE1",
                "SHARE_OBJECTS_ACCESSED": json.dumps(
                    [
                        {
                            "objectName": "DEMO_DATABASE.PUBLIC.TABLE1",
                            "objectDomain": "Table",
                        },
                        {
                            "objectName": "WEATHER_DB.PUBLIC.TABLE2",
                            "objectDomain": "Table",
                        },
                    ]
                ),
            },
        ]

        handler = create_handler(
            config_dict, mock_listings, mock_purchases, usage_with_multiple_dbs
        )
        wus = list(handler.get_marketplace_workunits())

        # Should have usage stats for both databases
        usage_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
        ]

        # We have 2 purchases (DEMO_DATABASE and WEATHER_DB), both accessed in one query
        # So we should have 2 usage workunits (one per database)
        assert len(usage_wus) == 2

    def test_usage_statistics_skips_unknown_listings(
        self,
        base_config: Dict[str, Any],
        mock_purchases: List[Dict[str, Any]],
        mock_usage_events: List[Dict[str, Any]],
    ) -> None:
        """Test that usage for unknown listings is skipped."""
        config_dict = base_config.copy()

        # No listings loaded, so usage should be skipped
        handler = create_handler(
            config_dict,
            mock_listings=[],
            mock_purchases=mock_purchases,
            mock_usage=mock_usage_events,
        )
        wus = list(handler.get_marketplace_workunits())

        # Should have no usage statistics workunits
        usage_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
        ]

        assert len(usage_wus) == 0

    def test_usage_statistics_skips_unknown_databases(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_usage_events: List[Dict[str, Any]],
    ) -> None:
        """Test that usage for unknown databases is skipped."""
        config_dict = base_config.copy()

        # No purchases loaded, so usage should be skipped
        handler = create_handler(
            config_dict,
            mock_listings=mock_listings,
            mock_purchases=[],
            mock_usage=mock_usage_events,
        )
        wus = list(handler.get_marketplace_workunits())

        # Should have no usage statistics workunits
        usage_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
        ]

        assert len(usage_wus) == 0


# Configuration Tests


class TestMarketplaceConfiguration:
    """Test configuration options for marketplace ingestion."""

    def test_listings_only_configuration(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test with only listings enabled."""
        config_dict = base_config.copy()

        handler = create_handler(config_dict, mock_listings, None, None)
        wus = list(handler.get_marketplace_workunits())

        # Should only have data product workunits
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 2

        # No dataset enhancements or lineage
        dataset_props_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
        ]
        assert len(dataset_props_wus) == 0

    def test_purchases_only_configuration(
        self, base_config: Dict[str, Any], mock_purchases: List[Dict[str, Any]]
    ) -> None:
        """Test with only purchases enabled."""
        config_dict = base_config.copy()

        handler = create_handler(config_dict, None, mock_purchases, None)
        wus = list(handler.get_marketplace_workunits())

        # Should only have dataset enhancement workunits
        dataset_props_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
        ]
        assert len(dataset_props_wus) == 2

        # No data products or lineage
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 0

    def test_marketplace_time_windows_used(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
        mock_usage_events: List[Dict[str, Any]],
    ) -> None:
        """Test that marketplace config uses its own time windows (BaseTimeWindowConfig)."""
        from datetime import datetime, timezone

        custom_start = datetime(2024, 6, 1, tzinfo=timezone.utc)
        custom_end = datetime(2024, 7, 1, tzinfo=timezone.utc)

        config_dict = base_config.copy()
        config_dict["marketplace"] = {
            "enabled": True,
            "start_time": custom_start.isoformat(),
            "end_time": custom_end.isoformat(),
            "bucket_duration": "HOUR",
        }
        config_dict["email_domain"] = "test.com"

        handler = create_handler(
            config_dict, mock_listings, mock_purchases, mock_usage_events
        )
        wus = list(handler.get_marketplace_workunits())

        usage_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
        ]

        assert len(usage_wus) >= 1
        usage_stats = cast(DatasetUsageStatisticsClass, usage_wus[0].metadata.aspect)  # type: ignore[union-attr]

        assert usage_stats.timestampMillis == int(custom_start.timestamp() * 1000)
        assert usage_stats.eventGranularity is not None
        assert usage_stats.eventGranularity.unit == "HOUR"


# Edge Cases and Error Handling


class TestMarketplaceEdgeCases:
    """Test edge cases and error handling."""

    def test_listing_purchase_matching_logic(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test the business logic of linking purchases to listings via shares config."""
        config_with_shares = base_config.copy()
        config_with_shares["shares"] = {
            "ACME_SHARE": {
                "database": "ACME_DATA",
                "platform_instance": None,
                "listing_global_name": "ACME.DATA.LISTING",
                "consumers": [{"database": "IMPORTED_DB", "platform_instance": None}],
            }
        }

        handler = create_handler(config_with_shares, mock_listings, [])
        handler._load_marketplace_data()

        purchase = SnowflakeMarketplacePurchase(
            database_name="IMPORTED_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment=None,
        )

        found_listing = handler._find_listing_for_purchase(purchase)
        assert found_listing == "ACME.DATA.LISTING", (
            "Should use explicit listing_global_name from shares config"
        )

    def test_marketplace_owner_patterns_apply_correctly(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that owner patterns are correctly applied to marketplace listings."""
        config_dict = base_config.copy()
        config_dict["marketplace"]["internal_marketplace_owner_patterns"] = {
            "^Acme.*": ["acme-team", "urn:li:corpGroup:data"],
            "Weather": ["weather-team"],
        }

        handler = create_handler(config_dict, mock_listings, [])
        handler._load_marketplace_data()

        acme_listing = handler._marketplace_listings["ACME.DATA.LISTING"]
        owners = handler._resolve_owners_for_listing(acme_listing)

        assert len(owners) == 2
        assert "urn:li:corpuser:acme-team" in owners
        assert "urn:li:corpGroup:data" in owners

    def test_structured_property_definitions_created_correctly(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test that structured property definitions are created with correct schema."""
        config_dict = base_config.copy()
        config_dict["marketplace"][
            "marketplace_properties_as_structured_properties"
        ] = True

        handler = create_handler(config_dict, [], [])
        wus = list(handler.get_marketplace_workunits())

        prop_def_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), StructuredPropertyDefinition
            )
        ]

        assert len(prop_def_wus) >= 6
        property_ids = [
            cast(StructuredPropertyDefinition, wu.metadata.aspect).qualifiedName  # type: ignore[union-attr]
            for wu in prop_def_wus
        ]

        assert "snowflake.marketplace.provider" in property_ids
        assert "snowflake.marketplace.category" in property_ids
        assert "snowflake.marketplace.listing_global_name" in property_ids

    def test_listing_without_optional_fields(self, base_config: Dict[str, Any]) -> None:
        """Test listing with minimal fields."""
        minimal_listing = [
            {
                "name": "minimal",
                "global_name": "MINIMAL.LISTING",  # Snowflake returns "global_name"
                "title": "Minimal",
                "provider": "Test",
                "category": None,
                "description": None,
                "created_on": None,
            }
        ]

        handler = create_handler(base_config, minimal_listing, [])
        wus = list(handler.get_marketplace_workunits())

        # Should still create data product
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 1

    def test_purchase_without_comment(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test purchase without comment field."""
        purchase_no_comment = [
            {
                "DATABASE_NAME": "NO_COMMENT_DB",
                "PURCHASE_DATE": datetime(2024, 7, 1, tzinfo=timezone.utc),
                "OWNER": "ACCOUNTADMIN",
                "COMMENT": None,
            }
        ]

        handler = create_handler(base_config, mock_listings, purchase_no_comment)
        wus = list(handler.get_marketplace_workunits())

        # Should still create dataset enhancement with UNKNOWN_LISTING status
        dataset_props_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
        ]
        assert len(dataset_props_wus) == 1
        props = cast(DatasetPropertiesClass, dataset_props_wus[0].metadata.aspect)  # type: ignore[union-attr]
        assert props.customProperties["purchase_status"] == "UNKNOWN_LISTING"
