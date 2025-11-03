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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProperties,
    DatasetUsageStatistics,
)


class _FakeCursor:
    """Mock cursor for test database queries."""

    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = rows
        self.rowcount = len(rows)

    def execute(self, _query: str) -> "_FakeCursor":
        return self

    def __iter__(self):
        return iter(self._rows)


class _FakeNativeConn:
    """Mock Snowflake connection for testing."""

    def __init__(self) -> None:
        self._last_query: Optional[str] = None
        self._mock_responses: Dict[str, List[Dict[str, Any]]] = {}

    def set_mock_response(self, query_pattern: str, rows: List[Dict[str, Any]]) -> None:
        """Set mock response for a query pattern."""
        self._mock_responses[query_pattern] = rows

    def cursor(self, _cursor_type):  # type: ignore[no-untyped-def]
        return self

    def execute(self, query: str) -> _FakeCursor:
        self._last_query = query

        # Check mock responses
        for pattern, rows in self._mock_responses.items():
            if pattern in query:
                return _FakeCursor(rows)

        # Default empty
        return _FakeCursor([])

    def is_closed(self) -> bool:
        return False

    def close(self) -> None:
        return None


# Test Fixtures


@pytest.fixture
def base_config() -> Dict[str, Any]:
    """Base configuration for marketplace tests."""
    return {
        "account_id": "test_account",
        "warehouse": "COMPUTE_WH",
        "role": "test_role",
        "username": "test_user",
        "include_internal_marketplace": True,
    }


@pytest.fixture
def mock_listings() -> List[Dict[str, Any]]:
    """Mock marketplace listings data."""
    return [
        {
            "name": "acme_data_listing",
            "listing_global_name": "ACME.DATA.LISTING",
            "title": "Acme Data",
            "provider": "Acme Corp",
            "category": "Sample",
            "description": "Sample listing",
            "created_on": datetime(2024, 1, 1, tzinfo=timezone.utc),
        },
        {
            "name": "weather_listing",
            "listing_global_name": "WEATHER.PUBLIC.GLOBAL_DATA",
            "title": "Weather Data",
            "provider": "Weather Co",
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
        handler = create_handler(base_config, mock_listings, mock_purchases)
        wus = list(handler.get_marketplace_workunits())

        # Verify data products were created (check for data product URNs)
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 2

        # Verify dataset enhancement for DEMO_DATABASE
        dataset_props_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetProperties)
            and hasattr(wu.metadata, "entityUrn")
            and "demo_database" in cast(str, wu.metadata.entityUrn).lower()
        ]
        assert len(dataset_props_wus) == 1

        # Verify the enhanced properties have marketplace metadata
        demo_db_props = cast(DatasetProperties, dataset_props_wus[0].metadata.aspect)  # type: ignore[union-attr]
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
        config_dict["fetch_internal_marketplace_listing_details"] = True

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
        # externalUrl should be populated from documentation_url
        assert props.externalUrl == "https://docs.acme.example"
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


# Heuristic Matching Tests


class TestListingPurchaseMatching:
    """Test the heuristic matching of purchases to listings."""

    @pytest.mark.parametrize(
        "comment,expected_listing",
        [
            pytest.param(
                "Sample listing data from ACME.DATA.LISTING",
                "ACME.DATA.LISTING",
                id="exact_match",
            ),
            pytest.param(
                "Data from acme.data.listing for analysis",
                "ACME.DATA.LISTING",
                id="case_insensitive",
            ),
            pytest.param(
                "Contains WEATHER.PUBLIC.GLOBAL_DATA in middle",
                "WEATHER.PUBLIC.GLOBAL_DATA",
                id="substring_match",
            ),
            pytest.param(
                "No listing info here",
                None,
                id="no_match",
            ),
            pytest.param(
                None,
                None,
                id="null_comment",
            ),
        ],
    )
    def test_find_listing_for_purchase_heuristic(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        comment: Optional[str],
        expected_listing: Optional[str],
    ) -> None:
        """Test heuristic matching with various comment patterns."""
        handler = create_handler(base_config, mock_listings, [])
        handler._load_marketplace_data()

        # Create test purchase
        purchase = SnowflakeMarketplacePurchase(
            database_name="TEST_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment=comment,
        )

        found_listing = handler._find_listing_for_purchase(purchase)
        assert found_listing == expected_listing

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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetProperties)
            and hasattr(wu.metadata, "entityUrn")
            and "unknown_db" in cast(str, wu.metadata.entityUrn).lower()
        ]

        assert len(dataset_props_wus) == 1
        props = cast(DatasetProperties, dataset_props_wus[0].metadata.aspect)  # type: ignore[union-attr]
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetUsageStatistics)
        ]

        assert len(usage_wus) >= 1

        # Verify usage statistics
        usage_stats = cast(DatasetUsageStatistics, usage_wus[0].metadata.aspect)  # type: ignore[union-attr]
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetUsageStatistics)
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetUsageStatistics)
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetUsageStatistics)
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetProperties)
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetProperties)
        ]
        assert len(dataset_props_wus) == 2

        # No data products or lineage
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 0

    def test_all_features_disabled(self, base_config: Dict[str, Any]) -> None:
        """Test with all marketplace features disabled."""
        config_dict = base_config.copy()
        config_dict["include_internal_marketplace"] = False

        handler = create_handler(config_dict, None, None, None)
        wus = list(handler.get_marketplace_workunits())

        # Should have no workunits
        assert len(wus) == 0


# Edge Cases and Error Handling


class TestMarketplaceEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_listings_and_purchases(self, base_config: Dict[str, Any]) -> None:
        """Test with no listings or purchases."""
        handler = create_handler(base_config, [], [])
        wus = list(handler.get_marketplace_workunits())

        assert len(wus) == 0
        assert handler.report.marketplace_listings_scanned == 0
        assert handler.report.marketplace_purchases_scanned == 0

    def test_listing_without_optional_fields(self, base_config: Dict[str, Any]) -> None:
        """Test listing with minimal fields."""
        minimal_listing = [
            {
                "name": "minimal",
                "listing_global_name": "MINIMAL.LISTING",
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
            and isinstance(getattr(wu.metadata, "aspect", None), DatasetProperties)
        ]
        assert len(dataset_props_wus) == 1
        props = cast(DatasetProperties, dataset_props_wus[0].metadata.aspect)  # type: ignore[union-attr]
        assert props.customProperties["purchase_status"] == "UNKNOWN_LISTING"
