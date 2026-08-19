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
    SnowsightUrlBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
    GlobalTagsClass,
)
from tests.unit.snowflake.conftest_marketplace import (  # type: ignore[import-untyped]
    FakeNativeConn as _FakeNativeConn,
)

# ---------------------------------------------------------------------------
# Helpers for inspecting PATCH workunits
# ---------------------------------------------------------------------------


def _aspect_name(wu: MetadataWorkUnit) -> Optional[str]:
    """Return the aspect name for both UPSERT (typed) and PATCH workunits."""
    aspect = getattr(wu.metadata, "aspect", None)
    if aspect is not None and hasattr(aspect, "get_aspect_name"):
        return aspect.get_aspect_name()
    return getattr(wu.metadata, "aspectName", None)


def _patch_ops(wu: MetadataWorkUnit) -> List[Dict[str, Any]]:
    """Return the list of JSON-Patch operations from a PATCH workunit.

    Handles both the bare-list payload (plain JSON-Patch array, used when no
    ``arrayPrimaryKeys`` are needed) and the ``GenericJsonPatch`` envelope
    (``{"arrayPrimaryKeys": ..., "patch": [...]}``).
    """
    aspect = getattr(wu.metadata, "aspect", None)
    if aspect is None or not hasattr(aspect, "value"):
        return []
    try:
        payload = json.loads(aspect.value)
    except Exception:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("patch", [])
    return []


def _patch_paths(wu: MetadataWorkUnit) -> List[str]:
    return [op.get("path", "") for op in _patch_ops(wu)]


# Test Fixtures


@pytest.fixture
def base_config() -> Dict[str, Any]:
    """Base config; uses ``marketplace_mode='both'`` so the provider-side
    usage SQL is exercised (it's skipped in pure consumer mode)."""
    return {
        "account_id": "test_account",
        "warehouse": "COMPUTE_WH",
        "role": "test_role",
        "username": "test_user",
        "marketplace": {
            "enabled": True,
            "marketplace_mode": "both",
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
    """Pre-aggregated rows from the bucketed marketplace_listing_access_history query."""
    return [
        {
            "BUCKET_START_TIME": datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
            "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
            "OBJECT_NAME": "DEMO_DATABASE.PUBLIC.CUSTOMERS",
            "OBJECT_DOMAIN": "Table",
            "TOTAL_QUERIES": 2,
            "UNIQUE_ACCOUNTS": 2,
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

    snowsight_url_builder = SnowsightUrlBuilder(
        account_locator="test-account",
        region="aws_us_west_2",
    )

    return SnowflakeMarketplaceHandler(
        config=config,
        report=report,
        connection=connection,
        identifiers=identifiers,
        snowsight_url_builder=snowsight_url_builder,
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

        demo_container_urn = handler.identifiers.gen_database_key(
            "DEMO_DATABASE"
        ).as_urn()
        tag_paths = []
        for wu in wus:
            if (
                _aspect_name(wu) == "globalTags"
                and getattr(wu.metadata, "entityUrn", None) == demo_container_urn
            ):
                tag_paths.extend(_patch_paths(wu))
        assert any("urn:li:tag:Marketplace:Imported" in p for p in tag_paths)
        assert any("urn:li:tag:Marketplace:Provider:INTERNAL" in p for p in tag_paths)

        demo_memory_wus = [
            wu
            for wu in wus
            if _aspect_name(wu) == "institutionalMemory"
            and getattr(wu.metadata, "entityUrn", None) == demo_container_urn
        ]
        assert len(demo_memory_wus) == 1

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

        # DataProductProperties is now emitted as PATCH ops keyed by
        # field path; collect the ACME ops by looking at the single DP urn
        # whose ops include listing_global_name=ACME.DATA.LISTING.
        dp_patch_ops_by_urn: Dict[str, List[Dict[str, Any]]] = {}
        for wu in wus:
            if _aspect_name(wu) != "dataProductProperties":
                continue
            entity_urn = cast(str, getattr(wu.metadata, "entityUrn", "") or "")
            if not entity_urn:
                continue
            dp_patch_ops_by_urn.setdefault(entity_urn, []).extend(_patch_ops(wu))

        acme_ops: List[Dict[str, Any]] = []
        for ops in dp_patch_ops_by_urn.values():
            cp_lookup = {
                op["path"]: op["value"]
                for op in ops
                if op.get("op") == "add"
                and op.get("path", "").startswith("/customProperties/")
            }
            if cp_lookup.get("/customProperties/listing_global_name") == (
                "ACME.DATA.LISTING"
            ):
                acme_ops = ops
                break

        assert acme_ops, "Expected DP property patch ops for ACME listing"

        op_by_path = {
            op["path"]: op["value"] for op in acme_ops if op.get("op") == "add"
        }
        assert op_by_path.get("/externalUrl") == (
            "https://app.snowflake.com/marketplace/internal/listing/ACME.DATA.LISTING"
        )
        assert (
            op_by_path.get("/customProperties/documentation_url")
            == "https://docs.acme.example"
        )
        assert (
            op_by_path.get("/customProperties/quickstart_url")
            == "https://quick.acme.example"
        )
        assert (
            op_by_path.get("/customProperties/support_email") == "support@acme.example"
        )
        assert op_by_path.get("/customProperties/support_contact") == "Acme Support"
        assert (
            op_by_path.get("/customProperties/request_approver")
            == "approver@acme.example"
        )

        # Check InstitutionalMemory PATCH for documentation links (marketplace URL is in externalUrl)
        im_patch_ops: List[Dict[str, Any]] = []
        for wu in wus:
            if _aspect_name(wu) == "institutionalMemory":
                im_patch_ops.extend(_patch_ops(wu))
        assert len(im_patch_ops) >= 2  # At least 2 documentation URLs
        urls = [op["value"]["url"] for op in im_patch_ops if op.get("op") == "add"]
        assert "https://docs.acme.example" in urls
        assert "https://quick.acme.example" in urls
        descs = [
            op["value"]["description"] for op in im_patch_ops if op.get("op") == "add"
        ]
        assert all(d == "Documentation" for d in descs)


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

        # Seed a fake purchase since mock_purchases is empty in this test.
        purchase = SnowflakeMarketplacePurchase(
            database_name="IMPORTED_ACME_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment=None,
        )
        handler._marketplace_purchases[purchase.database_name] = purchase

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
        """Unmatched purchases get the Marketplace:Imported tag but no
        listing-link InstitutionalMemory entry."""
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

        unknown_container_urn = handler.identifiers.gen_database_key(
            "UNKNOWN_DB"
        ).as_urn()

        tag_paths: List[str] = []
        for wu in wus:
            if (
                _aspect_name(wu) == "globalTags"
                and getattr(wu.metadata, "entityUrn", None) == unknown_container_urn
            ):
                tag_paths.extend(_patch_paths(wu))
        assert any("urn:li:tag:Marketplace:Imported" in p for p in tag_paths)
        assert not any("urn:li:tag:Marketplace:Provider:" in p for p in tag_paths)

        memory_wus = [
            wu
            for wu in wus
            if _aspect_name(wu) == "institutionalMemory"
            and getattr(wu.metadata, "entityUrn", None) == unknown_container_urn
        ]
        assert len(memory_wus) == 0


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
        """Each row produces one usage workunit on the real table Dataset URN,
        with ``userCounts`` omitted (consumer accounts aren't users). The
        zero-bucket padding wrapper is intentionally dropped to avoid
        contending with the main usage extractor on shared ``(urn, ts, DAY)``
        keys, so only the non-zero rows surface here."""
        handler = create_handler(
            base_config, mock_listings, mock_purchases, mock_usage_events
        )
        wus = list(handler.get_marketplace_workunits())

        non_empty_usage_wus = [
            wu
            for wu in wus
            if isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
            and cast(
                DatasetUsageStatisticsClass,
                wu.metadata.aspect,  # type: ignore[union-attr]
            ).totalSqlQueries
        ]

        assert len(non_empty_usage_wus) == 1
        usage_wu = non_empty_usage_wus[0]

        entity_urn = cast(str, getattr(usage_wu.metadata, "entityUrn", ""))
        assert entity_urn.startswith("urn:li:dataset:")
        assert "demo_database.public.customers" in entity_urn.lower()

        usage_stats = cast(DatasetUsageStatisticsClass, usage_wu.metadata.aspect)  # type: ignore[union-attr]
        assert usage_stats.totalSqlQueries == 2
        assert usage_stats.uniqueUserCount == 2
        assert usage_stats.userCounts is None
        assert usage_stats.timestampMillis == int(
            datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
        )

        assert handler.report.marketplace_usage_events_processed == 2

    def test_usage_statistics_one_row_per_table_per_bucket(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
    ) -> None:
        """One workunit per (bucket, listing, accessed object) row."""
        usage_rows = [
            {
                "BUCKET_START_TIME": datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
                "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
                "OBJECT_NAME": "DEMO_DATABASE.PUBLIC.TABLE1",
                "OBJECT_DOMAIN": "Table",
                "TOTAL_QUERIES": 5,
                "UNIQUE_ACCOUNTS": 3,
            },
            {
                "BUCKET_START_TIME": datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
                "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
                "OBJECT_NAME": "WEATHER_DB.PUBLIC.TABLE2",
                "OBJECT_DOMAIN": "Table",
                "TOTAL_QUERIES": 2,
                "UNIQUE_ACCOUNTS": 1,
            },
        ]

        handler = create_handler(base_config, mock_listings, mock_purchases, usage_rows)
        wus = list(handler.get_marketplace_workunits())

        non_empty_usage_wus = [
            wu
            for wu in wus
            if isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
            and cast(
                DatasetUsageStatisticsClass,
                wu.metadata.aspect,  # type: ignore[union-attr]
            ).totalSqlQueries
        ]

        assert len(non_empty_usage_wus) == 2

    def test_usage_statistics_skips_unknown_listings(
        self,
        base_config: Dict[str, Any],
        mock_purchases: List[Dict[str, Any]],
        mock_usage_events: List[Dict[str, Any]],
    ) -> None:
        """Rows referencing an unknown listing are dropped."""
        config_dict = base_config.copy()

        handler = create_handler(
            config_dict,
            mock_listings=[],
            mock_purchases=mock_purchases,
            mock_usage=mock_usage_events,
        )
        wus = list(handler.get_marketplace_workunits())

        usage_wus = [
            wu
            for wu in wus
            if isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
        ]

        assert len(usage_wus) == 0

    def test_usage_statistics_skips_non_table_objects(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
    ) -> None:
        """Non-table object domains (functions, stages, ...) are ignored."""
        usage_rows = [
            {
                "BUCKET_START_TIME": datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
                "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
                "OBJECT_NAME": "DEMO_DATABASE.PUBLIC.MY_FUNC",
                "OBJECT_DOMAIN": "Function",
                "TOTAL_QUERIES": 5,
                "UNIQUE_ACCOUNTS": 3,
            },
        ]
        handler = create_handler(base_config, mock_listings, mock_purchases, usage_rows)
        wus = list(handler.get_marketplace_workunits())

        usage_wus = [
            wu
            for wu in wus
            if isinstance(
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

        container_tag_wus = [
            wu
            for wu in wus
            if isinstance(getattr(wu.metadata, "aspect", None), GlobalTagsClass)
            and cast(str, getattr(wu.metadata, "entityUrn", "")).startswith(
                "urn:li:container:"
            )
        ]
        assert len(container_tag_wus) == 0

    def test_purchases_only_configuration(
        self, base_config: Dict[str, Any], mock_purchases: List[Dict[str, Any]]
    ) -> None:
        """Test with only purchases enabled."""
        config_dict = base_config.copy()

        handler = create_handler(config_dict, None, mock_purchases, None)
        wus = list(handler.get_marketplace_workunits())

        container_tag_wus = [
            wu
            for wu in wus
            if _aspect_name(wu) == "globalTags"
            and cast(str, getattr(wu.metadata, "entityUrn", "")).startswith(
                "urn:li:container:"
            )
        ]
        assert len(container_tag_wus) == 2

        # No data products
        data_product_urns = _get_data_product_urns(wus)
        assert len(data_product_urns) == 0

    def test_marketplace_time_windows_used(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
    ) -> None:
        """Marketplace usage uses the parent connector's time window."""
        from datetime import datetime, timezone

        custom_start = datetime(2024, 6, 1, tzinfo=timezone.utc)
        custom_end = datetime(2024, 7, 1, tzinfo=timezone.utc)

        bucket_time = datetime(2024, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
        usage_rows = [
            {
                "BUCKET_START_TIME": bucket_time,
                "OBJECT_NAME": "DEMO_DATABASE.PUBLIC.CUSTOMERS",
                "OBJECT_DOMAIN": "Table",
                "TOTAL_QUERIES": 1,
                "UNIQUE_ACCOUNTS": 1,
            },
        ]

        config_dict = base_config.copy()
        config_dict["start_time"] = custom_start.isoformat()
        config_dict["end_time"] = custom_end.isoformat()
        config_dict["bucket_duration"] = "HOUR"

        handler = create_handler(config_dict, mock_listings, mock_purchases, usage_rows)
        wus = list(handler.get_marketplace_workunits())

        non_empty_usage_wus = [
            wu
            for wu in wus
            if isinstance(
                getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass
            )
            and cast(
                DatasetUsageStatisticsClass,
                wu.metadata.aspect,  # type: ignore[union-attr]
            ).totalSqlQueries
        ]

        assert len(non_empty_usage_wus) == 1
        usage_stats = cast(
            DatasetUsageStatisticsClass,
            non_empty_usage_wus[0].metadata.aspect,  # type: ignore[union-attr]
        )

        assert usage_stats.timestampMillis == int(bucket_time.timestamp() * 1000)
        assert usage_stats.eventGranularity is not None
        assert usage_stats.eventGranularity.unit == "HOUR"


# Edge Cases and Error Handling


class TestMarketplaceEdgeCases:
    """Test edge cases and error handling."""

    def test_listing_purchase_matching_logic(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test the business logic of linking purchases to listings via the
        ``marketplace.listing_to_share_overrides`` map."""
        config_with_shares = base_config.copy()
        config_with_shares["shares"] = {
            "ACME_SHARE": {
                "database": "ACME_DATA",
                "platform_instance": None,
                "consumers": [{"database": "IMPORTED_DB", "platform_instance": None}],
            }
        }
        config_with_shares["marketplace"] = {
            **config_with_shares["marketplace"],
            "listing_to_share_overrides": {"ACME.DATA.LISTING": "ACME_SHARE"},
        }

        handler = create_handler(config_with_shares, mock_listings, [])
        handler._load_marketplace_data()
        # Seed the precomputed map with a single fake purchase. Done manually
        # because mock_purchases is empty in this test.
        handler._marketplace_purchases["IMPORTED_DB"] = SnowflakeMarketplacePurchase(
            database_name="IMPORTED_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment=None,
        )

        found_listing = handler._find_listing_for_purchase(
            handler._marketplace_purchases["IMPORTED_DB"]
        )
        assert found_listing == "ACME.DATA.LISTING", (
            "Should use explicit listing_to_share_overrides mapping"
        )

    def test_marketplace_owner_patterns_apply_correctly(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Typed owner inputs (URNs, ``user:``/``group:`` prefixes, emails)
        resolve to ownership URNs; free-form strings are dropped."""
        config_dict = base_config.copy()
        config_dict["marketplace"]["internal_marketplace_owner_patterns"] = {
            "^Acme.*": [
                "acme-team",
                "urn:li:corpGroup:data",
                "user:alice@acme.example",
                "group:data-platform",
            ],
            "Weather": ["weather-team"],
        }

        handler = create_handler(config_dict, mock_listings, [])
        handler._load_marketplace_data()

        acme_listing = handler._marketplace_listings["ACME.DATA.LISTING"]
        owners = handler._resolve_owners_for_listing(acme_listing)

        assert "urn:li:corpGroup:data" in owners
        assert "urn:li:corpuser:alice@acme.example" in owners
        assert "urn:li:corpGroup:data-platform" in owners
        assert "urn:li:corpuser:acme-team" not in owners
        assert len(owners) == 3

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
        assert "snowflake.marketplace.mode" not in property_ids

    def test_structured_property_assignments_use_correct_urns(
        self,
        base_config: Dict[str, Any],
        mock_listings: List[Dict[str, Any]],
        mock_purchases: List[Dict[str, Any]],
    ) -> None:
        """Test that data product structured property assignments emit valid URNs."""
        config_dict = base_config.copy()
        config_dict["marketplace"][
            "marketplace_properties_as_structured_properties"
        ] = True

        handler = create_handler(config_dict, mock_listings, mock_purchases)
        wus = list(handler.get_marketplace_workunits())

        sp_wus = [wu for wu in wus if _aspect_name(wu) == "structuredProperties"]

        assert len(sp_wus) >= 1

        for wu in sp_wus:
            for op in _patch_ops(wu):
                if op.get("op") == "add":
                    prop_urn = op["value"].get("propertyUrn", "")
                    assert prop_urn.startswith("urn:li:structuredProperty:"), prop_urn
                    assert "marketplace.mode" not in prop_urn

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
        """Purchase with no comment still gets a Marketplace tag, no memory link."""
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

        container_urn = handler.identifiers.gen_database_key("NO_COMMENT_DB").as_urn()

        tag_wus = [
            wu
            for wu in wus
            if _aspect_name(wu) == "globalTags"
            and getattr(wu.metadata, "entityUrn", None) == container_urn
        ]
        assert len(tag_wus) == 1

        memory_wus = [
            wu
            for wu in wus
            if _aspect_name(wu) == "institutionalMemory"
            and getattr(wu.metadata, "entityUrn", None) == container_urn
        ]
        assert len(memory_wus) == 0
        assert handler.report.marketplace_enhanced_datasets == 1
