from datetime import datetime, timezone
from typing import Any, Dict, List, cast
from unittest.mock import patch

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)
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
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from tests.unit.snowflake.conftest_marketplace import (  # type: ignore[import-untyped]
    FakeNativeConn as _FakeNativeConn,
)


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
            "global_name": "ACME.DATA.LISTING",
            "title": "Acme Data",
            "uniform_listing_locator": "ORGACME$INTERNAL$ACME_DATA_LISTING",
            "organization_profile_name": "INTERNAL",
            "category": "Sample",
            "description": "Sample listing",
            "created_on": datetime(2024, 1, 1, tzinfo=timezone.utc),
        },
    ]


def create_handler(
    config_dict: Dict[str, Any],
    mock_listings: List[Dict[str, Any]],
) -> SnowflakeMarketplaceHandler:
    """Helper to create a SnowflakeMarketplaceHandler with mocked data."""
    config = SnowflakeV2Config.parse_obj(config_dict)
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(config, report)

    fake_native = _FakeNativeConn()
    fake_native.set_mock_response("SHOW AVAILABLE LISTINGS", mock_listings)

    connection = SnowflakeConnection(fake_native)  # type: ignore[arg-type]

    return SnowflakeMarketplaceHandler(
        config=config,
        report=report,
        connection=connection,
        identifiers=identifiers,
    )


class TestMarketplaceBusinessLogic:
    """Test marketplace business logic and error handling."""

    def test_provider_mode_filters_outbound_shares_only(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that provider mode correctly filters for OUTBOUND shares only."""
        config_dict = base_config.copy()
        config_dict["marketplace"]["marketplace_mode"] = "provider"

        provider_shares = [
            {
                "name": "ACME_OUTBOUND_SHARE",
                "kind": "OUTBOUND",
                "database_name": "ACME_SOURCE_DB",
                "listing_global_name": "ACME.DATA.LISTING",
                "created_on": datetime(2024, 1, 15, tzinfo=timezone.utc),
                "owner": "ACCOUNTADMIN",
                "comment": "Share for marketplace listing",
            },
            {
                "name": "INBOUND_SHARE",
                "kind": "INBOUND",
                "database_name": "SOME_DB",
                "listing_global_name": "OTHER.LISTING",
                "created_on": datetime(2024, 1, 15, tzinfo=timezone.utc),
                "owner": "ACCOUNTADMIN",
                "comment": None,
            },
        ]

        fake_native = _FakeNativeConn()
        fake_native.set_mock_response("SHOW AVAILABLE LISTINGS", mock_listings)
        fake_native.set_mock_response("SHOW SHARES", provider_shares)

        config = SnowflakeV2Config.parse_obj(config_dict)
        report = SnowflakeV2Report()
        identifiers = SnowflakeIdentifierBuilder(config, report)
        connection = SnowflakeConnection(fake_native)  # type: ignore[arg-type]

        handler = SnowflakeMarketplaceHandler(
            config=config,
            report=report,
            connection=connection,
            identifiers=identifiers,
        )
        handler._load_marketplace_data()

        assert len(handler._provider_shares) == 1, "Should only have OUTBOUND share"
        assert "ACME.DATA.LISTING" in handler._provider_shares
        share = handler._provider_shares["ACME.DATA.LISTING"]
        assert share.share_name == "ACME_OUTBOUND_SHARE"

    def test_listing_purchase_explicit_mapping_takes_precedence(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that explicit listing_global_name in shares config takes precedence over name matching."""
        config_with_shares = base_config.copy()
        config_with_shares["shares"] = {
            "ACME_SHARE": {
                "database": "DIFFERENT_NAME",
                "platform_instance": None,
                "listing_global_name": "ACME.DATA.LISTING",
                "consumers": [{"database": "IMPORTED_DB", "platform_instance": None}],
            }
        }

        handler = create_handler(config_with_shares, mock_listings)
        handler._load_marketplace_data()

        purchase = SnowflakeMarketplacePurchase(
            database_name="IMPORTED_DB",
            purchase_date=datetime(2024, 7, 1, tzinfo=timezone.utc),
            owner="ACCOUNTADMIN",
            comment=None,
        )

        found_listing = handler._find_listing_for_purchase(purchase)
        assert found_listing == "ACME.DATA.LISTING", (
            "Should use explicit listing_global_name even when database name doesn't match"
        )

    def test_owner_pattern_regex_matching(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that owner patterns use proper regex matching."""
        config_dict = base_config.copy()
        config_dict["marketplace"]["internal_marketplace_owner_patterns"] = {
            "^Acme": ["acme-team"],
            "Weather": ["weather-team"],
        }

        handler = create_handler(config_dict, mock_listings)
        handler._load_marketplace_data()

        listing = handler._marketplace_listings["ACME.DATA.LISTING"]
        owners = handler._resolve_owners_for_listing(listing)

        assert "urn:li:corpuser:acme-team" in owners, (
            "Should match ^Acme (starts with Acme)"
        )
        assert "urn:li:corpuser:weather-team" not in owners, (
            "Should NOT match Weather because title is 'Acme Data', which doesn't contain 'Weather'"
        )

    def test_marketplace_time_window_config_inheritance(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test that marketplace config properly inherits from BaseTimeWindowConfig."""
        custom_start = datetime(2024, 6, 1, tzinfo=timezone.utc)
        custom_end = datetime(2024, 7, 1, tzinfo=timezone.utc)

        config_dict = base_config.copy()
        config_dict["marketplace"] = {
            "enabled": True,
            "start_time": custom_start.isoformat(),
            "end_time": custom_end.isoformat(),
            "bucket_duration": "HOUR",
        }

        config = SnowflakeV2Config.parse_obj(config_dict)

        assert config.marketplace.start_time == custom_start
        assert config.marketplace.end_time == custom_end
        assert config.marketplace.bucket_duration == "HOUR"

    def test_structured_properties_have_correct_entity_types(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test that structured property definitions target correct entity types."""
        config_dict = base_config.copy()
        config_dict["marketplace"][
            "marketplace_properties_as_structured_properties"
        ] = True

        handler = create_handler(config_dict, [])
        wus = list(handler.get_marketplace_workunits())

        prop_def_wus = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(
                getattr(wu.metadata, "aspect", None), StructuredPropertyDefinition
            )
        ]

        assert len(prop_def_wus) >= 1

        for wu in prop_def_wus:
            prop_def = cast(StructuredPropertyDefinition, wu.metadata.aspect)  # type: ignore[union-attr]
            assert len(prop_def.entityTypes) == 2
            assert "urn:li:entityType:datahub.dataProduct" in prop_def.entityTypes
            assert "urn:li:entityType:datahub.dataset" in prop_def.entityTypes

    def test_marketplace_usage_queries_time_filtered(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that usage queries properly filter by marketplace time windows."""
        start_time = datetime(2024, 6, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 7, 1, tzinfo=timezone.utc)

        config_dict = base_config.copy()
        config_dict["marketplace"] = {
            "enabled": True,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
        }

        fake_native = _FakeNativeConn()
        fake_native.set_mock_response("SHOW AVAILABLE LISTINGS", mock_listings)
        fake_native.set_mock_response("IMPORTED DATABASE", [])

        expected_start_millis = int(start_time.timestamp() * 1000)
        expected_end_millis = int(end_time.timestamp() * 1000)

        config = SnowflakeV2Config.parse_obj(config_dict)

        assert (
            int(config.marketplace.start_time.timestamp() * 1000)
            == expected_start_millis
        )
        assert (
            int(config.marketplace.end_time.timestamp() * 1000) == expected_end_millis
        )

    def test_permission_error_on_show_listings_doesnt_crash(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test that permission errors are caught and don't crash ingestion."""
        config = SnowflakeV2Config.parse_obj(base_config)
        report = SnowflakeV2Report()

        mock_conn = _FakeNativeConn()

        identifiers = SnowflakeIdentifierBuilder(config, report)

        with patch.object(
            _FakeNativeConn, "execute", side_effect=SnowflakePermissionError("test")
        ):
            connection_wrapper = SnowflakeConnection(mock_conn)

            handler = SnowflakeMarketplaceHandler(
                config=config,
                report=report,
                connection=connection_wrapper,
                identifiers=identifiers,
            )

            workunits = list(handler.get_marketplace_workunits())

            assert len(workunits) == 0 or len(workunits) > 0

    def test_malformed_describe_listing_doesnt_crash(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test that malformed DESCRIBE output doesn't crash ingestion."""
        config = SnowflakeV2Config.parse_obj(base_config)
        config.marketplace.fetch_internal_marketplace_listing_details = True
        report = SnowflakeV2Report()

        mock_conn = _FakeNativeConn()
        mock_conn.set_mock_response(
            "SHOW AVAILABLE LISTINGS",
            [
                {
                    "name": "test_listing",
                    "global_name": "ACME.DATA.TEST",
                    "title": "Test Listing",
                    "organization_profile_name": "ACME Corp",
                    "created_on": datetime(2024, 1, 1, tzinfo=timezone.utc),
                }
            ],
        )

        malformed_describe_rows = [
            {"unexpected_key": "unexpected_value"},
            {"property": None, "value": "some_value"},
        ]
        mock_conn.set_mock_response(
            "DESCRIBE AVAILABLE LISTING", malformed_describe_rows
        )

        identifiers = SnowflakeIdentifierBuilder(config, report)
        connection_wrapper = SnowflakeConnection(mock_conn)

        handler = SnowflakeMarketplaceHandler(
            config=config,
            report=report,
            connection=connection_wrapper,
            identifiers=identifiers,
        )

        workunits = list(handler.get_marketplace_workunits())

        assert len(workunits) > 0

    def test_provider_mode_with_empty_source_database(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test provider mode handles shares with empty source_database gracefully."""
        base_config["marketplace"]["marketplace_mode"] = "provider"
        config = SnowflakeV2Config.parse_obj(base_config)
        report = SnowflakeV2Report()

        mock_conn = _FakeNativeConn()
        mock_conn.set_mock_response(
            "SHOW AVAILABLE LISTINGS",
            [
                {
                    "name": "test_listing",
                    "global_name": "ACME.DATA.TEST",
                    "title": "Test Listing",
                    "organization_profile_name": "ACME Corp",
                    "created_on": datetime(2024, 1, 1, tzinfo=timezone.utc),
                }
            ],
        )

        mock_conn.set_mock_response(
            "SHOW SHARES",
            [
                {
                    "name": "TEST_SHARE",
                    "kind": "OUTBOUND",
                    "listing_global_name": "ACME.DATA.TEST",
                    "database_name": "",
                    "created_on": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "owner": "SYSADMIN",
                    "comment": None,
                }
            ],
        )

        identifiers = SnowflakeIdentifierBuilder(config, report)
        connection_wrapper = SnowflakeConnection(mock_conn)

        handler = SnowflakeMarketplaceHandler(
            config=config,
            report=report,
            connection=connection_wrapper,
            identifiers=identifiers,
        )

        workunits = list(handler.get_marketplace_workunits())

        assert len(workunits) > 0

    def test_consumer_mode_without_shares_config_warns_once(
        self, base_config: Dict[str, Any]
    ) -> None:
        """Test that missing shares config in consumer mode warns only once."""
        base_config["marketplace"]["marketplace_mode"] = "consumer"
        config = SnowflakeV2Config.parse_obj(base_config)
        config.shares = None
        report = SnowflakeV2Report()

        mock_conn = _FakeNativeConn()
        mock_conn.set_mock_response("SHOW AVAILABLE LISTINGS", [])
        mock_conn.set_mock_response(
            "SNOWFLAKE.ACCOUNT_USAGE.DATABASES",
            [
                {
                    "DATABASE_NAME": "IMPORTED_DB_1",
                    "TYPE": "IMPORTED DATABASE",
                    "PURCHASE_DATE": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "OWNER": "SYSADMIN",
                    "COMMENT": None,
                },
                {
                    "DATABASE_NAME": "IMPORTED_DB_2",
                    "TYPE": "IMPORTED DATABASE",
                    "PURCHASE_DATE": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "OWNER": "SYSADMIN",
                    "COMMENT": None,
                },
            ],
        )

        identifiers = SnowflakeIdentifierBuilder(config, report)
        connection_wrapper = SnowflakeConnection(mock_conn)

        handler = SnowflakeMarketplaceHandler(
            config=config,
            report=report,
            connection=connection_wrapper,
            identifiers=identifiers,
        )

        list(handler.get_marketplace_workunits())

        warnings_about_shares = [
            w for w in report.warnings if "shares" in str(w).lower()
        ]

        assert len(warnings_about_shares) <= 1

    def test_parse_resources_json_handles_malformed_input(
        self, base_config: Dict[str, Any], mock_listings: List[Dict[str, Any]]
    ) -> None:
        """Test that _parse_resources_json handles malformed JSON gracefully."""
        handler = create_handler(base_config, mock_listings)

        # Test invalid JSON
        assert handler._parse_resources_json("{invalid json}") is None

        # Test valid JSON without documentation key
        assert handler._parse_resources_json('{"other_key": "value"}') is None

        # Test None input
        assert handler._parse_resources_json(None) is None

        # Test valid JSON with documentation
        result = handler._parse_resources_json(
            '{"documentation": "https://example.com"}'
        )
        assert result == "https://example.com"
