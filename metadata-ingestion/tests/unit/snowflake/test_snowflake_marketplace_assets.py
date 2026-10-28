"""Asset collection and usage edge cases for SnowflakeMarketplaceHandler.

Data products for marketplace listings get their assets either from DESC SHARE
(provider side) or from enumerating an imported database (consumer side, and the
fallback when DESC SHARE is not permitted). These tests pin down the fallback
rules, the name validation, and the usage rows that are silently dropped.
"""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Union
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_marketplace import (
    SnowflakeMarketplaceHandler,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeMarketplaceListing,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)

QueryResult = Union[List[dict], Exception]
Dispatch = Callable[[str], QueryResult]
LISTING = "ORG.DATA.LISTING"


def _as_mock(obj: object) -> MagicMock:
    assert isinstance(obj, MagicMock)
    return obj


def _make_handler(
    dispatch: Dispatch,
    redundant_run_skip_handler: Optional[MagicMock] = None,
    **marketplace_overrides: object,
) -> SnowflakeMarketplaceHandler:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "table_pattern": {"deny": [".*\\.denied_.*"]},
            "database_pattern": {"deny": ["DENIED_DB"]},
            "marketplace": {"enabled": True, **marketplace_overrides},
        }
    )
    report = SnowflakeV2Report()
    connection = MagicMock()

    def query(sql: str) -> List[dict]:
        result = dispatch(sql)
        if isinstance(result, Exception):
            raise result
        return list(result)

    connection.query.side_effect = query
    return SnowflakeMarketplaceHandler(
        config=config,
        report=report,
        connection=connection,
        identifiers=SnowflakeIdentifierBuilder(config, report),
        snowsight_url_builder=None,
        redundant_run_skip_handler=redundant_run_skip_handler,
    )


def _listing(org: str = "ACME") -> SnowflakeMarketplaceListing:
    return SnowflakeMarketplaceListing(
        name="Listing",
        listing_global_name=LISTING,
        title="Listing",
        category=None,
        description=None,
        created_on=datetime(2024, 1, 1, tzinfo=timezone.utc),
        organization_profile_name=org,
    )


def _urn(handler: SnowflakeMarketplaceHandler, name: str) -> str:
    return handler.identifiers.gen_dataset_urn(name.lower())


def test_share_assets_keep_only_allowed_three_part_table_names() -> None:
    rows = [
        {"kind": "TABLE", "name": "DB.S.ORDERS"},
        {"kind": "VIEW", "name": "DB.S.V_ORDERS"},
        {"kind": "TABLE", "name": "DB.S.DENIED_SECRET"},
        {"kind": "TABLE", "name": "DB.ORDERS"},  # not fully qualified
        {"kind": "DATABASE", "name": "DB"},
    ]
    handler = _make_handler(lambda _sql: rows)

    assets = handler._collect_assets_from_share("MY_SHARE", LISTING)

    assert assets == [_urn(handler, "DB.S.ORDERS"), _urn(handler, "DB.S.V_ORDERS")]


def test_desc_share_permission_error_falls_back_to_database_enumeration() -> None:
    def dispatch(sql: str) -> QueryResult:
        if sql.startswith("DESC SHARE"):
            return SnowflakePermissionError("not authorized")
        return [{"SCHEMA_NAME": "S", "TABLE_NAME": "ORDERS"}]

    handler = _make_handler(dispatch)

    assets = handler._collect_assets_from_share(
        "MY_SHARE", LISTING, source_database="SRC_DB", source_database_schemas=["S"]
    )

    assert assets == [_urn(handler, "SRC_DB.S.ORDERS")]
    assert [i.title for i in handler.report.infos] == [
        "DESC SHARE permission denied — falling back to database enumeration"
    ]


def test_desc_share_failures_without_fallback_only_warn() -> None:
    denied = _make_handler(lambda _sql: SnowflakePermissionError("not authorized"))
    assert denied._collect_assets_from_share("MY_SHARE", LISTING) == []
    assert [w.title for w in denied.report.warnings] == [
        "Failed to describe provider share"
    ]

    broken = _make_handler(lambda _sql: RuntimeError("boom"))
    assert broken._collect_assets_from_share("MY_SHARE", LISTING) == []
    assert [w.title for w in broken.report.warnings] == [
        "Failed to parse share description"
    ]


def test_malformed_share_and_database_names_are_never_interpolated_into_sql() -> None:
    handler = _make_handler(lambda _sql: [])

    assert handler._collect_assets_from_share("bad name; drop", LISTING) == []
    assert handler._collect_assets_from_database("bad name; drop", LISTING) == []
    _as_mock(handler.connection).query.assert_not_called()
    assert len(handler.report.warnings) == 2


def test_database_enumeration_respects_patterns_and_reports_failures() -> None:
    handler = _make_handler(lambda _sql: [])
    assert handler._collect_assets_from_database("DENIED_DB", LISTING) == []
    assert handler._collect_assets_from_database("", LISTING) == []
    _as_mock(handler.connection).query.assert_not_called()

    denied = _make_handler(lambda _sql: SnowflakePermissionError("not authorized"))
    assert denied._collect_assets_from_database("DB", LISTING) == []
    broken = _make_handler(lambda _sql: RuntimeError("boom"))
    assert broken._collect_assets_from_database("DB", LISTING) == []
    assert [w.title for w in denied.report.warnings] == [
        "Failed to query imported database tables"
    ]
    assert [w.title for w in broken.report.warnings] == [
        "Failed to parse database tables"
    ]


def test_organization_domain_mapping_accepts_urns_names_and_registry_lookups() -> None:
    handler = _make_handler(
        lambda _sql: [],
        organization_to_domain={
            "ACME": "urn:li:domain:acme",
            "BYNAME": "finance",
            "REGISTERED": "ops",
        },
    )

    assert handler._resolve_domain_for_listing(_listing("ACME")) == "urn:li:domain:acme"
    assert (
        handler._resolve_domain_for_listing(_listing("BYNAME"))
        == "urn:li:domain:finance"
    )
    assert handler._resolve_domain_for_listing(_listing("UNMAPPED")) is None
    assert handler._resolve_domain_for_listing(_listing("")) is None

    handler.domain_registry = MagicMock()
    handler.domain_registry.get_domain_urn.return_value = "urn:li:domain:resolved"
    assert (
        handler._resolve_domain_for_listing(_listing("REGISTERED"))
        == "urn:li:domain:resolved"
    )


def test_invalid_owner_regex_is_reported_and_skipped() -> None:
    handler = _make_handler(
        lambda _sql: [],
        internal_marketplace_owner_patterns={
            "[unclosed": ["user:bad"],
            "Listing": ["user:good", "Data Governance Team"],
        },
    )

    owners = handler._resolve_owners_for_listing(_listing())

    assert owners == ["urn:li:corpuser:good"]
    assert [w.title for w in handler.report.warnings] == [
        "Invalid marketplace owner regex"
    ]


def test_usage_is_skipped_for_a_redundant_window() -> None:
    skip_handler = MagicMock()
    skip_handler.should_skip_this_run.return_value = True
    handler = _make_handler(lambda _sql: [], redundant_run_skip_handler=skip_handler)

    assert list(handler._create_marketplace_usage_statistics()) == []
    _as_mock(handler.connection).query.assert_not_called()
    assert len(handler.report.warnings) == 1


def test_usage_permission_error_is_a_warning() -> None:
    handler = _make_handler(lambda _sql: SnowflakePermissionError("not authorized"))
    handler._marketplace_listings[LISTING] = _listing()

    assert list(handler._create_marketplace_usage_statistics()) == []
    assert len(handler.report.warnings) == 1


def test_usage_rows_that_cannot_be_attributed_are_dropped() -> None:
    handler = _make_handler(lambda _sql: [])
    bucket = datetime(2024, 6, 1, tzinfo=timezone.utc)

    def row(**overrides: Any) -> Dict[str, Any]:
        base: Dict[str, Any] = {
            "OBJECT_DOMAIN": "Table",
            "OBJECT_NAME": "DB.S.ORDERS",
            "BUCKET_START_TIME": bucket,
            "TOTAL_QUERIES": 3,
            "UNIQUE_ACCOUNTS": 2,
        }
        base.update(overrides)
        return base

    assert handler._build_marketplace_usage_workunit(row()) is not None
    assert handler._build_marketplace_usage_workunit(row(OBJECT_DOMAIN="Stage")) is None
    assert handler._build_marketplace_usage_workunit(row(OBJECT_NAME=None)) is None
    assert (
        handler._build_marketplace_usage_workunit(row(OBJECT_NAME="DB.ORDERS")) is None
    )
    assert (
        handler._build_marketplace_usage_workunit(row(OBJECT_NAME="DB.S.DENIED_X"))
        is None
    )
    assert (
        handler._build_marketplace_usage_workunit(row(BUCKET_START_TIME=None)) is None
    )
    assert handler.report.warnings == []

    # A value that cannot be coerced is reported rather than aborting the stage.
    assert handler._build_marketplace_usage_workunit(row(TOTAL_QUERIES="many")) is None
    assert [w.title for w in handler.report.warnings] == [
        "Failed to parse marketplace usage row"
    ]
