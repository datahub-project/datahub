import json
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.metadata.schema_classes import DataPlatformInstancePropertiesClass


def _default_session_rows(sql: str) -> List[Dict[str, Any]]:
    """Return sensible defaults for the session metadata queries."""
    defaults: Dict[str, List[Dict[str, Any]]] = {
        "CURRENT_VERSION": [{"CURRENT_VERSION()": "8.10.2"}],
        "CURRENT_ROLE": [{"CURRENT_ROLE()": "DATAHUB_ROLE"}],
        "CURRENT_WAREHOUSE": [{"CURRENT_WAREHOUSE()": "COMPUTE_WH"}],
        "CURRENT_ACCOUNT": [{"CURRENT_ACCOUNT()": "XY12345"}],
        "CURRENT_REGION": [{"CURRENT_REGION()": "AWS_US_EAST_1"}],
    }
    for key, rows in defaults.items():
        if key in sql.upper().replace("()", ""):
            return rows
    return []


def _query_side_effect(
    org_name: Optional[str] = "ACME_ORG",
) -> Callable[[str], List[Dict[str, Any]]]:
    def side_effect(sql: str) -> List[Dict[str, Any]]:
        if "CURRENT_ORGANIZATION_NAME" in sql:
            if org_name is None:
                return [{"CURRENT_ORGANIZATION_NAME()": None}]
            return [{"CURRENT_ORGANIZATION_NAME()": org_name}]
        return _default_session_rows(sql)

    return side_effect


def _make_source_and_conn(
    platform_instance: Optional[str] = "instance1",
    include_organization_metadata: bool = True,
    org_name: Optional[str] = "ACME_ORG",
    org_query_fails: bool = False,
    publish_share_database_mapping: bool = True,
) -> Tuple[SnowflakeV2Source, MagicMock]:
    """Create a partially-initialized SnowflakeV2Source for unit testing."""
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance=platform_instance,
        include_organization_metadata=include_organization_metadata,
        publish_share_database_mapping=publish_share_database_mapping,
    )
    report = SnowflakeV2Report()

    source = object.__new__(SnowflakeV2Source)
    source.config = config
    source.report = report
    identifiers = MagicMock()
    identifiers.platform = "snowflake"
    source.identifiers = identifiers

    conn = MagicMock()
    source.connection = conn
    ctx = MagicMock()
    ctx.graph = None
    source.ctx = ctx
    if org_query_fails:

        def query_side_effect(sql: str) -> List[Dict[str, Any]]:
            if "CURRENT_ORGANIZATION_NAME" in sql:
                raise Exception("Unknown function CURRENT_ORGANIZATION_NAME")
            return _default_session_rows(sql)

        conn.query.side_effect = query_side_effect
    else:
        conn.query.side_effect = _query_side_effect(org_name)

    return source, conn


def _run_inspect(source: SnowflakeV2Source, conn: MagicMock) -> None:
    """Run inspect_session_metadata, patching is_standard_edition to avoid SHOW TAGS."""
    with patch.object(source, "is_standard_edition", return_value=True):
        source.inspect_session_metadata(conn)


def test_inspect_session_captures_org_name() -> None:
    source, conn = _make_source_and_conn(org_name="ACME_ORG")
    _run_inspect(source, conn)
    assert source.report.organization_name == "ACME_ORG"


def test_inspect_session_captures_account_locator() -> None:
    source, conn = _make_source_and_conn()
    _run_inspect(source, conn)
    assert source.report.account_locator == "XY12345"
    assert source.report.region == "AWS_US_EAST_1"


def test_inspect_session_org_name_none_when_not_in_org() -> None:
    source, conn = _make_source_and_conn(org_name=None)
    _run_inspect(source, conn)
    assert source.report.organization_name is None


def test_inspect_session_org_name_skipped_when_function_unavailable() -> None:
    source, conn = _make_source_and_conn(org_query_fails=True)
    _run_inspect(source, conn)
    assert source.report.organization_name is None


def test_inspect_session_warns_on_region_failure_when_external_urls_enabled() -> None:
    # Region drives Snowsight URL building; silently dropping URLs leaves the
    # user wondering why external URLs disappeared. include_external_url
    # defaults to True, so the warning should fire.
    config = SnowflakeV2Config(account_id="abc12345", include_external_url=True)
    report = SnowflakeV2Report()
    source = object.__new__(SnowflakeV2Source)
    source.config = config
    source.report = report
    source.identifiers = MagicMock()
    source.connection = MagicMock()
    source.ctx = MagicMock()
    source.ctx.graph = None

    conn = MagicMock()

    def query_side_effect(sql: str) -> List[Dict[str, Any]]:
        if "CURRENT_REGION" in sql:
            raise Exception("permission denied on CURRENT_REGION")
        return _default_session_rows(sql)

    conn.query.side_effect = query_side_effect

    with patch.object(source, "is_standard_edition", return_value=True):
        source.inspect_session_metadata(conn)

    assert source.report.region is None
    assert any(
        "Could not determine Snowflake region" in (w.title or "")
        for w in source.report.warnings
    )


def test_inspect_session_silent_on_region_failure_when_external_urls_disabled() -> None:
    config = SnowflakeV2Config(account_id="abc12345", include_external_url=False)
    report = SnowflakeV2Report()
    source = object.__new__(SnowflakeV2Source)
    source.config = config
    source.report = report
    source.identifiers = MagicMock()
    source.connection = MagicMock()
    source.ctx = MagicMock()
    source.ctx.graph = None

    conn = MagicMock()

    def query_side_effect(sql: str) -> List[Dict[str, Any]]:
        if "CURRENT_REGION" in sql:
            raise Exception("permission denied on CURRENT_REGION")
        return _default_session_rows(sql)

    conn.query.side_effect = query_side_effect

    with patch.object(source, "is_standard_edition", return_value=True):
        source.inspect_session_metadata(conn)

    assert not any(
        "Could not determine Snowflake region" in (w.title or "")
        for w in source.report.warnings
    )


def test_inspect_session_org_name_skipped_when_disabled() -> None:
    source, conn = _make_source_and_conn(
        include_organization_metadata=False, org_name="ACME_ORG"
    )
    _run_inspect(source, conn)
    assert source.report.organization_name is None


def test_platform_instance_workunit_emitted_with_all_properties() -> None:
    source, conn = _make_source_and_conn(platform_instance="instance1")
    _run_inspect(source, conn)

    wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 1
    aspect = wus[0].get_aspect_of_type(DataPlatformInstancePropertiesClass)
    assert aspect is not None
    assert aspect.name == "instance1"
    assert aspect.customProperties["organization_name"] == "ACME_ORG"
    assert aspect.customProperties["account_locator"] == "XY12345"
    assert aspect.customProperties["account_identifier"] == "ACME_ORG.XY12345"


def test_platform_instance_workunit_emitted_without_org_name() -> None:
    """Even without org name, account_locator is still emitted."""
    source, conn = _make_source_and_conn(platform_instance="instance1", org_name=None)
    _run_inspect(source, conn)

    wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 1
    aspect = wus[0].get_aspect_of_type(DataPlatformInstancePropertiesClass)
    assert aspect is not None
    assert aspect.customProperties["account_locator"] == "XY12345"
    assert "organization_name" not in aspect.customProperties
    assert "account_identifier" not in aspect.customProperties


def test_platform_instance_workunit_not_emitted_without_platform_instance() -> None:
    source, _ = _make_source_and_conn(platform_instance=None)
    source.report.organization_name = "ACME_ORG"

    wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 0


# --- publish_share_database_mapping (producer-side) ---
#
# Producers mine ACCOUNT_USAGE.QUERY_HISTORY for `GRANT USAGE ON DATABASE ...
# TO SHARE ...` DDL and publish the resulting share→database mapping on the
# DataPlatformInstance customProperties. Consumers read it from the graph to
# resolve cross-account producer database names.


def _patch_discover(
    return_mapping: Dict[str, str],
) -> Any:
    return patch(
        "datahub.ingestion.source.snowflake.snowflake_shares."
        "SnowflakeSharesHandler.discover_share_database_mapping",
        return_value=return_mapping,
    )


def test_publish_share_database_mapping_sets_custom_property_and_counter() -> None:
    source, conn = _make_source_and_conn(platform_instance="instance1")
    _run_inspect(source, conn)

    mapping = {"ANALYTICS_SHARE": "PROD_ANALYTICS", "RAW_SHARE": "PROD_RAW"}
    with _patch_discover(mapping):
        wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 1
    aspect = wus[0].get_aspect_of_type(DataPlatformInstancePropertiesClass)
    assert aspect is not None
    # Stored as a JSON string with sorted keys so consumer parsing is stable.
    published = json.loads(aspect.customProperties["share_database_mapping"])
    assert published == mapping
    assert source.report.num_share_database_mappings_published == 2


def test_publish_share_database_mapping_skipped_when_empty() -> None:
    source, conn = _make_source_and_conn(platform_instance="instance1")
    _run_inspect(source, conn)

    with _patch_discover({}):
        wus = list(source._gen_platform_instance_workunits())

    aspect = wus[0].get_aspect_of_type(DataPlatformInstancePropertiesClass)
    assert aspect is not None
    assert "share_database_mapping" not in aspect.customProperties
    assert source.report.num_share_database_mappings_published == 0


def test_publish_warns_when_org_name_missing_so_account_identifier_unemitted() -> None:
    # Without org name, account_identifier isn't emitted, so cross-account
    # consumers can't resolve this producer by graph lookup. The warning
    # tells the user to fall back to account_mapping with the bare locator.
    source, conn = _make_source_and_conn(platform_instance="instance1", org_name=None)
    _run_inspect(source, conn)

    with _patch_discover({"X": "Y"}):
        list(source._gen_platform_instance_workunits())

    assert any(
        "Cross-account share lineage may be unreachable" in (w.title or "")
        for w in source.report.warnings
    )


def test_publish_no_warning_when_org_name_present() -> None:
    source, conn = _make_source_and_conn(platform_instance="instance1")
    _run_inspect(source, conn)

    with _patch_discover({"X": "Y"}):
        list(source._gen_platform_instance_workunits())

    assert not any(
        "Cross-account share lineage may be unreachable" in (w.title or "")
        for w in source.report.warnings
    )


def test_publish_share_database_mapping_skipped_when_disabled() -> None:
    source, conn = _make_source_and_conn(
        platform_instance="instance1", publish_share_database_mapping=False
    )
    _run_inspect(source, conn)

    # The handler must not be invoked at all when the flag is off.
    with _patch_discover({"X": "Y"}) as mock_discover:
        wus = list(source._gen_platform_instance_workunits())

    mock_discover.assert_not_called()
    aspect = wus[0].get_aspect_of_type(DataPlatformInstancePropertiesClass)
    assert aspect is not None
    assert "share_database_mapping" not in aspect.customProperties
    assert source.report.num_share_database_mappings_published == 0
