from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.metadata.schema_classes import DataPlatformInstancePropertiesClass


def _default_session_rows(sql: str) -> List[Dict[str, str]]:
    """Return sensible defaults for the session metadata queries."""
    defaults: Dict[str, List[Dict[str, str]]] = {
        "CURRENT_VERSION": [{"CURRENT_VERSION()": "8.10.2"}],
        "CURRENT_ROLE": [{"CURRENT_ROLE()": "DATAHUB_ROLE"}],
        "CURRENT_WAREHOUSE": [{"CURRENT_WAREHOUSE()": "COMPUTE_WH"}],
    }
    for key, rows in defaults.items():
        if key in sql.upper():
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
) -> Tuple[SnowflakeV2Source, MagicMock]:
    """Create a partially-initialized SnowflakeV2Source for unit testing.

    Uses object.__new__ to avoid the full __init__ which requires a PipelineContext
    and real Snowflake connection. Only sets the attributes needed by
    inspect_session_metadata and _gen_platform_instance_workunits.
    """
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance=platform_instance,
        include_organization_metadata=include_organization_metadata,
    )
    report = SnowflakeV2Report()

    source = object.__new__(SnowflakeV2Source)
    source.config = config
    source.report = report
    identifiers = MagicMock()
    identifiers.platform = "snowflake"
    source.identifiers = identifiers

    conn = MagicMock()
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


def test_inspect_session_org_name_none_when_not_in_org() -> None:
    source, conn = _make_source_and_conn(org_name=None)
    _run_inspect(source, conn)
    assert source.report.organization_name is None


def test_inspect_session_org_name_skipped_when_function_unavailable() -> None:
    source, conn = _make_source_and_conn(org_query_fails=True)
    _run_inspect(source, conn)
    assert source.report.organization_name is None


def test_inspect_session_org_name_skipped_when_disabled() -> None:
    source, conn = _make_source_and_conn(
        include_organization_metadata=False, org_name="ACME_ORG"
    )
    _run_inspect(source, conn)
    assert source.report.organization_name is None


def test_platform_instance_workunit_emitted() -> None:
    source, _ = _make_source_and_conn(platform_instance="instance1")
    source.report.organization_name = "ACME_ORG"

    wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 1
    aspect = wus[0].get_aspect_of_type(DataPlatformInstancePropertiesClass)
    assert aspect is not None
    assert aspect.name == "instance1"
    assert aspect.customProperties["organization_name"] == "ACME_ORG"
    assert "dataPlatformInstance" in wus[0].get_urn()


def test_platform_instance_workunit_not_emitted_without_platform_instance() -> None:
    source, _ = _make_source_and_conn(platform_instance=None)
    source.report.organization_name = "ACME_ORG"

    wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 0


def test_platform_instance_workunit_not_emitted_without_org_name() -> None:
    source, _ = _make_source_and_conn(platform_instance="instance1")
    source.report.organization_name = None

    wus = list(source._gen_platform_instance_workunits())

    assert len(wus) == 0
