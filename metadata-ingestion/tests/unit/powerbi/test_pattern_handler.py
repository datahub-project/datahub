"""Unit tests for ``powerbi.m_query.pattern_handler`` Oracle-related helpers.

These tests live under ``tests/unit/`` (not ``tests/integration/``) so they are
picked up by ``./gradlew :metadata-ingestion:testQuick`` and contribute to the
codecov patch-coverage gate. They mock external collaborators rather than
exercising the full ``parser.get_upstream_tables`` pipeline; the corresponding
end-to-end coverage lives in ``tests/integration/powerbi/test_m_parser.py``.
"""

from contextlib import AbstractContextManager
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import sqlglot.errors

from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    DataPlatformPair,
    OraclePlatformDetail,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBIPlatformDetail,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    ResolvePlatformInstanceFromServerToPlatformInstance,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
    DataPlatformTable,
    IdentifierAccessor,
    Lineage,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    NativeQueryLineage,
    OdbcLineage,
    OracleLineage,
    _get_data_source_tokens,
    _remap_column_lineage_to_pbi_fields,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    Table,
)
from datahub.metadata.schema_classes import StringTypeClass
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)


def _build_config(**overrides: object) -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig.model_validate(
        {
            "tenant_id": "fake",
            "client_id": "foo",
            "client_secret": "bar",
            "enable_advance_lineage_sql_construct": False,
            "extract_column_level_lineage": False,
            **overrides,
        }
    )


def _build_oracle_lineage(
    *,
    config: PowerBiDashboardSourceConfig,
    columns: Optional[List[Column]] = None,
) -> OracleLineage:
    """Construct an OracleLineage with a real config + reporter and mocked
    PipelineContext + platform_instance_resolver. Tests that need to control
    the resolver replace ``platform_instance_resolver`` after construction."""
    table = Table(
        columns=columns,
        measures=[],
        expression="",
        name="t",
        full_name="ds.t",
    )
    return OracleLineage(
        ctx=MagicMock(spec=PipelineContext),
        table=table,
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=MagicMock(spec=AbstractDataPlatformInstanceResolver),
    )


def _build_native_query_lineage(
    *, config: PowerBiDashboardSourceConfig
) -> NativeQueryLineage:
    table = Table(
        columns=None,
        measures=[],
        expression="",
        name="t",
        full_name="ds.t",
    )
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None
    return NativeQueryLineage(
        ctx=ctx,
        table=table,
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=MagicMock(spec=AbstractDataPlatformInstanceResolver),
    )


def _snowflake_native_query_source_node(
    name_value: Optional[dict] = None,
) -> dict:
    """Structurally faithful (tokenRange/position noise stripped) AST fragment
    for ``Snowflake.Databases(SnowflakeURL,SnowflakeWarehouse){[Name=SnowflakeDBLake]}[Data]``.
    ``SnowflakeDBLake`` is an unquoted identifier (a Power Query dataset
    Parameter reference), not a literal string, unless ``name_value`` is given.
    """
    if name_value is None:
        name_value = {
            "kind": "IdentifierExpression",
            "identifier": {
                "kind": "Identifier",
                "literal": "SnowflakeDBLake",
            },
        }
    return {
        "kind": "RecursivePrimaryExpression",
        "head": {
            "kind": "IdentifierExpression",
            "identifier": {"kind": "Identifier", "literal": "Snowflake.Databases"},
        },
        "recursiveExpressions": {
            "kind": "ArrayWrapper",
            "elements": [
                {
                    "kind": "InvokeExpression",
                    "content": {
                        "kind": "ArrayWrapper",
                        "elements": [
                            {
                                "kind": "Csv",
                                "node": {
                                    "kind": "IdentifierExpression",
                                    "identifier": {
                                        "kind": "Identifier",
                                        "literal": "SnowflakeURL",
                                    },
                                },
                            },
                            {
                                "kind": "Csv",
                                "node": {
                                    "kind": "IdentifierExpression",
                                    "identifier": {
                                        "kind": "Identifier",
                                        "literal": "SnowflakeWarehouse",
                                    },
                                },
                            },
                        ],
                    },
                },
                {
                    "kind": "ItemAccessExpression",
                    "content": {
                        "kind": "RecordExpression",
                        "content": {
                            "kind": "ArrayWrapper",
                            "elements": [
                                {
                                    "kind": "Csv",
                                    "node": {
                                        "kind": "GeneralizedIdentifierPairedExpression",
                                        "key": {
                                            "kind": "GeneralizedIdentifier",
                                            "literal": "Name",
                                        },
                                        "value": name_value,
                                    },
                                },
                            ],
                        },
                    },
                },
                {"kind": "FieldSelector"},
            ],
        },
    }


_SNOWFLAKE_NATIVE_QUERY_PARAMETERS = {
    "SnowflakeURL": "myserver.snowflakecomputing.com",
    "SnowflakeWarehouse": "MYWH",
    "SnowflakeDBLake": "DATA_LAKE_DEV",
}


# ---------------------------------------------------------------------------
# OracleLineage._get_server_and_db_name
# ---------------------------------------------------------------------------


def test_oracle_get_server_and_db_name_recognizes_all_forms():
    # EZ-Connect: server case is preserved so existing recipe keys keep matching.
    assert OracleLineage._get_server_and_db_name(
        "localhost:1521/salesdb.domain.com"
    ) == ("localhost:1521", "salesdb")
    assert OracleLineage._get_server_and_db_name(
        "LOCALHOST:1521/SALESDB.DOMAIN.COM"
    ) == ("LOCALHOST:1521", "SALESDB")

    # TNS alias / descriptor: lowercased so recipe keys match regardless of
    # M-query casing (Oracle TNS lookup is case-insensitive in the source).
    assert OracleLineage._get_server_and_db_name("EDWPSFN") == ("edwpsfn", None)
    assert OracleLineage._get_server_and_db_name('"EDWPSFN"') == ("edwpsfn", None)
    assert OracleLineage._get_server_and_db_name("MYDB.WORLD") == ("mydb.world", None)
    assert OracleLineage._get_server_and_db_name(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1521))"
        "(CONNECT_DATA=(SERVICE_NAME=mydb.example.com)))"
    ) == ("mydb.example.com", None)

    # Unrecognized forms return (None, None).
    assert OracleLineage._get_server_and_db_name("host:port:SID") == (None, None)
    assert OracleLineage._get_server_and_db_name("") == (None, None)
    assert OracleLineage._get_server_and_db_name("/foo/bar") == (None, None)
    # SID= without SERVICE_NAME= must not be silently treated as a service name.
    assert OracleLineage._get_server_and_db_name(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1521))"
        "(CONNECT_DATA=(SID=mydb)))"
    ) == (None, None)


# ---------------------------------------------------------------------------
# OracleLineage._sql_has_unqualified_tables
# ---------------------------------------------------------------------------


def test_oracle_sql_has_unqualified_tables_treats_unparseable_as_unqualified():
    """If the inline SQL is unparseable, ``_sql_has_unqualified_tables`` returns
    True so the user gets a missing-default_schema warning instead of a silent
    failure — locks in the conservative fallback. We patch sqlglot.parse to raise
    so the test does not depend on which inputs sqlglot's permissive Oracle
    dialect happens to reject."""
    instance = _build_oracle_lineage(config=_build_config())
    with patch(
        "datahub.ingestion.source.powerbi.m_query.pattern_handler.sqlglot.parse",
        side_effect=sqlglot.errors.ParseError("boom"),
    ):
        assert instance._sql_has_unqualified_tables("SELECT 1") is True


def test_oracle_sql_has_unqualified_tables_lets_unexpected_exceptions_propagate():
    """The exception handler is narrowed to (SqlglotError, ValueError,
    AttributeError); a genuinely unexpected error (e.g. RuntimeError) should
    surface, not be silently swallowed under the conservative-True fallback."""
    instance = _build_oracle_lineage(config=_build_config())
    with (
        patch(
            "datahub.ingestion.source.powerbi.m_query.pattern_handler.sqlglot.parse",
            side_effect=RuntimeError("unexpected"),
        ),
        pytest.raises(RuntimeError, match="unexpected"),
    ):
        instance._sql_has_unqualified_tables("SELECT 1")


def test_sql_has_unqualified_tables_true_for_unqualified_sql():
    instance = _build_oracle_lineage(config=_build_config())
    assert instance._sql_has_unqualified_tables("SELECT * FROM EMPLOYEES") is True


def test_sql_has_unqualified_tables_false_for_qualified_sql():
    instance = _build_oracle_lineage(config=_build_config())
    assert (
        instance._sql_has_unqualified_tables(
            "SELECT EMPLOYEE_ID, NAME FROM HR.EMPLOYEES"
        )
        is False
    )


def test_sql_has_unqualified_tables_normalizes_special_chars_and_drop_statements():
    """``#(lf)`` line breaks and leading non-SELECT statements (USE/SET) are
    stripped before parsing. A query with both still detects qualification
    correctly."""
    instance = _build_oracle_lineage(config=_build_config())
    qualified = "USE HR;#(lf)SELECT EMPLOYEE_ID, NAME#(lf)FROM HR.EMPLOYEES"
    assert instance._sql_has_unqualified_tables(qualified) is False
    unqualified = "USE HR;#(lf)SELECT EMPLOYEE_ID, NAME#(lf)FROM EMPLOYEES"
    assert instance._sql_has_unqualified_tables(unqualified) is True


# ---------------------------------------------------------------------------
# OracleLineage._create_lineage_from_query
# ---------------------------------------------------------------------------


def test_create_lineage_from_query_returns_empty_when_advance_lineage_disabled():
    """``enable_advance_lineage_sql_construct=False`` disables the inline-Query
    branch entirely — matches MSSql and NativeQueryLineage behavior. The skip
    is surfaced as a structured ``reporter.info`` so the user investigating
    "no Oracle lineage" sees it in the ingestion report."""
    config = _build_config(enable_advance_lineage_sql_construct=False)
    instance = _build_oracle_lineage(config=config)
    # parse_custom_sql must not be touched in this branch.
    instance.parse_custom_sql = MagicMock(  # type: ignore[method-assign]
        side_effect=AssertionError("parse_custom_sql should not be called")
    )

    result = instance._create_lineage_from_query(
        server="EDWPSFN", query="SELECT * FROM EMPLOYEES"
    )
    assert result == Lineage.empty()

    info_titles = [entry.title for entry in instance.reporter.infos]
    assert any(
        "inline native-query lineage skipped" in (t or "") for t in info_titles
    ), (
        f"Expected info entry about skipped inline native-query lineage; got: {info_titles}"
    )


def test_create_lineage_from_query_warns_when_unqualified_sql_and_no_default_schema():
    """Plain ``PlatformDetail`` (no default_schema) + unqualified SQL must emit
    the 'missing default_schema' structured warning."""
    config = _build_config(enable_advance_lineage_sql_construct=True)
    instance = _build_oracle_lineage(config=config)
    instance.platform_instance_resolver = MagicMock(
        spec=AbstractDataPlatformInstanceResolver
    )
    instance.platform_instance_resolver.get_platform_instance.return_value = (
        PlatformDetail()
    )
    instance.parse_custom_sql = MagicMock(  # type: ignore[method-assign]
        return_value=Lineage.empty()
    )

    instance._create_lineage_from_query(
        server="EDWPSFN", query="SELECT * FROM EMPLOYEES"
    )

    warning_titles = [entry.title for entry in instance.reporter.warnings]
    assert any("default_schema" in (t or "") for t in warning_titles), (
        f"Expected default_schema warning; got: {warning_titles}"
    )
    instance.parse_custom_sql.assert_called_once()
    _, kwargs = instance.parse_custom_sql.call_args
    assert kwargs["database"] is None
    assert kwargs["schema"] is None


def test_create_lineage_from_query_skips_warning_when_sql_is_qualified():
    """Fully-qualified SQL must not produce a default_schema warning even when
    no ``default_schema`` is configured."""
    config = _build_config(enable_advance_lineage_sql_construct=True)
    instance = _build_oracle_lineage(config=config)
    instance.platform_instance_resolver = MagicMock(
        spec=AbstractDataPlatformInstanceResolver
    )
    instance.platform_instance_resolver.get_platform_instance.return_value = (
        PlatformDetail()
    )
    instance.parse_custom_sql = MagicMock(  # type: ignore[method-assign]
        return_value=Lineage.empty()
    )

    instance._create_lineage_from_query(
        server="EDWPSFN",
        query="SELECT EMPLOYEE_ID, NAME FROM HR.EMPLOYEES",
    )

    warning_titles = [entry.title for entry in instance.reporter.warnings]
    assert not any("default_schema" in (t or "") for t in warning_titles), (
        f"No default_schema warning expected; got: {warning_titles}"
    )


def test_create_lineage_from_query_threads_oracle_default_schema():
    """When the resolver returns ``OraclePlatformDetail(default_schema='hr')``,
    ``parse_custom_sql`` must be called with ``schema='hr'`` (and the resolved
    detail threaded through), and its result returned unchanged. The downstream
    column remap now lives inside ``parse_custom_sql`` itself, covered by
    ``test_parse_custom_sql_remaps_downstream_columns_to_pbi_field_casing``."""
    config = _build_config(enable_advance_lineage_sql_construct=True)
    pbi_columns = [
        Column(
            name="EMPLOYEE_ID",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
        )
    ]
    instance = _build_oracle_lineage(config=config, columns=pbi_columns)
    instance.platform_instance_resolver = MagicMock(
        spec=AbstractDataPlatformInstanceResolver
    )
    oracle_detail = OraclePlatformDetail(default_schema="hr")
    instance.platform_instance_resolver.get_platform_instance.return_value = (
        oracle_detail
    )

    parsed_lineage = Lineage(
        upstreams=[
            DataPlatformTable(
                data_platform_pair=instance.get_platform_pair(),
                urn="urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)",
            )
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:powerbi,t,PROD)",
                    column="employee_id",
                    column_type=None,
                    native_column_type="VARCHAR",
                ),
                upstreams=[
                    ColumnRef(
                        table="urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)",
                        column="employee_id",
                    )
                ],
            )
        ],
    )
    instance.parse_custom_sql = MagicMock(  # type: ignore[method-assign]
        return_value=parsed_lineage
    )

    result = instance._create_lineage_from_query(
        server="EDWPSFN",
        query="SELECT EMPLOYEE_ID FROM EMPLOYEES",
    )

    instance.parse_custom_sql.assert_called_once()
    _, kwargs = instance.parse_custom_sql.call_args
    # database=None matches Oracle ingestion's 2-part URN shape.
    assert kwargs["database"] is None
    assert kwargs["schema"] == "hr"
    # The resolved OraclePlatformDetail must be threaded through to
    # parse_custom_sql so the resolver isn't re-run inside it.
    assert kwargs["platform_detail"] is oracle_detail

    # _create_lineage_from_query delegates to parse_custom_sql and returns its
    # result verbatim (the remap happens inside parse_custom_sql, mocked here).
    assert result is parsed_lineage

    # Unqualified-table SQL was parsed, but with default_schema set the
    # missing-default_schema warning must NOT fire.
    warning_titles = [entry.title for entry in instance.reporter.warnings]
    assert not any("default_schema" in (t or "") for t in warning_titles)


def test_parse_custom_sql_remaps_downstream_columns_to_pbi_field_casing():
    """parse_custom_sql (the shared SQL-parsing path used by native-query, ODBC,
    Oracle, and the two/three-step patterns) must remap the parsed downstream
    column from the SQL alias' casing to the PowerBI field's casing, so the
    downstream schemaField URN resolves to a real field."""
    config = _build_config(enable_advance_lineage_sql_construct=True)
    pbi_columns = [
        Column(
            name="EmployeeId",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
        )
    ]
    instance = _build_oracle_lineage(config=config, columns=pbi_columns)

    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)"
    parsed_result = MagicMock()
    parsed_result.in_tables = [upstream_urn]
    parsed_result.debug_info = None
    parsed_result.column_lineage = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(table=None, column="employeeid"),
            upstreams=[ColumnRef(table=upstream_urn, column="employeeid")],
        )
    ]

    with patch(
        "datahub.ingestion.source.powerbi.m_query.pattern_handler."
        "native_sql_parser.parse_custom_sql",
        return_value=parsed_result,
    ):
        result = instance.parse_custom_sql(
            query="SELECT employeeid FROM hr.employees",
            server="server",
            database=None,
            schema="hr",
            platform_detail=PlatformDetail(),
        )

    assert len(result.column_lineage) == 1
    # SQL alias casing ("employeeid") remapped to the PowerBI field ("EmployeeId").
    assert result.column_lineage[0].downstream.column == "EmployeeId"


# ---------------------------------------------------------------------------
# OracleLineage.create_lineage — Oracle.Database routing branches
# ---------------------------------------------------------------------------


def _make_data_access_func_detail(
    *, args: List[Optional[str]], record_fields: Dict[str, str]
) -> DataAccessFunctionDetail:
    """Build a placeholder ``DataAccessFunctionDetail``; routing tests patch
    ``_get_arg_values`` / ``_get_record_args`` to return canned values, so the
    ``arg_list`` and ``node_map`` contents do not need to be realistic."""
    return DataAccessFunctionDetail(
        arg_list={},
        data_access_function_name="Oracle.Database",
        identifier_accessor=None,
        node_map={},
        parameters={},
    )


def test_data_access_func_detail_repr_excludes_parse_tree():
    """DataAccessFunctionDetail is logged at debug and embedded in warning
    contexts on nearly every table. node_map (the full parse tree) and arg_list
    must stay out of its repr or those log lines balloon to GBs / OOM."""
    node_map = {i: {"id": i, "kind": "IdentifierExpression"} for i in range(500)}
    detail = DataAccessFunctionDetail(
        arg_list={"kind": "InvokeExpression", "sentinel_arg": "ARG_SENTINEL"},
        data_access_function_name="Oracle.Database",
        identifier_accessor=None,
        node_map=node_map,
        parameters={},
    )
    rendered = repr(detail)
    assert "Oracle.Database" in rendered
    assert "IdentifierExpression" not in rendered
    assert "ARG_SENTINEL" not in rendered
    # The heavy fields are still accessible; only their repr is suppressed.
    assert detail.node_map is node_map


def _patch_arg_helpers(
    args: List[Optional[str]], record_fields: Dict[str, str]
) -> AbstractContextManager[None]:
    """Context manager that patches both ``_get_arg_values`` and
    ``_get_record_args`` at the pattern_handler module level."""
    return patch.multiple(
        "datahub.ingestion.source.powerbi.m_query.pattern_handler",
        _get_arg_values=MagicMock(return_value=args),
        _get_record_args=MagicMock(return_value=record_fields),
    )


def test_create_lineage_routes_to_query_branch_when_query_record_present():
    """``Oracle.Database(server, [Query="…"])`` must dispatch to
    ``_create_lineage_from_query``, not the accessor-based path."""
    instance = _build_oracle_lineage(config=_build_config())
    instance._create_lineage_from_query = MagicMock(  # type: ignore[method-assign]
        return_value=Lineage.empty()
    )
    detail = _make_data_access_func_detail(
        args=["EDWPSFN"], record_fields={"Query": "SELECT * FROM HR.EMPLOYEES"}
    )

    with _patch_arg_helpers(
        args=["EDWPSFN"], record_fields={"Query": "SELECT * FROM HR.EMPLOYEES"}
    ):
        instance.create_lineage(detail)

    instance._create_lineage_from_query.assert_called_once_with(
        server="edwpsfn", query="SELECT * FROM HR.EMPLOYEES"
    )


def test_create_lineage_returns_empty_when_no_query_and_no_db_name():
    """Bare TNS alias with no ``Query=`` means we cannot reach the accessor
    path (which needs a db_name); the routing must short-circuit to
    ``Lineage.empty()`` rather than crash dereferencing ``identifier_accessor``."""
    instance = _build_oracle_lineage(config=_build_config())
    detail = _make_data_access_func_detail(args=["EDWPSFN"], record_fields={})

    with _patch_arg_helpers(args=["EDWPSFN"], record_fields={}):
        result = instance.create_lineage(detail)

    assert result == Lineage.empty()


def test_create_lineage_returns_empty_when_args_are_empty_or_first_is_none():
    """``Oracle.Database()`` with no args (or ``args[0] is None``) returns
    ``Lineage.empty()`` and does not emit a connection-string warning — the
    warning is only for *unrecognized* string forms, not absent ones."""
    instance = _build_oracle_lineage(config=_build_config())

    # Empty args.
    with _patch_arg_helpers(args=[], record_fields={}):
        assert (
            instance.create_lineage(
                _make_data_access_func_detail(args=[], record_fields={})
            )
            == Lineage.empty()
        )

    # First arg is None (unresolved parameter reference).
    with _patch_arg_helpers(args=[None], record_fields={}):
        assert (
            instance.create_lineage(
                _make_data_access_func_detail(args=[None], record_fields={})
            )
            == Lineage.empty()
        )

    # No "connection string not recognized" warning for these branches.
    warning_titles = [entry.title for entry in instance.reporter.warnings]
    assert not any(
        "connection string not recognized" in (t or "") for t in warning_titles
    ), (
        "Empty/None args[0] should short-circuit silently; the unrecognized-form "
        f"warning is reserved for actual unparseable strings. Got: {warning_titles}"
    )


def test_create_lineage_warns_when_connection_string_unrecognized():
    """An Oracle.Database call whose first argument is a non-empty but
    unparseable string (e.g. ``"host:port:SID"``) must emit a structured
    ``reporter.warning`` so an operator sees it in the ingestion report."""
    instance = _build_oracle_lineage(config=_build_config())

    with _patch_arg_helpers(args=["host:port:SID"], record_fields={}):
        result = instance.create_lineage(
            _make_data_access_func_detail(args=["host:port:SID"], record_fields={})
        )

    assert result == Lineage.empty()
    warning_titles = [entry.title for entry in instance.reporter.warnings]
    assert any(
        "connection string not recognized" in (t or "") for t in warning_titles
    ), f"Expected unrecognized-form warning; got: {warning_titles}"


# ---------------------------------------------------------------------------
# ResolvePlatformInstanceFromServerToPlatformInstance — case-insensitive lookup
# ---------------------------------------------------------------------------


def test_server_to_platform_instance_lookup_is_case_insensitive():
    """A lowercased TNS-alias server name must still match a mixed-case
    ``server_to_platform_instance`` recipe key — Oracle TNS lookup is
    case-insensitive in the source system."""
    config = _build_config(
        server_to_platform_instance={
            "EDWPSFN": {"platform_instance": "prod_oracle"},
        },
    )
    resolver = ResolvePlatformInstanceFromServerToPlatformInstance(config)
    detail = PowerBIPlatformDetail(
        data_platform_pair=DataPlatformPair(
            powerbi_data_platform_name="Oracle", datahub_data_platform_name="oracle"
        ),
        data_platform_server="edwpsfn",
    )
    resolved = resolver.get_platform_instance(detail)
    assert resolved.platform_instance == "prod_oracle"


def test_server_to_platform_instance_rejects_case_insensitive_duplicate_keys():
    """``server_to_platform_instance`` keys must be unique case-insensitively
    because the resolver falls back to a case-insensitive lookup. A recipe
    with both ``EDWPSFN`` and ``edwpsfn`` would silently pick one by dict
    insertion order — the validator forbids that ambiguity at config-load."""
    with pytest.raises(ValueError, match="case-insensitive duplicate keys"):
        _build_config(
            server_to_platform_instance={
                "EDWPSFN": {"platform_instance": "a"},
                "edwpsfn": {"platform_instance": "b"},
            },
        )


def test_server_to_platform_instance_lookup_returns_default_when_server_missing():
    """A server with no matching key (cased or lowercased) falls back to a
    plain ``PlatformDetail()`` — guards against ``data_platform_server=None``
    and unknown servers both routing through the same default branch."""
    config = _build_config(
        server_to_platform_instance={
            "EDWPSFN": {"platform_instance": "prod_oracle"},
        },
    )
    resolver = ResolvePlatformInstanceFromServerToPlatformInstance(config)
    pair = DataPlatformPair(
        powerbi_data_platform_name="Oracle", datahub_data_platform_name="oracle"
    )

    # Empty server: returns default.
    resolved = resolver.get_platform_instance(
        PowerBIPlatformDetail(data_platform_pair=pair, data_platform_server="")
    )
    assert resolved.platform_instance is None

    # Unknown server: returns default.
    resolved = resolver.get_platform_instance(
        PowerBIPlatformDetail(
            data_platform_pair=pair, data_platform_server="other_alias"
        )
    )
    assert resolved.platform_instance is None


# ---------------------------------------------------------------------------
# _remap_column_lineage_to_pbi_fields
# ---------------------------------------------------------------------------


def test_remap_column_lineage_to_pbi_fields_restores_original_case():
    """sqlglot returns downstream column names in the upstream's case (Oracle
    is lowercase). PowerBI fields come back from the API in their original
    case. Without remapping, the downstream schemaField URN is wrong."""
    cll = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:powerbi,t,PROD)",
                column="vendor_id",
                column_type=None,
                native_column_type="VARCHAR",
            ),
            upstreams=[
                ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_vendor,PROD)",
                    column="vendor_id",
                )
            ],
        )
    ]
    pbi_columns = [
        Column(
            name="VENDOR_ID",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
        )
    ]

    remapped = _remap_column_lineage_to_pbi_fields(cll, pbi_columns)
    assert remapped[0].downstream.column == "VENDOR_ID"
    # Upstream column case must be preserved (Oracle stores lowercase).
    assert remapped[0].upstreams[0].column == "vendor_id"


def test_remap_column_lineage_empty_inputs():
    """Edge cases: empty column_lineage and None pbi_columns both return input unchanged."""
    pbi_columns = [
        Column(
            name="VENDOR_ID",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
        )
    ]
    cll = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:powerbi,t,PROD)",
                column="vendor_id",
                column_type=None,
                native_column_type="VARCHAR",
            ),
            upstreams=[
                ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_vendor,PROD)",
                    column="vendor_id",
                )
            ],
        )
    ]

    assert _remap_column_lineage_to_pbi_fields([], pbi_columns) == []
    assert _remap_column_lineage_to_pbi_fields(cll, None) == cll

    # Already-matching case: downstream column name already matches pbi field name.
    matching_cll = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:powerbi,t,PROD)",
                column="VENDOR_ID",
                column_type=None,
                native_column_type="VARCHAR",
            ),
            upstreams=[],
        )
    ]
    remapped = _remap_column_lineage_to_pbi_fields(matching_cll, pbi_columns)
    assert remapped == matching_cll


# ---------------------------------------------------------------------------
# OdbcLineage.expression_lineage — two-tier vs three-tier URN arity
# ---------------------------------------------------------------------------


def _build_odbc_lineage() -> OdbcLineage:
    """OdbcLineage with a column-less table (so create_table_column_lineage is a
    no-op) and a resolver that returns a plain PlatformDetail (no instance)."""
    table = Table(columns=None, measures=[], expression="", name="t", full_name="ds.t")
    resolver = MagicMock(spec=AbstractDataPlatformInstanceResolver)
    resolver.get_platform_instance.return_value = PlatformDetail()
    return OdbcLineage(
        ctx=MagicMock(spec=PipelineContext),
        table=table,
        config=_build_config(),
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=resolver,
    )


def _nav_accessor(*levels: tuple) -> IdentifierAccessor:
    """Build a Database->Schema->Table IdentifierAccessor chain from
    (Kind, Name) tuples, returning the head (first level)."""
    head: Optional[IdentifierAccessor] = None
    for kind, name in reversed(levels):
        head = IdentifierAccessor(
            identifier=name, items={"Kind": kind, "Name": name}, next=head
        )
    assert head is not None
    return head


def test_odbc_two_tier_platform_drops_pseudo_catalog():
    """Hive's ODBC navigation exposes a constant "HIVE" pseudo-catalog above the
    real schema. For two-tier platforms it must be dropped so the upstream URN is
    ``schema.table`` — matching HiveSource (a TwoTierSQLAlchemySource)."""
    instance = _build_odbc_lineage()
    detail = DataAccessFunctionDetail(
        arg_list={},
        data_access_function_name="Odbc.DataSource",
        identifier_accessor=_nav_accessor(
            ("Database", "HIVE"),
            ("Schema", "product_analytics"),
            ("Table", "vg_a1_user_profile"),
        ),
        node_map={},
    )
    pair = DataPlatformPair(
        powerbi_data_platform_name="Hive", datahub_data_platform_name="hive"
    )

    result = instance.expression_lineage(detail, "hive", pair, server_name="dsn")

    assert [u.urn for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:hive,product_analytics.vg_a1_user_profile,PROD)"
    ]


def test_odbc_three_tier_platform_keeps_database():
    """A genuine three-tier platform (BigQuery: project.dataset.table) must keep
    all three navigation levels — the two-tier handling must not regress it."""
    instance = _build_odbc_lineage()
    detail = DataAccessFunctionDetail(
        arg_list={},
        data_access_function_name="Odbc.DataSource",
        identifier_accessor=_nav_accessor(
            ("Database", "myproject"),
            ("Schema", "mydataset"),
            ("Table", "mytable"),
        ),
        node_map={},
    )
    pair = DataPlatformPair(
        powerbi_data_platform_name="GoogleBigQuery",
        datahub_data_platform_name="bigquery",
    )

    result = instance.expression_lineage(detail, "bigquery", pair, server_name="dsn")

    assert [u.urn for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,myproject.mydataset.mytable,PROD)"
    ]


def test_odbc_two_tier_platform_with_no_schema():
    """A two-tier platform navigated as Database (pseudo-catalog) + Table with no
    Schema level must NOT fall through to ``database.table`` — that pseudo-catalog
    (e.g. "HIVE") is not a real schema and would yield a dangling URN. The lineage
    must be skipped with a warning instead."""
    instance = _build_odbc_lineage()
    detail = DataAccessFunctionDetail(
        arg_list={},
        data_access_function_name="Odbc.DataSource",
        identifier_accessor=_nav_accessor(
            ("Database", "HIVE"),
            ("Table", "vg_a1_user_profile"),
        ),
        node_map={},
    )
    pair = DataPlatformPair(
        powerbi_data_platform_name="Hive", datahub_data_platform_name="hive"
    )

    result = instance.expression_lineage(detail, "hive", pair, server_name="dsn")

    assert result.upstreams == []
    assert any(
        w.title == "Cannot build two-tier ODBC table name"
        for w in instance.reporter.warnings
    )


def test_remap_column_lineage_multi_table_shared_column_name():
    """Two upstream tables sharing a column name (e.g. SETID from both
    PS_COR_CNTRCT_PROJ and PS_COR_CNTRCT_PRIM) must each get the PowerBI
    casing applied to their downstream — guards against mis-collapsing."""
    cll = [
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:powerbi,t,PROD)",
                column="setid",
                column_type=None,
                native_column_type="VARCHAR",
            ),
            upstreams=[
                ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_cor_cntrct_proj,PROD)",
                    column="setid",
                ),
                ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_cor_cntrct_prim,PROD)",
                    column="setid",
                ),
            ],
        ),
        ColumnLineageInfo(
            downstream=DownstreamColumnRef(
                table="urn:li:dataset:(urn:li:dataPlatform:powerbi,t,PROD)",
                column="vendor_id",
                column_type=None,
                native_column_type="VARCHAR",
            ),
            upstreams=[
                ColumnRef(
                    table="urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_vendor,PROD)",
                    column="vendor_id",
                )
            ],
        ),
    ]
    pbi_columns = [
        Column(
            name="SETID",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
        ),
        Column(
            name="VENDOR_ID",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
        ),
    ]

    remapped = _remap_column_lineage_to_pbi_fields(cll, pbi_columns)
    assert [c.downstream.column for c in remapped] == ["SETID", "VENDOR_ID"]
    # Each upstream ColumnRef must keep its original (table, column) — remapping
    # the downstream casing must not collapse, swap, or drop upstream entries.
    setid_upstreams = [(u.table, u.column) for u in remapped[0].upstreams]
    assert setid_upstreams == [
        (
            "urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_cor_cntrct_proj,PROD)",
            "setid",
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:oracle,edwpsfn.ps_cor_cntrct_prim,PROD)",
            "setid",
        ),
    ]


# ---------------------------------------------------------------------------
# OdbcLineage — view leaves and dialect-aware SQL detection
# (reuses the _build_odbc_lineage / _nav_accessor helpers defined above)
# ---------------------------------------------------------------------------


def test_odbc_view_leaf_resolves_like_table():
    # A view leaf uses Kind="View"; without handling it the upstream is dropped.
    instance = _build_odbc_lineage()
    detail = DataAccessFunctionDetail(
        arg_list={},
        data_access_function_name="Odbc.DataSource",
        identifier_accessor=_nav_accessor(
            ("Database", "my_project"),
            ("Schema", "my_dataset"),
            ("View", "my_view"),
        ),
        node_map={},
    )
    pair = DataPlatformPair(
        powerbi_data_platform_name="GoogleBigQuery",
        datahub_data_platform_name="bigquery",
    )

    result = instance.expression_lineage(detail, "bigquery", pair, server_name="dsn")

    assert [u.urn for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_view,PROD)"
    ]


def test_is_sql_query_uses_platform_dialect():
    # BigQuery backtick-quoted, hyphenated project ids only parse under the
    # bigquery dialect; the default dialect rejects them.
    query = "SELECT t.* FROM `my-proj-1.my_dataset.my_table` t WHERE col_a IN (1, 2)"
    assert OdbcLineage.is_sql_query(query) is False
    assert OdbcLineage.is_sql_query(query, "bigquery") is True


def test_is_sql_query_handles_other_platforms_without_raising():
    # Plain SQL is recognised regardless of platform, and a platform sqlglot has
    # no dialect for (e.g. db2) must fall back to the default dialect, not raise.
    query = "SELECT a, b FROM my_schema.my_table"
    assert OdbcLineage.is_sql_query(query, "databricks") is True
    assert OdbcLineage.is_sql_query(query, "db2") is True
    assert OdbcLineage.is_sql_query("not a query at all", "db2") is False


# ---------------------------------------------------------------------------
# Snowflake Value.NativeQuery with a parameterized database identifier
# ---------------------------------------------------------------------------


def test_get_data_source_tokens_resolves_snowflake_item_access_with_parameter():
    """``Snowflake.Databases(SnowflakeURL, SnowflakeWarehouse){[Name=SnowflakeDBLake]}[Data]``.
    Positional IdentifierExpression args and the ItemAccess ``Name`` field all
    resolve from dataset parameters, so tokens[1] is the server rather than
    the record key ``Name``.
    """
    source_node = _snowflake_native_query_source_node()

    tokens = _get_data_source_tokens(
        node_map={},
        arg_node=source_node,
        parameters=_SNOWFLAKE_NATIVE_QUERY_PARAMETERS,
    )

    assert tokens[0] == "Snowflake.Databases"
    assert tokens[1] == "myserver.snowflakecomputing.com"
    assert tokens[2] == "MYWH"
    assert "Name" in tokens
    assert tokens[tokens.index("Name") + 1] == "DATA_LAKE_DEV"


def test_get_data_source_tokens_omits_name_when_parameter_unresolved():
    """Sanity check for the other side of the same fix: if the parameter isn't
    supplied at all, the ItemAccessExpression is now visited (the structural
    gap is fixed), but the identifier still can't be resolved to a literal --
    so ``Name`` correctly does not appear, rather than resolving to garbage.
    """
    source_node = _snowflake_native_query_source_node()

    tokens = _get_data_source_tokens(
        node_map={},
        arg_node=source_node,
        parameters=None,
    )

    assert "Name" not in tokens


def test_native_query_lineage_get_db_name_resolves_snowflake_from_name_token():
    """``get_db_name()`` had Databricks and BigQuery branches but fell through
    to ``None`` for every Snowflake token list, regardless of content."""
    instance = _build_native_query_lineage(config=_build_config())

    tokens = [
        "Snowflake.Databases",
        "SnowflakeURL",
        "SnowflakeWarehouse",
        "Name",
        "DATA_LAKE_DEV",
    ]

    assert instance.get_db_name(tokens) == "DATA_LAKE_DEV"


def test_native_query_lineage_get_db_name_returns_none_for_snowflake_without_name():
    """No ``Name`` token (e.g. a bare ``Snowflake.Databases(server, warehouse)``
    with no database navigation step at all) -- still correctly returns None
    rather than crashing or guessing."""
    instance = _build_native_query_lineage(config=_build_config())

    tokens = ["Snowflake.Databases", "SnowflakeURL", "SnowflakeWarehouse"]

    assert instance.get_db_name(tokens) is None


def _native_query_arg_list(sql: str, source_node: Optional[dict] = None) -> dict:
    """InvokeExpression node for ``Value.NativeQuery(<source>, "<sql>", null, [...])``
    -- just the 2 args ``create_lineage`` actually reads (source, sql)."""
    return {
        "kind": "InvokeExpression",
        "content": {
            "kind": "ArrayWrapper",
            "elements": [
                {
                    "kind": "Csv",
                    "node": source_node or _snowflake_native_query_source_node(),
                },
                {
                    "kind": "Csv",
                    "node": {
                        "kind": "LiteralExpression",
                        "literalKind": "Text",
                        "literal": f'"{sql}"',
                    },
                },
            ],
        },
    }


def test_create_lineage_warns_when_snowflake_database_name_unresolved():
    """PR #18030 added a warning for the equivalent unresolved-parameter case
    on the three-step navigation-chain path (no embedded SQL). This is the
    same failure mode on the ``Value.NativeQuery`` + embedded-SQL path:
    silently producing no/wrong lineage when a parameter can't be resolved.
    """
    instance = _build_native_query_lineage(
        config=_build_config(enable_advance_lineage_sql_construct=True)
    )
    detail = DataAccessFunctionDetail(
        arg_list=_native_query_arg_list(
            "select * from unqualified_table",
            source_node=_snowflake_native_query_source_node(),
        ),
        data_access_function_name="Value.NativeQuery",
        identifier_accessor=None,
        node_map={},
        # Server/warehouse resolve; SnowflakeDBLake is deliberately omitted.
        parameters={
            "SnowflakeURL": "myserver.snowflakecomputing.com",
            "SnowflakeWarehouse": "MYWH",
        },
    )

    instance.create_lineage(detail)

    warning_titles = [w.title for w in instance.reporter.warnings]
    assert "Unresolved database name in Value.NativeQuery" in warning_titles


def test_create_lineage_does_not_warn_when_snowflake_database_name_resolved():
    """Sanity check for the other side: when the parameter *is* supplied and
    resolves, no spurious warning is emitted."""
    instance = _build_native_query_lineage(
        config=_build_config(enable_advance_lineage_sql_construct=True)
    )
    detail = DataAccessFunctionDetail(
        arg_list=_native_query_arg_list(
            "select col_a from my_schema.my_table",
            source_node=_snowflake_native_query_source_node(),
        ),
        data_access_function_name="Value.NativeQuery",
        identifier_accessor=None,
        node_map={},
        parameters=_SNOWFLAKE_NATIVE_QUERY_PARAMETERS,
    )

    instance.create_lineage(detail)

    warning_titles = [w.title for w in instance.reporter.warnings]
    assert "Unresolved database name in Value.NativeQuery" not in warning_titles


def test_get_data_source_tokens_resolves_parameters_case_insensitively():
    source_node = _snowflake_native_query_source_node()

    tokens = _get_data_source_tokens(
        node_map={},
        arg_node=source_node,
        parameters={
            "snowflakeurl": "myserver.snowflakecomputing.com",
            "snowflakewarehouse": "MYWH",
            "snowflakedblake": "DATA_LAKE_DEV",
        },
    )

    assert tokens[1] == "myserver.snowflakecomputing.com"
    assert tokens[2] == "MYWH"
    assert tokens[tokens.index("Name") + 1] == "DATA_LAKE_DEV"


def test_create_lineage_skips_when_server_token_is_navigation_record_key():
    """Unresolved positional args + a literal {[Name=...]} used to put ``Name``
    in tokens[1], which create_lineage then treated as the Snowflake host."""
    instance = _build_native_query_lineage(
        config=_build_config(enable_advance_lineage_sql_construct=True)
    )
    detail = DataAccessFunctionDetail(
        arg_list=_native_query_arg_list(
            "select col_a from my_db.my_schema.my_table",
            source_node=_snowflake_native_query_source_node(
                name_value={
                    "kind": "LiteralExpression",
                    "literalKind": "Text",
                    "literal": '"DATA_LAKE_DEV"',
                }
            ),
        ),
        data_access_function_name="Value.NativeQuery",
        identifier_accessor=None,
        node_map={},
        parameters={},
    )

    result = instance.create_lineage(detail)

    assert result.upstreams == []
    assert result.column_lineage == []


def test_create_lineage_does_not_skip_bigquery_billing_project_record_key():
    """GoogleBigQuery.Database([BillingProject=...]) puts the record key
    ``BillingProject`` in tokens[1]. That is the connector's first argument, not
    a leaked Snowflake {[Name=...]} key — lineage must still be attempted.
    """
    instance = _build_native_query_lineage(
        config=_build_config(enable_advance_lineage_sql_construct=True)
    )
    detail = DataAccessFunctionDetail(
        arg_list=_native_query_arg_list("select * from public.my_table"),
        data_access_function_name="Value.NativeQuery",
        identifier_accessor=None,
        node_map={},
        parameters={},
    )
    sentinel = Lineage.empty()

    with (
        patch(
            "datahub.ingestion.source.powerbi.m_query.pattern_handler._get_data_source_tokens",
            return_value=[
                "GoogleBigQuery.Database",
                "BillingProject",
                "my_project",
                "Name",
                "my_project",
            ],
        ),
        patch.object(instance, "parse_custom_sql", return_value=sentinel) as mock_parse,
    ):
        result = instance.create_lineage(detail)

    mock_parse.assert_called_once()
    assert result is sentinel


def test_create_lineage_does_not_warn_when_sql_already_fully_qualified():
    instance = _build_native_query_lineage(
        config=_build_config(enable_advance_lineage_sql_construct=True)
    )
    detail = DataAccessFunctionDetail(
        arg_list=_native_query_arg_list(
            "select * from my_db.my_schema.my_table",
            source_node=_snowflake_native_query_source_node(),
        ),
        data_access_function_name="Value.NativeQuery",
        identifier_accessor=None,
        node_map={},
        parameters={
            "SnowflakeURL": "myserver.snowflakecomputing.com",
            "SnowflakeWarehouse": "MYWH",
        },
    )

    instance.create_lineage(detail)

    warning_titles = [w.title for w in instance.reporter.warnings]
    assert "Unresolved database name in Value.NativeQuery" not in warning_titles
