"""Unit tests for workbook customSQL chart SQL parsing (T4.F)."""

from typing import Dict, Optional, Tuple
from unittest.mock import patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    CustomSqlEntry,
    Element,
    Workbook,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.schema_classes import InputFieldsClass
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SNOWFLAKE_CONN = SigmaConnectionRecord(
    connection_id="conn-sf-wb",
    name="Snowflake",
    sigma_type="snowflake",
    datahub_platform="snowflake",
    is_mappable=True,
)

_UNMAPPABLE_CONN = SigmaConnectionRecord(
    connection_id="conn-csv-wb",
    name="CSV Upload",
    sigma_type="csv",
    datahub_platform="",
    is_mappable=False,
)

_CHART_URN = "urn:li:chart:(sigma,chart-el-01)"
_SNOWFLAKE_TABLE = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.orders,PROD)"
)


def _entry(
    name: str = "",
    connectionId: str = "",
    definition: str = "",
    type: str = "customSQL",
) -> CustomSqlEntry:
    return CustomSqlEntry(
        name=name, connectionId=connectionId, definition=definition, type=type
    )


def _make_source() -> SigmaSource:
    config = SigmaSourceConfig(
        client_id="test_id",
        client_secret="test_secret",
    )
    ctx = PipelineContext(run_id="test")
    with (
        patch.object(SigmaAPI, "_generate_token"),
        patch.object(SigmaSource, "_build_connection_registry") as mock_reg,
    ):
        mock_reg.return_value = SigmaConnectionRegistry(
            by_id={
                _SNOWFLAKE_CONN.connection_id: _SNOWFLAKE_CONN,
                _UNMAPPABLE_CONN.connection_id: _UNMAPPABLE_CONN,
            }
        )
        source = SigmaSource(config=config, ctx=ctx)
    return source


# ---------------------------------------------------------------------------
# get_workbook_lineage_entries (sigma_api.py)
# ---------------------------------------------------------------------------


class TestGetWorkbookLineageEntries:
    def test_returns_entries_list(self) -> None:
        source = _make_source()
        fake_entries = [
            {
                "type": "customSQL",
                "name": "q1",
                "connectionId": "conn-sf-wb",
                "definition": "SELECT 1",
            },
            {"type": "element", "elementId": "el-01", "sourceIds": ["q1"]},
        ]
        with patch.object(
            source.sigma_api,
            "_paginated_raw_entries",
            return_value=fake_entries,
        ) as mock_pag:
            result = source.sigma_api.get_workbook_lineage_entries("wb-001")
        assert result == fake_entries
        mock_pag.assert_called_once()
        call_url = mock_pag.call_args[0][0]
        assert "workbooks/wb-001/lineage" in call_url

    def test_silent_statuses_set_correctly(self) -> None:
        source = _make_source()
        with patch.object(
            source.sigma_api, "_paginated_raw_entries", return_value=[]
        ) as mock_pag:
            result = source.sigma_api.get_workbook_lineage_entries("wb-missing")
        assert result == []
        _, kwargs = mock_pag.call_args
        assert kwargs.get("silent_statuses") == (400, 403, 404)


# ---------------------------------------------------------------------------
# _build_workbook_customsql_registry
# ---------------------------------------------------------------------------


def _make_workbook(workbook_id: str = "wb-001") -> Workbook:
    return Workbook(
        workbookId=workbook_id,
        name="Test Workbook",
        ownerId="owner-001",
        latestVersion=1,
        workspaceId="ws-001",
        path="Test Workspace",
        badge=None,
        createdBy="test-user",
        updatedBy="test-user",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        url="https://app.sigmacomputing.com/workbook/wb-001",
    )


class TestBuildWorkbookCustomsqlRegistry:
    def test_maps_customsql_names_to_element_ids(self) -> None:
        source = _make_source()
        lineage_entries = [
            {
                "type": "customSQL",
                "name": "q1",
                "connectionId": _SNOWFLAKE_CONN.connection_id,
                "definition": "SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
            },
            {
                "type": "element",
                "elementId": "el-chart-01",
                "sourceIds": ["q1"],
            },
        ]
        with patch.object(
            source.sigma_api,
            "get_workbook_lineage_entries",
            return_value=lineage_entries,
        ):
            csql_by_name, eid_by_name = source._build_workbook_customsql_registry(
                _make_workbook()
            )
        assert "q1" in csql_by_name
        assert csql_by_name["q1"].definition.startswith("SELECT")
        assert eid_by_name["q1"] == ["el-chart-01"]

    def test_element_before_customsql_in_response(self) -> None:
        # Element entries may appear before customSQL entries in the response.
        source = _make_source()
        lineage_entries = [
            {
                "type": "element",
                "elementId": "el-chart-02",
                "sourceIds": ["q2"],
            },
            {
                "type": "customSQL",
                "name": "q2",
                "connectionId": _SNOWFLAKE_CONN.connection_id,
                "definition": "SELECT 1",
            },
        ]
        with patch.object(
            source.sigma_api,
            "get_workbook_lineage_entries",
            return_value=lineage_entries,
        ):
            csql_by_name, eid_by_name = source._build_workbook_customsql_registry(
                _make_workbook()
            )
        assert eid_by_name.get("q2") == ["el-chart-02"]

    def test_element_with_no_matching_customsql_ignored(self) -> None:
        source = _make_source()
        lineage_entries = [
            {
                "type": "element",
                "elementId": "el-orphan",
                "sourceIds": ["nonexistent"],
            },
        ]
        with patch.object(
            source.sigma_api,
            "get_workbook_lineage_entries",
            return_value=lineage_entries,
        ):
            csql_by_name, eid_by_name = source._build_workbook_customsql_registry(
                _make_workbook()
            )
        assert not csql_by_name
        assert not eid_by_name

    def test_empty_lineage_returns_empty_maps(self) -> None:
        source = _make_source()
        with patch.object(
            source.sigma_api, "get_workbook_lineage_entries", return_value=[]
        ):
            csql_by_name, eid_by_name = source._build_workbook_customsql_registry(
                _make_workbook()
            )
        assert not csql_by_name
        assert not eid_by_name


# ---------------------------------------------------------------------------
# _build_workbook_customsql_col_mapping
# ---------------------------------------------------------------------------


def _make_element_with_formulas(
    element_id: str,
    name: str,
    formulas: Dict[str, Optional[str]],
) -> Element:
    """Build an Element with pre-populated column_formulas."""
    el = Element(
        elementId=element_id,
        name=name,
        url=f"https://app.sigmacomputing.com/element/{element_id}",
        columns=list(formulas.keys()),
    )
    el.column_formulas = {k: v for k, v in formulas.items()}
    return el


class TestBuildWorkbookCustomsqlColMapping:
    def test_single_ref_formula_lands_in_passthrough(self) -> None:
        source = _make_source()
        el = _make_element_with_formulas(
            "el-01",
            "Custom SQL",
            {
                "order_id": "[Custom SQL/order_id]",
                "amount": "[Custom SQL/amount]",
            },
        )
        source._build_workbook_customsql_col_mapping(el, _CHART_URN)
        passthrough = source._customsql_passthrough_mappings.get(_CHART_URN, {})
        assert passthrough["order_id"] == "order_id"
        assert passthrough["amount"] == "amount"
        # _customsql_col_mappings must NOT be touched (no FGL rewrite for charts)
        assert _CHART_URN not in source._customsql_col_mappings

    def test_cross_source_ref_excluded(self) -> None:
        source = _make_source()
        el = _make_element_with_formulas(
            "el-01",
            "Custom SQL",
            {"col": "[OtherElement/col]"},
        )
        source._build_workbook_customsql_col_mapping(el, _CHART_URN)
        assert _CHART_URN not in source._customsql_passthrough_mappings

    def test_multi_ref_formula_excluded_from_passthrough(self) -> None:
        source = _make_source()
        el = _make_element_with_formulas(
            "el-01",
            "Custom SQL",
            {"computed": "[Custom SQL/a] + [Custom SQL/b]"},
        )
        source._build_workbook_customsql_col_mapping(el, _CHART_URN)
        assert _CHART_URN not in source._customsql_passthrough_mappings

    def test_case_insensitive_source_match(self) -> None:
        source = _make_source()
        el = _make_element_with_formulas(
            "el-01",
            "custom sql",
            {"COL": "[CUSTOM SQL/COL]"},
        )
        source._build_workbook_customsql_col_mapping(el, _CHART_URN)
        passthrough = source._customsql_passthrough_mappings.get(_CHART_URN, {})
        assert passthrough.get("col") == "COL"

    def test_none_formula_skipped(self) -> None:
        source = _make_source()
        el = _make_element_with_formulas(
            "el-01",
            "Custom SQL",
            {"no_formula": None},
        )
        source._build_workbook_customsql_col_mapping(el, _CHART_URN)
        assert _CHART_URN not in source._customsql_passthrough_mappings


# ---------------------------------------------------------------------------
# _process_workbook_customsql_element pre-flight checks
# ---------------------------------------------------------------------------


class TestProcessWorkbookCustomsqlElement:
    def test_missing_definition_skips(self) -> None:
        source = _make_source()
        source._process_workbook_customsql_element(
            _CHART_URN,
            _entry(
                name="q1", connectionId=_SNOWFLAKE_CONN.connection_id, definition=""
            ),
        )
        assert source.reporter.workbook_customsql_skipped == 1
        assert source.reporter.workbook_customsql_aggregator_invocations == 0

    def test_unknown_connection_skips(self) -> None:
        source = _make_source()
        source._process_workbook_customsql_element(
            _CHART_URN,
            _entry(name="q1", connectionId="unknown-conn", definition="SELECT 1"),
        )
        assert source.reporter.workbook_customsql_skipped == 1
        assert source.reporter.workbook_customsql_aggregator_invocations == 0

    def test_unmappable_platform_skips(self) -> None:
        source = _make_source()
        source._process_workbook_customsql_element(
            _CHART_URN,
            _entry(
                name="q1",
                connectionId=_UNMAPPABLE_CONN.connection_id,
                definition="SELECT 1",
            ),
        )
        assert source.reporter.workbook_customsql_skipped == 1
        assert source.reporter.workbook_customsql_aggregator_invocations == 0

    def test_successful_registration(self) -> None:
        source = _make_source()
        source._process_workbook_customsql_element(
            _CHART_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        assert source.reporter.workbook_customsql_aggregator_invocations == 1
        assert _CHART_URN in source._workbook_customsql_registered_urns

    def test_aggregator_exception_bumps_error_counter(self) -> None:
        source = _make_source()
        with patch.object(
            SqlParsingAggregator,
            "add_view_definition",
            side_effect=RuntimeError("boom"),
        ):
            source._process_workbook_customsql_element(
                _CHART_URN,
                _entry(
                    name="q1",
                    connectionId=_SNOWFLAKE_CONN.connection_id,
                    definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
                ),
            )
        assert source.reporter.workbook_customsql_aggregator_invocation_errors == 1
        assert source.reporter.workbook_customsql_aggregator_invocations == 0
        assert _CHART_URN not in source._workbook_customsql_registered_urns


# ---------------------------------------------------------------------------
# _rewrite_fgl_downstreams — workbook chart branch (end-to-end)
# ---------------------------------------------------------------------------


def _run_workbook_customsql_pipeline(
    sql: str,
    passthrough_mapping: Dict[str, str],
) -> Tuple[MetadataWorkUnit, SigmaSource]:
    """Register chart_urn with sql, drain; return the InputFields workunit + source."""
    source = _make_source()
    if passthrough_mapping:
        source._customsql_passthrough_mappings[_CHART_URN] = passthrough_mapping
    source._process_workbook_customsql_element(
        _CHART_URN,
        _entry(name="q1", connectionId=_SNOWFLAKE_CONN.connection_id, definition=sql),
    )
    mcps = list(source._drain_sql_aggregators())
    assert len(mcps) == 1, f"expected 1 workunit, got {len(mcps)}"
    return mcps[0], source


class TestRewriteFglDownstreamsWorkbookBranch:
    def test_named_columns_emit_input_fields(self) -> None:
        wu, source = _run_workbook_customsql_pipeline(
            sql="SELECT order_id, amount FROM TEST_DB.TEST_SCHEMA.ORDERS",
            passthrough_mapping={},
        )
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        assert isinstance(aspect, InputFieldsClass)
        assert len(aspect.fields) == 2
        upstream_urns = {f.schemaFieldUrn for f in aspect.fields}
        assert any("order_id" in u for u in upstream_urns)
        assert any("amount" in u for u in upstream_urns)
        assert source.reporter.workbook_customsql_upstream_emitted == 1
        assert source.reporter.workbook_customsql_column_lineage_emitted == 1
        assert source.reporter.dm_customsql_upstream_emitted == 0

    def test_select_star_synthesizes_input_fields_from_passthrough(self) -> None:
        wu, source = _run_workbook_customsql_pipeline(
            sql="SELECT * FROM TEST_DB.TEST_SCHEMA.ORDERS",
            passthrough_mapping={"order_id": "order_id", "amount": "amount"},
        )
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        assert isinstance(aspect, InputFieldsClass)
        assert len(aspect.fields) == 2
        field_paths = {
            f.schemaField.fieldPath for f in aspect.fields if f.schemaField is not None
        }
        assert field_paths == {"order_id", "amount"}
        assert source.reporter.workbook_customsql_column_lineage_emitted == 1
        assert source.reporter.dm_customsql_column_lineage_emitted == 0

    def test_select_star_no_passthrough_emits_empty_input_fields(self) -> None:
        wu, source = _run_workbook_customsql_pipeline(
            sql="SELECT * FROM TEST_DB.TEST_SCHEMA.ORDERS",
            passthrough_mapping={},
        )
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        assert isinstance(aspect, InputFieldsClass)
        assert aspect.fields == []
        assert source.reporter.workbook_customsql_column_lineage_emitted == 0

    def test_dm_counter_not_bumped_for_chart_urn(self) -> None:
        source = _make_source()
        source._process_workbook_customsql_element(
            _CHART_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT order_id FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        list(source._drain_sql_aggregators())
        assert source.reporter.dm_customsql_upstream_emitted == 0
        assert source.reporter.workbook_customsql_upstream_emitted == 1
