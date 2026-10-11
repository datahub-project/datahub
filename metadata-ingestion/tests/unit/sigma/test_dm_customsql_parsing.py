"""Unit tests for DM customSQL element SQL parsing (SqlParsingAggregator path)."""

import logging
from contextlib import contextmanager
from typing import Dict, Generator, cast
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
    SigmaDataModelColumn,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator


def _entry(
    name: str = "",
    connectionId: str = "",
    definition: str = "",
    type: str = "customSQL",
) -> CustomSqlEntry:
    return CustomSqlEntry(
        name=name, connectionId=connectionId, definition=definition, type=type
    )


def _get_aspect(wu: MetadataWorkUnit) -> UpstreamLineage:
    """Extract UpstreamLineage aspect from a workunit, asserting the correct types."""
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    return cast(UpstreamLineage, wu.metadata.aspect)


_SNOWFLAKE_CONN = SigmaConnectionRecord(
    connection_id="conn-sf-1",
    name="Snowflake",
    sigma_type="snowflake",
    datahub_platform="snowflake",
    is_mappable=True,
)

_UNMAPPABLE_CONN = SigmaConnectionRecord(
    connection_id="conn-csv-1",
    name="CSV Upload",
    sigma_type="csv",
    datahub_platform="",
    is_mappable=False,
)

_ELEMENT_URN = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el1,PROD)"
_SNOWFLAKE_TABLE = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.orders,PROD)"
)
_SNOWFLAKE_TABLE_2 = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.products,PROD)"
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


def _run_customsql_pipeline(
    sql: str,
    col_mapping: Dict[str, str],
) -> tuple[MetadataWorkUnit, SigmaSource]:
    """Register sql, drain aggregator; return the UpstreamLineage MCP and source."""
    source = _make_source()
    if col_mapping:
        source._customsql_col_mappings[_ELEMENT_URN] = col_mapping
        # All test mappings use simple single-ref formulas; treat as passthroughs.
        source._customsql_passthrough_mappings[_ELEMENT_URN] = col_mapping
    source._process_dm_customsql_element(
        _ELEMENT_URN,
        _entry(name="q1", connectionId=_SNOWFLAKE_CONN.connection_id, definition=sql),
    )
    mcps = list(source._drain_sql_aggregators())
    assert len(mcps) == 1, f"expected 1 workunit, got {len(mcps)}"
    return mcps[0], source


# ---------------------------------------------------------------------------
# _build_customsql_col_mapping
# ---------------------------------------------------------------------------


class TestCustomSqlColMapping:
    def test_formula_refs_produce_mapping(self) -> None:
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL")
        # Assign columns directly to bypass the /elements bare-string validator.
        element.columns = [
            SigmaDataModelColumn(
                columnId="c1",
                elementId="el-1",
                name="Customer Id",
                formula="[Custom SQL/CUSTOMER_ID]",
            ),
            SigmaDataModelColumn(
                columnId="c2",
                elementId="el-1",
                name="Total Spent",
                formula="[Custom SQL/TOTAL_SPENT]",
            ),
            SigmaDataModelColumn(
                columnId="c3",
                elementId="el-1",
                name="No Formula Col",
                formula=None,
            ),
            # Cross-element ref: must be excluded from the mapping.
            SigmaDataModelColumn(
                columnId="c4",
                elementId="el-1",
                name="Cross Ref Col",
                formula="[OtherElement/CUSTOMER_ID]",
            ),
        ]
        source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings[element_urn]
        assert mapping["customer_id"] == "Customer Id"
        assert mapping["total_spent"] == "Total Spent"
        # Cross-element ref must not pollute the map.
        assert len(mapping) == 2

    def test_multi_ref_formula_lands_in_col_mapping_not_passthrough(self) -> None:
        # A formula like If([Custom SQL/A]>0,[Custom SQL/B],0) has two refs.
        # Both A and B should land in _customsql_col_mappings for FGL rewriting
        # but neither in _customsql_passthrough_mappings (no SELECT * synthesis).
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL")
        element.columns = [
            SigmaDataModelColumn(
                columnId="c1",
                elementId="el-1",
                name="Computed Col",
                formula="If([Custom SQL/A]>0,[Custom SQL/B],0)",
            ),
        ]
        source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        passthrough = source._customsql_passthrough_mappings.get(element_urn, {})
        assert "a" in mapping and "b" in mapping
        assert "a" not in passthrough and "b" not in passthrough

    def test_case_insensitive_element_name_matching(self) -> None:
        # ref.source case may differ from element.name; matching must be case-insensitive.
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="custom sql")
        element.columns = [
            SigmaDataModelColumn(
                columnId="c1",
                elementId="el-1",
                name="Order Id",
                formula="[CUSTOM SQL/ORDER_ID]",  # uppercase source name
            ),
        ]
        source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        assert mapping.get("order_id") == "Order Id"

    def test_columnid_bare_uppercase_produces_mapping(self) -> None:
        # Passthrough column with no formula — only columnId (UPPER_SNAKE) available.
        # Mirrors the broken element 4lkhhi5LDn / "Custom SQL2" on dev tenant.
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        # Element name "Custom SQL2" intentionally does NOT match formula source "Custom SQL"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL2")
        element.columns = [
            SigmaDataModelColumn(
                columnId="CUSTOMER_ID",
                elementId="el-1",
                name="Customer Id",
                formula=None,
            ),
            SigmaDataModelColumn(
                columnId="TOTAL_SPENT",
                elementId="el-1",
                name="Total Spent",
                formula=None,
            ),
        ]
        source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        assert mapping.get("customer_id") == "Customer Id"
        assert mapping.get("total_spent") == "Total Spent"
        assert source.reporter.dm_customsql_col_mapping_via_columnid == 1

    def test_columnid_inode_format_produces_mapping(self) -> None:
        # inode-{urlId}/{NATIVE_NAME} format — warehouse-backed passthrough column.
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL")
        element.columns = [
            SigmaDataModelColumn(
                columnId="inode-abc123/CUSTOMER_ID",
                elementId="el-1",
                name="Customer Id",
                formula=None,
            ),
        ]
        source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        assert mapping.get("customer_id") == "Customer Id"
        assert source.reporter.dm_customsql_col_mapping_via_columnid == 1

    def test_columnid_opaque_hash_skipped(self) -> None:
        # Opaque hash columnIds (composition formulas) must not produce a mapping
        # via the columnId path; formula-ref path is unaffected.
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL")
        element.columns = [
            # Opaque hash — should be skipped
            SigmaDataModelColumn(
                columnId="1SRzZKnzmb",
                elementId="el-1",
                name="Aggregation",
                formula="Sum([Test Custom SQL/Total Spent])",
            ),
            # Formula-ref path still works for element-name-matching columns
            SigmaDataModelColumn(
                columnId="cb9IvPpXLU",
                elementId="el-1",
                name="Conditional",
                formula="[Custom SQL/ORDER_ID]",
            ),
        ]
        source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        # Only formula-ref col contributes; opaque columnId skipped
        assert "aggregation" not in mapping
        assert mapping.get("order_id") == "Conditional"
        # via_columnid not set because no valid columnId mapping was produced
        assert source.reporter.dm_customsql_col_mapping_via_columnid == 0
        # Both opaque-hash columnIds incremented the rejected counter
        assert source.reporter.dm_customsql_col_mapping_columnid_rejected == 2

    def test_columnid_and_formula_same_col_no_collision(self) -> None:
        # When both paths agree on the same SQL col → display name mapping,
        # no collision warning is logged and the final mapping is correct.
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL")
        element.columns = [
            SigmaDataModelColumn(
                columnId="CUSTOMER_ID",
                elementId="el-1",
                name="Customer Id",
                formula="[Custom SQL/CUSTOMER_ID]",
            ),
        ]
        with self._no_warnings(
            logging.getLogger("datahub.ingestion.source.sigma.sigma")
        ):
            source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        assert mapping.get("customer_id") == "Customer Id"

    def test_columnid_collision_warns(self) -> None:
        # Two columns map to the same SQL identifier via columnId — collision is logged.
        source = _make_source()
        element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        element = SigmaDataModelElement(elementId="el-1", name="Custom SQL")
        element.columns = [
            SigmaDataModelColumn(
                columnId="CUSTOMER_ID",
                elementId="el-1",
                name="Customer Id",
                formula=None,
            ),
            SigmaDataModelColumn(
                columnId="CUSTOMER_ID",
                elementId="el-1",
                name="Customer Identifier",
                formula=None,
            ),
        ]
        with self._assert_warning(
            logging.getLogger("datahub.ingestion.source.sigma.sigma"),
            "via columnId",
        ):
            source._build_customsql_col_mapping(element, element_urn)
        mapping = source._customsql_col_mappings.get(element_urn, {})
        # Last writer wins
        assert mapping.get("customer_id") == "Customer Identifier"

    # -------------------------------------------------------------------------
    # Helpers for log-assertion tests
    # -------------------------------------------------------------------------

    @staticmethod
    @contextmanager
    def _no_warnings(logger: logging.Logger) -> Generator[None, None, None]:
        """Context manager that fails if any WARNING (or above) is emitted."""
        records: list = []

        class _H(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno >= logging.WARNING:
                    records.append(record)

        h = _H()
        logger.addHandler(h)
        try:
            yield
        finally:
            logger.removeHandler(h)
        assert not records, f"Unexpected warnings: {[r.getMessage() for r in records]}"

    @staticmethod
    @contextmanager
    def _assert_warning(
        logger: logging.Logger, substr: str
    ) -> Generator[None, None, None]:
        """Context manager that asserts at least one WARNING containing ``substr``."""
        records: list = []

        class _H(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno >= logging.WARNING:
                    records.append(record)

        h = _H()
        logger.addHandler(h)
        try:
            yield
        finally:
            logger.removeHandler(h)
        assert any(substr in r.getMessage() for r in records), (
            f"Expected warning containing {substr!r}; got: {[r.getMessage() for r in records]}"
        )


# ---------------------------------------------------------------------------
# _process_dm_customsql_element pre-flight checks
# ---------------------------------------------------------------------------


class TestPreflightChecks:
    def test_missing_definition_skips_aggregator(self) -> None:
        source = _make_source()
        source._process_dm_customsql_element(
            "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)",
            _entry(
                name="q1", connectionId=_SNOWFLAKE_CONN.connection_id, definition=""
            ),
        )
        assert source.reporter.dm_customsql_skipped == 1
        assert source.reporter.dm_customsql_aggregator_invocations == 0

    def test_unknown_connection_skips_aggregator(self) -> None:
        source = _make_source()
        source._process_dm_customsql_element(
            "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)",
            _entry(name="q1", connectionId="conn-unknown", definition="SELECT 1"),
        )
        assert source.reporter.dm_customsql_skipped == 1
        assert source.reporter.dm_customsql_aggregator_invocations == 0

    def test_unmappable_platform_skips_aggregator(self) -> None:
        source = _make_source()
        source._process_dm_customsql_element(
            "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)",
            _entry(
                name="q1",
                connectionId=_UNMAPPABLE_CONN.connection_id,
                definition="SELECT 1",
            ),
        )
        assert source.reporter.dm_customsql_skipped == 1
        assert source.reporter.dm_customsql_aggregator_invocations == 0

    def test_multiple_customsql_source_ids_skips_second(self) -> None:
        # Only the first customSQL source_id per element is registered;
        # subsequent ones bump dm_customsql_skipped.
        source = _make_source()
        urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,dm.el,PROD)"
        e1 = _entry(
            name="q1",
            connectionId=_SNOWFLAKE_CONN.connection_id,
            definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
        )
        source._process_dm_customsql_element(urn, e1)
        source._process_dm_customsql_element(
            urn,
            _entry(name="q2", connectionId=e1.connectionId, definition=e1.definition),
        )
        assert source.reporter.dm_customsql_aggregator_invocations == 1
        assert source.reporter.dm_customsql_skipped == 1

    def test_aggregator_invocation_error_counter(self) -> None:
        source = _make_source()
        with patch.object(
            SqlParsingAggregator,
            "add_view_definition",
            side_effect=RuntimeError("boom"),
        ):
            source._process_dm_customsql_element(
                _ELEMENT_URN,
                _entry(
                    name="q1",
                    connectionId=_SNOWFLAKE_CONN.connection_id,
                    definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
                ),
            )
        assert source.reporter.dm_customsql_aggregator_invocation_errors == 1
        assert source.reporter.dm_customsql_aggregator_invocations == 0


# ---------------------------------------------------------------------------
# FGL rewrite and end-to-end aggregator (real SqlParsingAggregator)
# ---------------------------------------------------------------------------


class TestFglRewrite:
    def test_named_columns_fgl_rewritten_to_sigma_names(self) -> None:
        wu, _ = _run_customsql_pipeline(
            sql="SELECT ORDER_ID, AMOUNT FROM TEST_DB.TEST_SCHEMA.ORDERS",
            col_mapping={"order_id": "Order Id", "amount": "Amount"},
        )
        aspect = _get_aspect(wu)
        assert isinstance(aspect, UpstreamLineage)
        assert len(aspect.upstreams) == 1
        assert aspect.upstreams[0].dataset == _SNOWFLAKE_TABLE

        fgls = aspect.fineGrainedLineages or []
        assert len(fgls) > 0
        all_downstreams = [d for fgl in fgls for d in (fgl.downstreams or [])]
        for ds in all_downstreams:
            sfu = SchemaFieldUrn.from_string(ds)
            assert sfu.field_path in ("Order Id", "Amount"), (
                f"unexpected field: {sfu.field_path}"
            )

    def test_select_star_synthesizes_fgl_from_col_mapping(self) -> None:
        # SELECT * with a col_mapping: FGL is synthesized column-by-column
        # from the formula refs, connecting upstream table columns to Sigma
        # display names without needing the aggregator to expand *.
        wu, source = _run_customsql_pipeline(
            sql="SELECT * FROM TEST_DB.TEST_SCHEMA.ORDERS",
            col_mapping={"order_id": "Order Id", "amount": "Amount"},
        )
        aspect = _get_aspect(wu)
        assert isinstance(aspect, UpstreamLineage)
        assert len(aspect.upstreams) == 1
        fgls = aspect.fineGrainedLineages or []
        assert len(fgls) == 2
        downstream_fields = {
            SchemaFieldUrn.from_string(ds).field_path
            for fgl in fgls
            for ds in (fgl.downstreams or [])
        }
        assert downstream_fields == {"Order Id", "Amount"}
        assert source.reporter.dm_customsql_column_lineage_emitted == 1

    def test_select_star_no_col_mapping_emits_entity_lineage_only(self) -> None:
        wu, source = _run_customsql_pipeline(
            sql="SELECT * FROM TEST_DB.TEST_SCHEMA.ORDERS",
            col_mapping={},
        )
        aspect = _get_aspect(wu)
        assert isinstance(aspect, UpstreamLineage)
        assert len(aspect.upstreams) == 1
        assert not aspect.fineGrainedLineages
        assert source.reporter.dm_customsql_column_lineage_emitted == 0

    def test_cte_alias_not_in_upstreams(self) -> None:
        wu, _ = _run_customsql_pipeline(
            sql=(
                "WITH recent AS ("
                "  SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS"
                ") SELECT ORDER_ID FROM recent"
            ),
            col_mapping={"order_id": "Order Id"},
        )
        aspect = _get_aspect(wu)
        assert isinstance(aspect, UpstreamLineage)
        upstream_datasets = {u.dataset for u in aspect.upstreams}
        assert _SNOWFLAKE_TABLE in upstream_datasets
        assert len(upstream_datasets) == 1

    def test_join_produces_two_upstream_datasets(self) -> None:
        wu, _ = _run_customsql_pipeline(
            sql=(
                "SELECT o.ORDER_ID, p.NAME "
                "FROM TEST_DB.TEST_SCHEMA.ORDERS o "
                "JOIN TEST_DB.TEST_SCHEMA.PRODUCTS p ON o.PRODUCT_ID = p.ID"
            ),
            col_mapping={"order_id": "Order Id", "name": "Name"},
        )
        aspect = _get_aspect(wu)
        assert isinstance(aspect, UpstreamLineage)
        upstream_datasets = {u.dataset for u in aspect.upstreams}
        assert len(upstream_datasets) == 2
        assert _SNOWFLAKE_TABLE in upstream_datasets

    def test_unmapped_fgl_downstream_counter(self) -> None:
        # col_mapping covers order_id but not amount — amount FGL entry is dropped.
        source = _make_source()
        source._customsql_col_mappings[_ELEMENT_URN] = {"order_id": "Order Id"}
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT ORDER_ID, AMOUNT FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        list(source._drain_sql_aggregators())
        assert source.reporter.dm_customsql_fgl_downstream_unmapped == 1

    def test_no_col_mapping_fgl_passthrough(self) -> None:
        # No entry in _customsql_col_mappings -> FGL downstreams passed through unchanged.
        source = _make_source()
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        mcps = list(source._drain_sql_aggregators())
        assert len(mcps) == 1
        assert isinstance(mcps[0].metadata, MetadataChangeProposalWrapper)
        aspect = cast(UpstreamLineage, mcps[0].metadata.aspect)
        assert isinstance(aspect, UpstreamLineage)
        fgls = aspect.fineGrainedLineages or []
        assert len(fgls) > 0
        all_downstreams = [d for fgl in fgls for d in (fgl.downstreams or [])]
        for ds in all_downstreams:
            sfu = SchemaFieldUrn.from_string(ds)
            assert sfu.field_path == "order_id"  # SQL name; not rewritten

    def test_mixed_sources_extra_upstreams_and_fgls_merged(self) -> None:
        # When an element has both customSQL and non-customSQL source_ids,
        # both the non-customSQL Upstream edges and their FGL must appear in the
        # final UpstreamLineage — not silently overwritten by the drain MCP.
        source = _make_source()
        source._customsql_col_mappings[_ELEMENT_URN] = {"order_id": "Order Id"}
        source._customsql_passthrough_mappings[_ELEMENT_URN] = {"order_id": "Order Id"}
        extra_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,some.intra.element,PROD)"
        extra_field_urn = f"urn:li:schemaField:({extra_urn},intra_col)"
        # Simulate stashed non-customSQL upstreams + FGL from the per-element path.
        source._customsql_extra_upstreams[_ELEMENT_URN] = [
            Upstream(dataset=extra_urn, type=DatasetLineageType.TRANSFORMED)
        ]
        source._customsql_extra_fgls[_ELEMENT_URN] = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[extra_field_urn],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[f"urn:li:schemaField:({_ELEMENT_URN},Order Id)"],
                confidenceScore=1.0,
            )
        ]
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        mcps = list(source._drain_sql_aggregators())
        assert len(mcps) == 1
        assert isinstance(mcps[0].metadata, MetadataChangeProposalWrapper)
        aspect = cast(UpstreamLineage, mcps[0].metadata.aspect)
        assert isinstance(aspect, UpstreamLineage)
        # Both customSQL and non-customSQL upstreams present.
        upstream_datasets = {u.dataset for u in aspect.upstreams}
        assert _SNOWFLAKE_TABLE in upstream_datasets
        assert extra_urn in upstream_datasets
        # Non-customSQL FGL must survive the drain merge.
        all_upstream_fields = {
            f
            for fgl in (aspect.fineGrainedLineages or [])
            for f in (fgl.upstreams or [])
        }
        assert extra_field_urn in all_upstream_fields

    def test_preflight_failure_does_not_stash_extras(self) -> None:
        # When customSQL pre-flight fails, _customsql_extra_upstreams is NOT
        # stashed and the per-element MCP (non-customSQL upstreams) is emitted
        # immediately — not silently dropped.

        source = _make_source()
        # Simulate: customSQL pre-flight will fail (unknown connection),
        # so the element is NOT added to _customsql_registered_urns.
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(name="q1", connectionId="conn-unknown", definition="SELECT 1"),
        )
        assert _ELEMENT_URN not in source._customsql_registered_urns
        # Extras must NOT be stashed (no customSQL lineage to consolidate with).
        assert _ELEMENT_URN not in source._customsql_extra_upstreams

    def test_fgl_rewrite_passes_through_when_ds_parent_not_in_mappings(self) -> None:
        # When the downstream schemaField's parent URN is not in
        # _customsql_col_mappings (e.g. FGL referencing a non-customSQL
        # element), the original URN is passed through unchanged.
        source = _make_source()
        other_element_urn = "urn:li:dataset:(urn:li:dataPlatform:sigma,other.el,PROD)"
        other_field_urn = f"urn:li:schemaField:({other_element_urn},some_col)"
        upstream_field_urn = f"urn:li:schemaField:({_SNOWFLAKE_TABLE},order_id)"
        source._customsql_registered_urns.add(_ELEMENT_URN)

        mcp = MetadataChangeProposalWrapper(
            entityUrn=_ELEMENT_URN,
            aspect=UpstreamLineage(
                upstreams=[
                    Upstream(dataset=_SNOWFLAKE_TABLE, type=DatasetLineageType.VIEW)
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[upstream_field_urn],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[other_field_urn],  # parent not in col_mappings
                        confidenceScore=0.2,
                    )
                ],
            ),
        )
        result = source._rewrite_fgl_downstreams(mcp)
        aspect = cast(UpstreamLineage, result.aspect)
        fgls = aspect.fineGrainedLineages or []
        assert len(fgls) == 1
        # Original URN passed through unchanged.
        assert other_field_urn in (fgls[0].downstreams or [])

    def test_select_star_join_emits_entity_lineage_only(self) -> None:
        # SELECT * with multiple upstreams (JOIN): synthesis is skipped
        # (can't assign columns to tables without schema) and only entity
        # lineage is emitted.
        wu, source = _run_customsql_pipeline(
            sql=(
                "SELECT * FROM TEST_DB.TEST_SCHEMA.ORDERS o "
                "JOIN TEST_DB.TEST_SCHEMA.PRODUCTS p ON o.PRODUCT_ID = p.ID"
            ),
            col_mapping={"order_id": "Order Id"},
        )
        aspect = _get_aspect(wu)
        assert isinstance(aspect, UpstreamLineage)
        assert len(aspect.upstreams) == 2
        assert not aspect.fineGrainedLineages
        assert source.reporter.dm_customsql_column_lineage_emitted == 0

    def test_select_star_with_computed_col_only_synthesizes_passthrough(self) -> None:
        # When the element has a passthrough column AND a computed-expression column,
        # only the passthrough (single-ref formula) appears in synthesised SELECT * FGL.
        source = _make_source()
        source._customsql_col_mappings[_ELEMENT_URN] = {
            "order_id": "Order Id",
            "a": "Computed",
            "b": "Computed",
        }
        # Passthrough-only: order_id is single-ref; a and b come from a multi-ref formula.
        source._customsql_passthrough_mappings[_ELEMENT_URN] = {"order_id": "Order Id"}
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT * FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        mcps = list(source._drain_sql_aggregators())
        assert len(mcps) == 1
        assert isinstance(mcps[0].metadata, MetadataChangeProposalWrapper)
        aspect = cast(UpstreamLineage, mcps[0].metadata.aspect)
        assert isinstance(aspect, UpstreamLineage)
        fgls = aspect.fineGrainedLineages or []
        downstream_fields = {
            SchemaFieldUrn.from_string(ds).field_path
            for fgl in fgls
            for ds in (fgl.downstreams or [])
        }
        # Only the passthrough column should appear; computed columns excluded.
        assert downstream_fields == {"Order Id"}

    def test_drain_continues_after_aggregator_gen_metadata_fails(self) -> None:
        # If gen_metadata raises for one aggregator, other platforms still drain
        # and aggregator.close() is always called.
        from unittest.mock import MagicMock

        source = _make_source()
        # Register two different platform aggregators.
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT ORDER_ID FROM TEST_DB.TEST_SCHEMA.ORDERS",
            ),
        )
        # Patch gen_metadata on the Snowflake aggregator to raise.
        cache_key = ("snowflake", "PROD", None)
        bad_aggregator = source._sql_aggregators[cache_key]
        close_mock = MagicMock(wraps=bad_aggregator.close)
        with (
            patch.object(bad_aggregator, "close", close_mock),
            patch.object(
                bad_aggregator,
                "gen_metadata",
                MagicMock(side_effect=RuntimeError("boom")),
            ),
        ):
            list(source._drain_sql_aggregators())

        # drain warning must be reported, close must still be called.
        assert any(
            "drain failed" in (w.title or "").lower() for w in source.reporter.warnings
        )
        close_mock.assert_called_once()

    def test_parse_failed_counter(self) -> None:
        source = _make_source()
        source._process_dm_customsql_element(
            _ELEMENT_URN,
            _entry(
                name="q1",
                connectionId=_SNOWFLAKE_CONN.connection_id,
                definition="SELECT FROM",
            ),
        )
        list(source._drain_sql_aggregators())
        assert source.reporter.dm_customsql_parse_failed == 1
