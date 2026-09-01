import time
from typing import Any, Dict, Iterator, List, Optional, cast

import pytest
import time_machine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.montecarlo import assertion as mc_assertion
from datahub.ingestion.source.montecarlo.assertion import (
    MonteCarloAssertionBuilder,
    MonteCarloAssertionKey,
)
from datahub.ingestion.source.montecarlo.client import (
    MonteCarloAlert,
    MonteCarloAssertionDef,
    MonteCarloAuthError,
    MonteCarloClient,
    MonteCarloComparison,
    ResolvedTable,
    _parse_comparisons,
)
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import (
    MconResolver,
    parse_mcon,
)
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.ingestion.source.montecarlo.source import MonteCarloSource
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionRunEventClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DataPlatformInstanceClass,
    DatasetAssertionScopeClass,
)
from datahub.utilities.ratelimiter import DailyCallBudget, DailyCallBudgetExceeded


def make_config(**overrides: Any) -> MonteCarloSourceConfig:
    base: Dict[str, Any] = {"api_id": "id", "api_token": "token"}
    base.update(overrides)
    return MonteCarloSourceConfig.parse_obj(base)


class FakeResolverClient:
    """Stands in for MonteCarloClient for resolver/builder tests."""

    def __init__(self, tables: Dict[str, ResolvedTable]) -> None:
        self._tables = tables

    def get_table(self, mcon: str) -> Optional[ResolvedTable]:
        return self._tables.get(mcon)


def test_alerts_default_window_is_30_days() -> None:
    cfg = make_config()
    assert cfg.alerts_lookback_days == 30
    assert cfg.include_alerts is True


def test_include_alerts_requires_include_assertions() -> None:
    # Alerts attach to assertions, so this combination is rejected at config time.
    with pytest.raises(ValueError):
        make_config(include_alerts=True, include_assertions=False)


def test_parse_mcon() -> None:
    parsed = parse_mcon("MCON++acct++warehouse-1++table++db.schema.tbl")
    assert parsed is not None
    assert parsed.resource_id == "warehouse-1"
    assert parsed.object_id == "db.schema.tbl"
    assert parse_mcon("not-an-mcon") is None
    assert parse_mcon("MCON++too++few") is None


def test_assertion_key_is_stable() -> None:
    guid1 = MonteCarloAssertionKey(monitor_uuid="abc").guid()
    guid2 = MonteCarloAssertionKey(monitor_uuid="abc").guid()
    guid3 = MonteCarloAssertionKey(monitor_uuid="def").guid()
    assert guid1 == guid2
    assert guid1 != guid3


def test_resolver_uses_connection_map_then_connection_type() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-1++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    # connection_to_platform_map wins over the resolved connection type.
    cfg = make_config(
        connection_to_platform_map={
            "wh-1": {
                "platform": "snowflake",
                "platform_instance": "prod",
                "env": "PROD",
            }
        }
    )
    resolver = MconResolver(cfg, client, report)
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "snowflake" in urn and "prod" in urn
    assert report.mcons_resolved == 1


def test_resolver_auto_maps_connection_type_when_enabled() -> None:
    """With auto_map_connection_types enabled, a warehouse missing from
    connection_to_platform_map is resolved from its connection type."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    resolver = MconResolver(make_config(auto_map_connection_types=True), client, report)
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None and "bigquery" in urn
    assert report.mcons_resolved == 1


def test_resolver_skips_auto_mappable_warehouse_by_default() -> None:
    """auto_map_connection_types defaults to False, so a warehouse that *could* be
    auto-mapped is skipped with a warning unless explicitly listed in
    connection_to_platform_map. Explicit mapping is the safe default."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    resolver = MconResolver(make_config(), client, report)
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert mcon in report.mcons_unmapped_platform
    assert report.mcons_resolved == 0


def test_resolver_uses_default_platform_when_auto_mapping_enabled() -> None:
    """default_platform is only consulted when auto_map_connection_types is enabled;
    it covers connection types not in the built-in connection-type map."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-x++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="exotic-db"
            )
        }
    )
    cfg = make_config(auto_map_connection_types=True, default_platform="postgres")
    resolver = MconResolver(cfg, client, report)
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "postgres" in urn
    assert report.mcons_resolved == 1


def test_resolver_default_platform_ignored_when_auto_mapping_disabled() -> None:
    """default_platform alone (without auto_map_connection_types) is now rejected
    at config-validation time by the _require_auto_map_for_default_platform
    validator, so it can never silently no-op at resolution time. The resolver-
    level behavior here is therefore unreachable; see
    test_default_platform_requires_auto_map for the validator check."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="default_platform requires"):
        make_config(default_platform="postgres")


def test_resolver_warns_on_unmapped_platform() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-3++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="exotic-db"
            )
        }
    )
    resolver = MconResolver(make_config(), client, report)
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert mcon in report.mcons_unmapped_platform


def test_resolver_handles_get_table_exception() -> None:
    """Exceptions from client.get_table are caught, warned, and cached as None."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-err++table++db.sch.tbl"

    class ErrorClient:
        calls = 0

        def get_table(self, mcon: str) -> Optional[ResolvedTable]:
            ErrorClient.calls += 1
            raise RuntimeError("network error")

    resolver = MconResolver(make_config(), ErrorClient(), report)
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert report.mcons_resolution_failed == 1
    # Second call should use the cache, not call the client again.
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert ErrorClient.calls == 1


def test_resolver_caches_results() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"

    class CountingClient(FakeResolverClient):
        calls = 0

        def get_table(self, mcon: str) -> Optional[ResolvedTable]:
            CountingClient.calls += 1
            return super().get_table(mcon)

    client = CountingClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    resolver = MconResolver(make_config(), client, report)
    resolver.dataset_urn_for_mcon(mcon)
    resolver.dataset_urn_for_mcon(mcon)
    assert CountingClient.calls == 1


def _build_assertion_workunits(
    builder: MonteCarloAssertionBuilder, definition: MonteCarloAssertionDef
) -> List[MetadataWorkUnit]:
    return list(builder.build_assertion(definition))


def _aspect(wu: MetadataWorkUnit) -> object:
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    return wu.metadata.aspect


def test_build_assertion_emits_custom_and_platform_instance() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)

    definition = MonteCarloAssertionDef(
        uuid="mon-1",
        name="Freshness on orders",
        description="orders should be fresh",
        monitor_type="FRESHNESS",
        entity_mcons=[mcon],
        resource_id="wh-2",
        data_quality_dimension="FRESHNESS",
    )
    wus = _build_assertion_workunits(builder, definition)
    aspects = [_aspect(wu) for wu in wus]
    assert any(isinstance(a, AssertionInfoClass) for a in aspects)
    assert any(isinstance(a, DataPlatformInstanceClass) for a in aspects)

    info = next(a for a in aspects if isinstance(a, AssertionInfoClass))
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.customAssertion is not None
    assert info.customAssertion.type == "FRESHNESS"
    assert info.customProperties["mc_monitor_uuid"] == "mon-1"
    assert report.assertions_emitted == 1


def test_build_assertion_oss_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    # Force the cloud SDK to appear unavailable; the OSS fallback must still emit
    # the assertionInfo + dataPlatformInstance aspects.
    monkeypatch.setattr(mc_assertion, "_load_cloud_assertion_class", lambda: None)

    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)

    definition = MonteCarloAssertionDef(
        uuid="mon-1", monitor_type="VOLUME", entity_mcons=[mcon]
    )
    aspects = [_aspect(wu) for wu in _build_assertion_workunits(builder, definition)]
    info = next(a for a in aspects if isinstance(a, AssertionInfoClass))
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.customAssertion is not None and info.customAssertion.type == "VOLUME"
    assert any(isinstance(a, DataPlatformInstanceClass) for a in aspects)
    assert report.assertions_emitted == 1


def test_build_assertion_skips_unresolvable_asset() -> None:
    report = MonteCarloSourceReport()
    cfg = make_config()
    resolver = MconResolver(cfg, FakeResolverClient({}), report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    definition = MonteCarloAssertionDef(
        uuid="mon-x", entity_mcons=["MCON++a++b++table++c"]
    )
    assert _build_assertion_workunits(builder, definition) == []
    assert report.assertions_emitted == 0


def test_resolver_lowercases_snowflake_by_default() -> None:
    # Snowflake emits lowercased URNs, so MC must lowercase to match even without
    # the convert_urns_to_lowercase flag set.
    mcon = "MCON++acct++wh-1++table++DB.SCH.TBL"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="DB.SCH.TBL", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-1": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "db.sch.tbl" in urn and "DB.SCH.TBL" not in urn


def test_resolver_preserves_case_for_case_sensitive_platform() -> None:
    # BigQuery is case-sensitive and its source preserves case, so MC must not
    # lowercase by default.
    mcon = "MCON++acct++wh-1++table++Proj.Dataset.Events"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon,
                full_table_id="Proj.Dataset.Events",
                connection_type="bigquery",
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-1": {"platform": "bigquery"}})
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "Proj.Dataset.Events" in urn


def test_resolver_converts_full_table_id_colon_to_dot() -> None:
    # Monte Carlo's full_table_id uses its own "database:schema.table" form;
    # DataHub dataset URNs need dot-separated "database.schema.table" so the
    # assertion attaches to the same dataset entity the warehouse source emits.
    mcon = "MCON++acct++wh-1++table++mydb:public.mytable"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon,
                full_table_id="mydb:public.mytable",
                connection_type="snowflake",
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-1": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "mydb.public.mytable" in urn
    assert "mydb:public.mytable" not in urn


def test_resolver_lowercases_urn_when_configured() -> None:
    # The flag forces lowercase even for a case-preserving platform (BigQuery).
    mcon = "MCON++acct++wh-1++table++Proj.Dataset.Events"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon,
                full_table_id="Proj.Dataset.Events",
                connection_type="bigquery",
            )
        }
    )
    cfg = make_config(
        connection_to_platform_map={"wh-1": {"platform": "bigquery"}},
        convert_urns_to_lowercase=True,
    )
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "proj.dataset.events" in urn and "Proj.Dataset.Events" not in urn


def test_get_monitors_paginates_with_offset() -> None:
    # getMonitors returns a plain list, so the client must walk it with
    # limit/offset; verify monitors past the first page are still fetched.
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 2
    client.report = None
    pages: Dict[int, List[Dict[str, Any]]] = {
        0: [{"uuid": "m1"}, {"uuid": "m2"}],
        2: [{"uuid": "m3"}],
    }
    seen_offsets: List[int] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        offset = variables["offset"]
        seen_offsets.append(offset)
        return {"get_monitors": pages.get(offset, [])}

    client._call = fake_call  # type: ignore[method-assign]
    uuids = [m.uuid for m in client.get_monitors()]
    assert uuids == ["m1", "m2", "m3"]
    # Stops after the short second page rather than requesting a third.
    assert seen_offsets == [0, 2]


def test_get_monitors_reports_records_missing_uuid() -> None:
    # A record without a uuid is skipped and surfaced in the report, not silently
    # dropped to logs only.
    report = MonteCarloSourceReport()
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = report
    client._call = lambda query, variables: {  # type: ignore[method-assign]
        "get_monitors": [{"uuid": "m1"}, {"name": "no-uuid"}]
    }
    uuids = [m.uuid for m in client.get_monitors()]
    assert uuids == ["m1"]
    assert len(report.warnings) == 1


def test_build_assertion_warns_on_empty_entity_mcons() -> None:
    """Monitors with no entity_mcons produce a warning and no workunits."""
    report = MonteCarloSourceReport()
    cfg = make_config()
    resolver = MconResolver(cfg, FakeResolverClient({}), report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    definition = MonteCarloAssertionDef(uuid="mon-empty", entity_mcons=[])
    assert _build_assertion_workunits(builder, definition) == []
    assert len(report.warnings) == 1


def test_build_assertion_filtered_by_monitor_pattern() -> None:
    """monitor_pattern deny rules drop the assertion and record it as filtered."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(monitor_pattern={"deny": ["^Freshness.*"]})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    definition = MonteCarloAssertionDef(
        uuid="mon-fresh", name="Freshness on orders", entity_mcons=[mcon]
    )
    assert _build_assertion_workunits(builder, definition) == []
    assert report.assertions_emitted == 0
    assert "Freshness on orders" in report.filtered


# --- Structured CustomAssertionInfo fields ---


def _builder_with_resolved_snowflake(
    report: MonteCarloSourceReport, mcon: str
) -> MonteCarloAssertionBuilder:
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    return MonteCarloAssertionBuilder(cfg, report, resolver)


def _custom_assertion(
    builder: MonteCarloAssertionBuilder, definition: MonteCarloAssertionDef
) -> CustomAssertionInfoClass:
    """Return the CustomAssertionInfo aspect emitted for a definition."""
    aspects = [_aspect(wu) for wu in _build_assertion_workunits(builder, definition)]
    info = next(a for a in aspects if isinstance(a, AssertionInfoClass))
    assert info.customAssertion is not None
    return info.customAssertion


def test_no_comparisons_keeps_only_monitor_uuid_in_custom_properties() -> None:
    """Without comparisons, native fields move to nativeType/nativeParameters
    and customProperties keeps only mc_monitor_uuid. The no-field case maps
    to DATASET_ROWS with _NATIVE_ operator/aggregation — dbt's
    unknown-test-no-column fallback — so the structured-rendering path
    still fires."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-1",
        monitor_type="FRESHNESS",
        entity_mcons=[mcon],
        resource_id="wh-2",
        severity="SEV-2",
        data_quality_dimension="FRESHNESS",
    )
    info = _custom_assertion(builder, definition)
    assert info.scope == DatasetAssertionScopeClass.DATASET_ROWS
    assert info.operator == AssertionStdOperatorClass._NATIVE_
    assert info.aggregation == AssertionStdAggregationClass._NATIVE_
    assert info.fields is None
    assert info.parameters is None
    assert info.nativeType == "FRESHNESS"
    assert info.nativeParameters == {
        "severity": "SEV-2",
        "data_quality_dimension": "FRESHNESS",
        "resource_id": "wh-2",
    }
    # customProperties is reduced to the single correlation key. (customProperties
    # lives on AssertionInfo, not on CustomAssertionInfo.)
    aspects = [_aspect(wu) for wu in _build_assertion_workunits(builder, definition)]
    info_aspect = next(a for a in aspects if isinstance(a, AssertionInfoClass))
    assert info_aspect.customProperties == {"mc_monitor_uuid": "mon-1"}


def test_column_comparison_sets_dataset_column_scope_and_fields() -> None:
    """A comparison with a field maps to DATASET_COLUMN scope with a schema-field URN."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-null-email",
        monitor_type="METRIC",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(
                comparison_type="THRESHOLD",
                operator="LTE",
                metric="null_rate",
                field="email",
                threshold=0.05,
            )
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.scope == DatasetAssertionScopeClass.DATASET_COLUMN
    assert info.operator == AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO
    assert info.aggregation == AssertionStdAggregationClass.NULL_PROPORTION
    assert info.fields is not None and len(info.fields) == 1
    assert info.fields[0].endswith(",email)")
    assert info.field == info.fields[0]
    assert info.parameters is not None
    assert info.parameters.value is not None
    assert info.parameters.value.value == "0.05"
    assert info.nativeType == "METRIC"
    assert info.nativeParameters is not None
    assert info.nativeParameters["comparison_type"] == "THRESHOLD"
    assert info.nativeParameters["metric"] == "null_rate"


def test_multi_column_comparison_populates_fields_list() -> None:
    """fields (multi-column) yields multiple schema-field URNs."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-multi",
        monitor_type="METRIC",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(
                operator="IS_NOT_NULL",
                fields=["col_a", "col_b"],
            )
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.scope == DatasetAssertionScopeClass.DATASET_COLUMN
    assert info.operator == AssertionStdOperatorClass.NOT_NULL
    assert info.fields is not None and len(info.fields) == 2
    assert info.fields[0].endswith(",col_a)") and info.fields[1].endswith(",col_b)")
    # NOT_NULL takes no parameters.
    assert info.parameters is None


def test_no_field_comparison_falls_back_to_dataset_rows() -> None:
    """A table-level comparison (no field/fields) maps to DATASET_ROWS."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-vol",
        monitor_type="VOLUME",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(
                comparison_type="ABSOLUTE_VOLUME",
                operator="GT",
                metric="row_count",
                threshold=1000,
            )
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.scope == DatasetAssertionScopeClass.DATASET_ROWS
    assert info.operator == AssertionStdOperatorClass.GREATER_THAN
    assert info.aggregation == AssertionStdAggregationClass.ROW_COUNT
    assert info.fields is None
    assert info.parameters is not None and info.parameters.value is not None
    assert info.parameters.value.value == "1000.0"


def test_inside_range_operator_maps_to_between_with_min_max() -> None:
    """INSIDE_RANGE → BETWEEN with minValue/maxValue from lower/upperThreshold."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-range",
        monitor_type="METRIC",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(
                operator="INSIDE_RANGE",
                metric="row_count",
                lower_threshold=100,
                upper_threshold=5000,
            )
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.operator == AssertionStdOperatorClass.BETWEEN
    assert info.parameters is not None
    assert info.parameters.minValue is not None and info.parameters.maxValue is not None
    assert info.parameters.minValue.value == "100.0"
    assert info.parameters.maxValue.value == "5000.0"


def test_unmapped_operator_falls_back_to_native() -> None:
    """AUTO / OUTSIDE_RANGE / NOOP have no clean DataHub operator → _NATIVE_."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-auto",
        monitor_type="ANOMALY",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(operator="AUTO", metric="row_count"),
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.operator == AssertionStdOperatorClass._NATIVE_


def test_native_operator_preserves_threshold_on_native_parameters() -> None:
    """A native (unmapped) operator with a scalar threshold keeps it on the
    structured parameters.value slot (via _std_parameters' fallthrough). The
    scalar case is NOT duplicated onto nativeParameters — only range bounds
    are surfaced there (see the range test)."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-native-thresh",
        monitor_type="ANOMALY",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(operator="AUTO", metric="row_count", threshold=42),
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.operator == AssertionStdOperatorClass._NATIVE_
    # Scalar threshold is preserved on the structured parameters slot.
    assert info.parameters is not None
    assert info.parameters.value is not None
    assert info.parameters.value.value == "42.0"
    assert info.nativeParameters is not None
    # Scalar threshold is not duplicated onto nativeParameters.
    assert "threshold" not in info.nativeParameters


def test_native_operator_preserves_range_thresholds_on_native_parameters() -> None:
    """A native range operator (e.g. OUTSIDE_RANGE) sets lower/upper thresholds
    instead of a scalar threshold; _std_parameters only handles the standard
    BETWEEN operator, so without surfacing them on nativeParameters the bounds
    would be silently dropped. This is the core fix for the dropped-thresholds
    bug."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-outside-range",
        monitor_type="METRIC",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(
                operator="OUTSIDE_RANGE",
                metric="row_count",
                lower_threshold=10,
                upper_threshold=100,
            ),
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.operator == AssertionStdOperatorClass._NATIVE_
    # No scalar threshold → parameters is None; bounds live on nativeParameters.
    assert info.parameters is None
    assert info.nativeParameters is not None
    assert info.nativeParameters["lower_threshold"] == "10.0"
    assert info.nativeParameters["upper_threshold"] == "100.0"


def test_standard_operator_does_not_duplicate_threshold_on_native_parameters() -> None:
    """A standard operator (GT) puts the threshold on parameters.value; it must
    NOT also be duplicated onto nativeParameters (only _NATIVE_ operators
    surface range bounds there)."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-gt",
        monitor_type="VOLUME",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(operator="GT", metric="row_count", threshold=1000),
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.operator == AssertionStdOperatorClass.GREATER_THAN
    assert info.parameters is not None and info.parameters.value is not None
    assert info.nativeParameters is not None
    assert "threshold" not in info.nativeParameters
    assert "lower_threshold" not in info.nativeParameters
    assert "upper_threshold" not in info.nativeParameters


def test_unmapped_metric_falls_back_to_native_aggregation() -> None:
    """An unknown metric string → _NATIVE_ aggregation (gaps are safe)."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-custom-metric",
        monitor_type="METRIC",
        entity_mcons=[mcon],
        comparisons=[
            MonteCarloComparison(operator="GT", metric="some_exotic_metric"),
        ],
    )
    info = _custom_assertion(builder, definition)
    assert info.aggregation == AssertionStdAggregationClass._NATIVE_
    # operator is still mapped (GT → GREATER_THAN); only the aggregation is native.
    assert info.operator == AssertionStdOperatorClass.GREATER_THAN


def test_compound_rule_folds_remaining_comparisons_into_logic() -> None:
    """A multi-comparison rule maps comparisons[0] structurally and folds the
    rest into logic as JSON."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    definition = MonteCarloAssertionDef(
        uuid="mon-compound",
        monitor_type="METRIC",
        entity_mcons=[mcon],
        custom_sql="SELECT 1",
        comparisons=[
            MonteCarloComparison(operator="GT", metric="row_count", threshold=10),
            MonteCarloComparison(operator="LT", metric="row_count", threshold=100),
        ],
    )
    info = _custom_assertion(builder, definition)
    # First comparison drives the structured fields.
    assert info.operator == AssertionStdOperatorClass.GREATER_THAN
    # logic carries custom_sql plus the JSON of the remaining comparison.
    assert info.logic is not None
    assert "SELECT 1" in info.logic
    assert '"operator": "LT"' in info.logic or '"operator":"LT"' in info.logic


def test_build_run_event_links_to_ingested_monitor() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    _build_assertion_workunits(
        builder, MonteCarloAssertionDef(uuid="mon-1", entity_mcons=[mcon])
    )

    alert = MonteCarloAlert(
        uuid="alert-1",
        monitor_uuids=["mon-1"],
        severity="SEV-2",
        created_time="2026-05-01T00:00:00+00:00",
    )
    wus = list(builder.build_run_event(alert))
    assert len(wus) == 1
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.runId == "alert-1"
    assert run_event.result is not None and run_event.result.type == "FAILURE"
    # The run event must bind to the ingested monitor's assertion and its dataset,
    # not to swapped/blank URNs — mutation-tested to fail if either is wrong.
    assert run_event.assertionUrn == builder._assertion_urn("mon-1")
    assert run_event.asserteeUrn == resolver.dataset_urn_for_mcon(mcon)
    assert report.run_events_emitted == 1


def test_build_run_event_skips_unknown_monitor() -> None:
    report = MonteCarloSourceReport()
    cfg = make_config()
    resolver = MconResolver(cfg, FakeResolverClient({}), report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    alert = MonteCarloAlert(
        uuid="alert-2",
        monitor_uuids=["ghost"],
        created_time="2026-05-01T00:00:00+00:00",
    )
    assert list(builder.build_run_event(alert)) == []
    assert report.run_events_emitted == 0


def test_build_run_event_uses_first_ingested_monitor_uuid() -> None:
    """An alert listing several monitor UUIDs attaches a run event to the first
    one we ingested an assertion for — not just monitor_uuids[0]. Previously an
    incident whose first listed monitor was filtered out / unresolved was
    silently dropped even when a later monitor was ingested."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    # Ingest only "mon-2"; "mon-1" and "mon-3" are not ingested (filtered /
    # unresolved).
    _build_assertion_workunits(
        builder, MonteCarloAssertionDef(uuid="mon-2", entity_mcons=[mcon])
    )

    alert = MonteCarloAlert(
        uuid="alert-1",
        monitor_uuids=["mon-1", "mon-2", "mon-3"],
        severity="SEV-2",
        created_time="2026-05-01T00:00:00+00:00",
    )
    wus = list(builder.build_run_event(alert))
    assert len(wus) == 1
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.runId == "alert-1"
    # The run event attaches to mon-2's assertion, proving the second UUID was used.
    assert run_event.assertionUrn == builder._assertion_urn("mon-2")
    assert report.run_events_emitted == 1


def test_build_assertion_failed_emit_does_not_register_monitor() -> None:
    """If _emit_assertion raises, the monitor must NOT be registered in
    _ingested_by_monitor — otherwise build_run_event would later bind a failure
    run event to an assertion that was never created this run (Bugbot finding)."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)

    def boom(*args: Any, **kwargs: Any) -> Iterator[MetadataWorkUnit]:
        raise RuntimeError("emit exploded")

    builder._emit_assertion = boom  # type: ignore[method-assign]
    definition = MonteCarloAssertionDef(uuid="mon-1", entity_mcons=[mcon])

    with pytest.raises(RuntimeError):
        list(builder.build_assertion(definition))

    # The monitor was never emitted, so it must not be registered — a later
    # alert for it must be skipped, not bound to a non-existent assertion.
    assert "mon-1" not in builder._ingested_by_monitor
    assert report.assertions_emitted == 0

    alert = MonteCarloAlert(
        uuid="alert-x",
        monitor_uuids=["mon-1"],
        created_time="2026-05-01T00:00:00+00:00",
    )
    assert list(builder.build_run_event(alert)) == []
    assert report.run_events_emitted == 0


def test_build_run_event_warns_on_unmatched_alert() -> None:
    """An alert whose monitors were not ingested must produce no run event AND
    emit a report warning (not silently drop), so the operator can see the
    skipped alert in the run report."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(connection_to_platform_map={"wh-2": {"platform": "snowflake"}})
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)

    # Alert for a monitor we never ingested, with only asset_mcons.
    alert = MonteCarloAlert(
        uuid="alert-y",
        monitor_uuids=[],
        asset_mcons=[mcon],
        created_time="2026-05-01T00:00:00+00:00",
    )
    assert list(builder.build_run_event(alert)) == []
    assert report.run_events_emitted == 0
    # The drop must be visible in the report, not silent.
    titles = [w.title for w in report.warnings]
    assert any("no ingested monitor" in (t or "") for t in titles)


def _client_with_responses(responses: List[Dict[str, Any]]) -> MonteCarloClient:
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = None
    calls = {"i": 0}

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        resp = responses[calls["i"]]
        calls["i"] += 1
        return resp

    client._client = None  # type: ignore[assignment]
    client._call = fake_call  # type: ignore[method-assign]
    return client


def test_client_get_monitors_parses_list() -> None:
    client = _client_with_responses(
        [
            {
                "get_monitors": [
                    {
                        "uuid": "m1",
                        "name": "n1",
                        "monitor_type": "FRESHNESS",
                        "entity_mcons": ["MCON++a++b++table++c"],
                        "resource_id": "wh-1",
                    }
                ]
            }
        ]
    )
    monitors = list(client.get_monitors())
    assert len(monitors) == 1
    assert monitors[0].uuid == "m1"
    assert monitors[0].native_type == "FRESHNESS"


def test_parse_comparisons_flattens_custom_metric_object() -> None:
    """``customMetric`` is a ``CustomMetric`` object on the MCD schema (not a
    scalar); the query selects ``{ uuid metricName }`` and pycarlo snake_cases
    it to ``custom_metric: {uuid, metric_name}``. The parser must flatten that
    to the human-readable ``metric_name`` string on ``MonteCarloComparison``."""
    raw: List[Dict[str, Any]] = [
        {
            "comparison_type": "THRESHOLD",
            "operator": "GT",
            "metric": None,
            "custom_metric": {"uuid": "cm-1", "metric_name": "revenue_per_order"},
            "field": "total",
            "fields": [],
            "threshold": 100,
            "upper_threshold": None,
            "lower_threshold": None,
        }
    ]
    parsed = _parse_comparisons(raw)
    assert len(parsed) == 1
    c = parsed[0]
    assert c.custom_metric == "revenue_per_order"
    assert c.operator == "GT"
    assert c.field == "total"


def test_parse_comparisons_tolerates_scalar_custom_metric() -> None:
    """A scalar ``custom_metric`` (e.g. from an older fixture or a hand-built
    dict) still validates, since ``MonteCarloComparison.custom_metric`` is
    ``Optional[str]`` and the flattener only acts on dicts."""
    parsed = _parse_comparisons([{"operator": "EQ", "custom_metric": "legacy"}])
    assert len(parsed) == 1
    assert parsed[0].custom_metric == "legacy"


def test_parse_comparisons_drops_malformed_entry() -> None:
    """A malformed comparison is dropped rather than aborting the whole list."""
    parsed = _parse_comparisons(
        [
            "not-a-dict",  # dropped (not a dict)
            {"operator": "EQ"},  # kept
        ]
    )
    assert len(parsed) == 1
    assert parsed[0].operator == "EQ"


def test_client_get_monitors_forwards_domain_ids() -> None:
    """domain_ids are forwarded to the API as a GraphQL variable."""
    captured: Dict[str, Any] = {}

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        captured.update(variables)
        return {"get_monitors": []}

    cfg = make_config(domain_ids=["dom-1"])
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = cfg
    client.page_size = 100
    client.report = None
    client._call = fake_call  # type: ignore[method-assign]

    list(client.get_monitors())
    assert captured.get("domainIds") == ["dom-1"]


def test_client_get_monitors_filters_by_type_pattern() -> None:
    """monitor_type_pattern filters monitors client-side by their monitorType."""
    cfg = make_config(monitor_type_pattern={"allow": ["FRESHNESS"]})
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = cfg
    client.page_size = 100
    client.report = None
    client._call = lambda query, variables: {  # type: ignore[method-assign]
        "get_monitors": [
            {"uuid": "m1", "monitor_type": "FRESHNESS"},
            {"uuid": "m2", "monitor_type": "VOLUME"},
        ]
    }
    monitors = list(client.get_monitors())
    assert [m.uuid for m in monitors] == ["m1"]


def test_client_get_monitors_resolves_table_monitor_entity_mcons() -> None:
    """A TABLE monitor with no entityMcons is resolved via getTableMonitor's
    FULL_TABLE_ID filter, then getTable(dwId, fullTableId) for the MCON."""
    calls: List[str] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if "getMonitors" in query:
            return {
                "get_monitors": [
                    {
                        "uuid": "m1",
                        "monitor_type": "TABLE",
                        "entity_mcons": [],
                        "resource_id": "wh-1",
                    }
                ]
            }
        if "getTableMonitor" in query:
            calls.append("getTableMonitor")
            assert variables == {"monitorUuid": "m1"}
            return {
                "get_table_monitor": {
                    "asset_selection": {
                        "filters": [
                            {"type": "FULL_TABLE_ID", "full_table_id": "db.sch.tbl"}
                        ]
                    }
                }
            }
        if "getTable" in query:
            calls.append("getTable")
            assert variables == {"dwId": "wh-1", "fullTableId": "db.sch.tbl"}
            return {"get_table": {"mcon": "MCON++a++wh-1++table++db.sch.tbl"}}
        raise AssertionError(f"unexpected query: {query}")

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = None
    client._call = fake_call  # type: ignore[method-assign]

    monitors = list(client.get_monitors())
    assert len(monitors) == 1
    assert monitors[0].entity_mcons == ["MCON++a++wh-1++table++db.sch.tbl"]
    assert calls == ["getTableMonitor", "getTable"]


def test_client_get_monitors_table_monitor_without_full_table_id_filter() -> None:
    """A TABLE monitor scoped by a pattern filter (not FULL_TABLE_ID) has no
    fixed table list to resolve here, so it's left with empty entity_mcons."""
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = None
    calls: List[str] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if "getMonitors" in query:
            calls.append("getMonitors")
            return {
                "get_monitors": [
                    {
                        "uuid": "m1",
                        "monitor_type": "TABLE",
                        "entity_mcons": [],
                        "resource_id": "wh-1",
                    }
                ]
            }
        if "getTableMonitor" in query:
            calls.append("getTableMonitor")
            return {
                "get_table_monitor": {
                    "asset_selection": {"filters": [{"type": "TABLE_TAG"}]}
                }
            }
        raise AssertionError(f"unexpected query: {query}")

    client._call = fake_call  # type: ignore[method-assign]
    monitors = list(client.get_monitors())
    assert monitors[0].entity_mcons == []
    # No FULL_TABLE_ID filter was present, so getTable must never be called —
    # proves the pattern-filter case is genuinely skipped, not accidentally
    # resolved to an empty result some other way.
    assert calls == ["getMonitors", "getTableMonitor"]


@pytest.mark.parametrize("failing_query", ["getTableMonitor", "getTable"])
def test_client_get_monitors_table_monitor_auth_error_is_fatal(
    failing_query: str,
) -> None:
    """A MonteCarloAuthError raised while resolving a TABLE monitor's scope
    (either the getTableMonitor or the getTable call) must propagate unwrapped,
    not be demoted to a per-monitor warning like a recoverable failure would be."""
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = None

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if "getMonitors" in query:
            return {
                "get_monitors": [
                    {
                        "uuid": "m1",
                        "monitor_type": "TABLE",
                        "entity_mcons": [],
                        "resource_id": "wh-1",
                    }
                ]
            }
        if "getTableMonitor" in query:
            if failing_query == "getTableMonitor":
                raise MonteCarloAuthError("bad credentials")
            return {
                "get_table_monitor": {
                    "asset_selection": {
                        "filters": [
                            {"type": "FULL_TABLE_ID", "full_table_id": "db.sch.tbl"}
                        ]
                    }
                }
            }
        if "getTable" in query:
            raise MonteCarloAuthError("bad credentials")
        raise AssertionError(f"unexpected query: {query}")

    client._call = fake_call  # type: ignore[method-assign]
    with pytest.raises(MonteCarloAuthError):
        list(client.get_monitors())


def test_client_get_custom_rules_paginates() -> None:
    page1 = {
        "get_custom_rules": {
            "edges": [
                {
                    "node": {
                        "uuid": "r1",
                        "rule_type": "CUSTOM_SQL",
                        "custom_sql": "select 1",
                    }
                }
            ],
            "page_info": {"has_next_page": True, "end_cursor": "c1"},
        }
    }
    page2 = {
        "get_custom_rules": {
            "edges": [{"node": {"uuid": "r2", "rule_type": "CUSTOM_SQL"}}],
            "page_info": {"has_next_page": False, "end_cursor": None},
        }
    }
    client = _client_with_responses([page1, page2])
    rules = list(client.get_custom_rules())
    assert [r.uuid for r in rules] == ["r1", "r2"]
    assert rules[0].custom_sql == "select 1"


def test_client_get_custom_rules_populates_name_from_rule_name() -> None:
    # The MCD CustomRule type exposes `ruleName` (snake_cased to `rule_name` by
    # pycarlo), not `name`. The client must surface it on
    # MonteCarloAssertionDef.name so monitor_pattern can filter rules by name
    # (otherwise rules match against their UUID only).
    page = {
        "get_custom_rules": {
            "edges": [
                {
                    "node": {
                        "uuid": "r1",
                        "rule_name": "Orders non-negative",
                        "rule_type": "CUSTOM_SQL",
                    }
                },
            ],
            "page_info": {"has_next_page": False, "end_cursor": None},
        }
    }
    client = _client_with_responses([page])
    rules = list(client.get_custom_rules())
    assert rules[0].name == "Orders non-negative"


def test_client_get_table_parses_connection_type() -> None:
    client = _client_with_responses(
        [
            {
                "get_table": {
                    "mcon": "MCON++a++b++table++db.sch.tbl",
                    "full_table_id": "db.sch.tbl",
                    "warehouse": {"connection_type": "snowflake"},
                }
            }
        ]
    )
    table = client.get_table("MCON++a++b++table++db.sch.tbl")
    assert table is not None
    assert table.full_table_id == "db.sch.tbl"
    assert table.connection_type == "snowflake"


def test_resolver_non_obvious_connection_types() -> None:
    """sql-server and synapse both map to mssql (non-obvious aliases), but only
    when auto_map_connection_types is enabled."""
    mcon = "MCON++acct++wh++table++db.sch.tbl"
    for connection_type in ("sql-server", "synapse"):
        client = FakeResolverClient(
            {
                mcon: ResolvedTable(
                    mcon=mcon,
                    full_table_id="db.sch.tbl",
                    connection_type=connection_type,
                )
            }
        )
        urn = MconResolver(
            make_config(auto_map_connection_types=True),
            client,
            MonteCarloSourceReport(),
        ).dataset_urn_for_mcon(mcon)
        assert urn is not None
        assert "mssql" in urn, f"{connection_type} should resolve to mssql"


def _bare_source() -> MonteCarloSource:
    # Bypass __init__ (which constructs a pycarlo-backed client) to unit-test the
    # per-phase emit logic in isolation.
    source = MonteCarloSource.__new__(MonteCarloSource)
    source.report = MonteCarloSourceReport()
    return source


def test_emit_reports_fetch_failure_as_failure_not_crash() -> None:
    source = _bare_source()

    def fetch():
        raise RuntimeError("api down")

    wus = list(
        source._emit(
            "monitor",
            fetch,
            source.report.report_monitor_scanned,
            lambda item: iter(()),
        )
    )
    assert wus == []
    assert len(source.report.failures) == 1


def test_emit_skips_failing_item_and_continues() -> None:
    source = _bare_source()
    items = [MonteCarloAssertionDef(uuid="a"), MonteCarloAssertionDef(uuid="b")]

    def build(item: MonteCarloAssertionDef) -> Iterator[MetadataWorkUnit]:
        if item.uuid == "a":
            raise ValueError("bad monitor")
        yield cast(MetadataWorkUnit, "wu-b")  # sentinel; _emit only passes it through

    wus = list(
        source._emit(
            "monitor", lambda: items, source.report.report_monitor_scanned, build
        )
    )
    assert wus == ["wu-b"]  # 'b' still emitted after 'a' failed
    assert source.report.monitors_scanned == 2
    assert len(source.report.warnings) == 1


def test_alert_tolerates_malformed_created_time() -> None:
    # A non-ISO/garbage timestamp is nulled rather than raising, so one bad alert
    # doesn't abort the whole alert page.
    assert (
        MonteCarloAlert(uuid="x", created_time="not-a-timestamp").created_time is None
    )
    parsed = MonteCarloAlert(uuid="y", created_time="2026-05-20T08:00:00Z").created_time
    assert parsed is not None and parsed.year == 2026


# --- Rate limiting ---


def test_call_retries_on_429_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    class RateLimitedError(Exception):
        status_code = 429

    report = MonteCarloSourceReport()
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = report
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None
    attempts = {"n": 0}

    def flaky(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RateLimitedError()
        return {"ok": True}

    client._client = flaky  # type: ignore[assignment]
    result = client._call("query {ok}", {})
    assert result == {"ok": True}
    assert attempts["n"] == 3
    # Verify the user actually sees rate-limit feedback via the real report
    # (not just the retry-count mechanics) — this is the production path,
    # since client.report is None only during test_connection(). Both retries
    # share one title+message, so they aggregate into a single entry with
    # one context string per retry (see SourceReport.report_log's log_key).
    assert len(report.warnings) == 1
    assert report.warnings[0].title == "Monte Carlo API call failed; retrying"
    assert report.warnings[0].message.startswith("rate limited (429)")
    assert len(report.warnings[0].context) == 2


def test_call_gives_up_after_max_retries_on_persistent_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    class RateLimitedError(Exception):
        status_code = 429

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = None
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None
    attempts = {"n": 0}

    def always_limited(
        query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        attempts["n"] += 1
        raise RateLimitedError()

    client._client = always_limited  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="Monte Carlo API call failed"):
        client._call("query {ok}", {})
    assert attempts["n"] == 6  # 1 initial attempt + 5 retries


def test_call_acquires_daily_budget_then_token_bucket_per_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_call() must check the daily budget before pacing through the token
    bucket, so a request that would blow the daily cap fails fast rather than
    waiting out a token-bucket delay first. Both must be re-acquired on EVERY
    physical attempt (including 429 retries), since each attempt is a real
    HTTP request against Monte Carlo's own quota — not just once per logical
    call."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    order: List[str] = []

    class RateLimitedError(Exception):
        status_code = 429

    class TrackingLimiter:
        def acquire(self) -> None:
            order.append(self.name)  # type: ignore[attr-defined]

    daily = TrackingLimiter()
    daily.name = "daily"  # type: ignore[attr-defined]
    bucket = TrackingLimiter()
    bucket.name = "bucket"  # type: ignore[attr-defined]

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = None
    client.page_size = 100
    client._daily_budget = daily  # type: ignore[assignment]
    client._token_bucket = bucket  # type: ignore[assignment]
    attempts = {"n": 0}

    def flaky(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RateLimitedError()
        return {"ok": True}

    client._client = flaky  # type: ignore[assignment]

    client._call("query {ok}", {})
    # 3 attempts (2 retries + success) -> daily+bucket acquired 3 times each,
    # daily always before bucket within each attempt.
    assert order == ["daily", "bucket"] * 3


def test_call_daily_budget_exhaustion_mid_retry_is_not_masked_by_429_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the daily budget is exhausted on a retry attempt (not the first),
    DailyCallBudgetExceeded must propagate with its own distinct message —
    not get caught and re-wrapped into the generic 'Monte Carlo API call
    failed' RuntimeError that persistent-429s raise."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    class RateLimitedError(Exception):
        status_code = 429

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = None
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = DailyCallBudget(daily_limit=1)
    attempts = {"n": 0}

    def flaky_then_over_budget(
        query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        # The daily budget's own acquire() is what raises on the 2nd attempt,
        # so this callable is never actually invoked a 2nd time.
        attempts["n"] += 1
        raise RateLimitedError()

    client._client = flaky_then_over_budget  # type: ignore[assignment]
    with pytest.raises(DailyCallBudgetExceeded, match="call budget"):
        client._call("query {ok}", {})
    # Exhausted on the 2nd attempt (the 1st consumed the only budget slot).
    assert attempts["n"] == 1


def test_call_does_not_retry_non_429_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls = []
    monkeypatch.setattr(time, "sleep", lambda seconds: sleep_calls.append(seconds))

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = None
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None

    class BadRequestError(Exception):
        status_code = 400

    def bad_request(
        query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        raise BadRequestError()

    client._client = bad_request  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="Monte Carlo API call failed"):
        client._call("query {ok}", {})
    assert sleep_calls == []


@pytest.mark.parametrize("status_code", [401, 403])
def test_call_raises_auth_error_on_401_403(
    status_code: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 401/403 becomes MonteCarloAuthError (fatal), not the generic RuntimeError
    wrapper, so bad credentials abort rather than degrade to per-asset warnings."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    class AuthError(Exception):
        pass

    AuthError.status_code = status_code  # type: ignore[attr-defined]

    def raise_auth(query: str, variables: Optional[Dict[str, Any]] = None) -> None:
        raise AuthError()

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = None
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None
    client._client = raise_auth  # type: ignore[assignment]
    with pytest.raises(MonteCarloAuthError, match="rejected the API credentials"):
        client._call("query {ok}", {})


def test_emit_propagates_fatal_run_error_from_fetch() -> None:
    """A run-level fatal (exhausted budget / bad creds) raised while fetching must
    abort, not be demoted to a phase-level failure like an ordinary fetch error."""
    source = _bare_source()

    def fetch() -> Iterator[MonteCarloAssertionDef]:
        raise DailyCallBudgetExceeded("budget exhausted")
        yield  # pragma: no cover - makes fetch a generator

    with pytest.raises(DailyCallBudgetExceeded):
        list(
            source._emit(
                "monitor",
                fetch,
                source.report.report_monitor_scanned,
                lambda i: iter(()),
            )
        )
    assert len(source.report.failures) == 0


def test_emit_propagates_fatal_run_error_from_build() -> None:
    """A fatal raised while building a single item must abort the run, not be
    demoted to a per-item warning that lets the loop keep consuming quota."""
    source = _bare_source()
    items = [MonteCarloAssertionDef(uuid="a")]

    def build(item: MonteCarloAssertionDef) -> Iterator[MetadataWorkUnit]:
        raise MonteCarloAuthError("bad credentials")
        yield  # pragma: no cover - makes build a generator

    with pytest.raises(MonteCarloAuthError):
        list(
            source._emit(
                "monitor", lambda: items, source.report.report_monitor_scanned, build
            )
        )
    assert len(source.report.warnings) == 0


def test_get_workunits_warns_when_all_assertions_skipped() -> None:
    """If monitors/rules are scanned but zero assertions are emitted (e.g. a
    misconfigured connection_to_platform_map), the run must surface a distinct
    failure rather than a misleading 'successful' empty run."""
    source = MonteCarloSource.__new__(MonteCarloSource)
    source.config = make_config(include_assertions=True, include_alerts=False)
    source.report = MonteCarloSourceReport()

    # A client whose get_monitors / get_custom_rules return items that all fail
    # to resolve to a dataset URN (empty entity_mcons → build_assertion warns
    # and yields nothing).
    class StubClient:
        def get_monitors(self):
            yield MonteCarloAssertionDef(uuid="m1", entity_mcons=[])
            yield MonteCarloAssertionDef(uuid="m2", entity_mcons=[])

        def get_custom_rules(self):
            return iter(())

    source.client = StubClient()  # type: ignore[assignment]

    class StubResolver:
        def dataset_urn_for_mcon(self, mcon: str) -> Optional[str]:
            return None

    source.builder = MonteCarloAssertionBuilder(  # type: ignore[arg-type]
        source.config,
        source.report,
        StubResolver(),  # type: ignore[arg-type]
    )

    wus = list(source.get_workunits_internal())
    assert wus == []
    # Scanned 2 monitors, emitted 0 assertions → guard fires a failure.
    assert source.report.monitors_scanned == 2
    assert source.report.assertions_emitted == 0
    assert any(
        f.title is not None and "No assertions emitted" in f.title
        for f in source.report.failures
    )


def test_get_workunits_no_guard_when_assertions_emitted() -> None:
    """The all-failed guard must NOT fire when at least one assertion is emitted."""
    source = MonteCarloSource.__new__(MonteCarloSource)
    source.config = make_config(
        include_assertions=True,
        include_alerts=False,
        connection_to_platform_map={"wh-2": {"platform": "snowflake"}},
    )
    source.report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"

    class StubClient:
        def get_monitors(self):
            yield MonteCarloAssertionDef(uuid="m1", entity_mcons=[mcon])

        def get_custom_rules(self):
            return iter(())

    source.client = StubClient()  # type: ignore[assignment]
    resolver = MconResolver(
        source.config,
        FakeResolverClient(
            {
                mcon: ResolvedTable(
                    mcon=mcon, full_table_id="db.sch.tbl", connection_type="snowflake"
                )
            }
        ),
        source.report,
    )
    source.builder = MonteCarloAssertionBuilder(source.config, source.report, resolver)

    wus = list(source.get_workunits_internal())
    assert len(wus) > 0
    assert source.report.assertions_emitted == 1
    assert len(source.report.failures) == 0


def test_get_workunits_no_guard_when_nothing_scanned() -> None:
    """The guard must not fire when nothing was scanned at all (e.g. an empty
    Monte Carlo account) — that's a legitimate empty run, not a misconfiguration."""
    source = MonteCarloSource.__new__(MonteCarloSource)
    source.config = make_config(include_assertions=True, include_alerts=False)
    source.report = MonteCarloSourceReport()

    class EmptyClient:
        def get_monitors(self):
            return iter(())

        def get_custom_rules(self):
            return iter(())

    source.client = EmptyClient()  # type: ignore[assignment]
    # A real builder is required (its bound method is captured when _emit is
    # called), but it is never invoked since no items are scanned.
    source.builder = MonteCarloAssertionBuilder(  # type: ignore[arg-type]
        source.config,
        source.report,
        MconResolver(source.config, FakeResolverClient({}), source.report),
    )
    wus = list(source.get_workunits_internal())
    assert wus == []
    assert source.report.monitors_scanned == 0
    assert len(source.report.failures) == 0


def test_get_workunits_no_guard_when_all_dropped_by_pattern() -> None:
    """The all-failed guard must NOT fire when every scanned monitor was dropped
    by an intentional name pattern (deny-all / tight filter): scanned > 0 and
    emitted == 0, but the empty result is the user's intent, not a
    misconfiguration. Only monitors that were actually *attempted* (not filtered)
    count toward the guard."""
    source = MonteCarloSource.__new__(MonteCarloSource)
    # A deny-all monitor_pattern: every monitor is dropped during build.
    source.config = make_config(
        include_assertions=True, include_alerts=False, monitor_pattern={"deny": [".*"]}
    )
    source.report = MonteCarloSourceReport()

    class StubClient:
        def get_monitors(self):
            yield MonteCarloAssertionDef(
                uuid="m1", name="alpha", entity_mcons=["MCON++x"]
            )
            yield MonteCarloAssertionDef(
                uuid="m2", name="beta", entity_mcons=["MCON++y"]
            )

        def get_custom_rules(self):
            return iter(())

    source.client = StubClient()  # type: ignore[assignment]
    source.builder = MonteCarloAssertionBuilder(  # type: ignore[arg-type]
        source.config,
        source.report,
        MconResolver(source.config, FakeResolverClient({}), source.report),
    )
    wus = list(source.get_workunits_internal())
    assert wus == []
    # Both scanned, both dropped by pattern, zero emitted — guard must NOT fire.
    assert source.report.monitors_scanned == 2
    assert source.report.dropped == 2
    assert source.report.assertions_emitted == 0
    assert len(source.report.failures) == 0


@pytest.mark.parametrize(
    "fatal", [DailyCallBudgetExceeded("x"), MonteCarloAuthError("x")]
)
def test_resolver_propagates_fatal_run_error(fatal: Exception) -> None:
    """The resolver's broad except must let run-level fatals through instead of
    demoting them to a per-MCON warning."""

    class FatalClient:
        def get_table(self, mcon: str) -> Optional[ResolvedTable]:
            raise fatal

    resolver = MconResolver(make_config(), FatalClient(), MonteCarloSourceReport())
    with pytest.raises(type(fatal)):
        resolver.dataset_urn_for_mcon("MCON++a++b++table++c")


def test_rate_limit_burst_requires_rate() -> None:
    # burst without a sustained rate would be silently ignored (the token bucket
    # is only built when rate_limit_requests_per_second is set).
    with pytest.raises(ValueError):
        make_config(rate_limit_burst=10)


def test_get_alerts_builds_lookback_window() -> None:
    """alerts_lookback_days maps to the createdTime {after, before} window."""
    captured: Dict[str, Any] = {}

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        captured.update(variables)
        return {"get_alerts": {"edges": [], "page_info": {"has_next_page": False}}}

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config(alerts_lookback_days=7)
    client.page_size = 100
    client.report = None
    client._call = fake_call  # type: ignore[method-assign]
    with time_machine.travel("2026-06-15 00:00:00 +0000", tick=False):
        list(client.get_alerts())
    window = captured["createdTime"]
    assert window["before"].startswith("2026-06-15")
    assert window["after"].startswith("2026-06-08")  # 7 days earlier


def test_paginate_raises_on_null_end_cursor_with_more_pages() -> None:
    # has_next_page True but a null end_cursor is a server contract violation:
    # silently truncating would emit incomplete assertions and could trigger
    # stale deletion of existing ones, so the client must raise rather than
    # break. The raised RuntimeError is caught by source._emit and surfaced as
    # a phase-level report.failure.
    client = _client_with_responses(
        [
            {
                "get_custom_rules": {
                    "edges": [{"node": {"uuid": "r1", "rule_type": "X"}}],
                    "page_info": {"has_next_page": True, "end_cursor": None},
                }
            }
        ]
    )
    with pytest.raises(RuntimeError, match="no endCursor"):
        list(client.get_custom_rules())


def test_paginate_raises_on_repeated_end_cursor() -> None:
    # A server that returns the same endCursor twice would loop forever and
    # exhaust the daily call budget; the client must detect the repeat and
    # raise rather than re-fetching the same page indefinitely.
    page = {
        "get_custom_rules": {
            "edges": [{"node": {"uuid": "r1", "rule_type": "X"}}],
            "page_info": {"has_next_page": True, "end_cursor": "dup"},
        }
    }
    client = _client_with_responses([page, page])
    with pytest.raises(RuntimeError, match="repeated endCursor"):
        list(client.get_custom_rules())


def test_paginate_raises_on_malformed_connection() -> None:
    # A 2xx response missing the connection object (or returning the wrong
    # type) must not be treated as an empty page — silent truncation could
    # trigger stale deletion of existing assertions.
    client = _client_with_responses([{"get_custom_rules": None}])
    with pytest.raises(RuntimeError, match="malformed response"):
        list(client.get_custom_rules())


def test_paginate_offset_raises_on_non_list_root() -> None:
    # getMonitors must return a list; a non-list (e.g. null) is a contract
    # violation and must raise rather than be treated as an empty page.
    client = _client_with_responses([{"get_monitors": None}])
    with pytest.raises(RuntimeError, match="malformed response"):
        list(client.get_monitors())


def test_get_table_missing_full_table_id_warns_and_returns_none() -> None:
    report = MonteCarloSourceReport()
    client = _client_with_responses(
        [{"get_table": {"mcon": "m", "warehouse": {"connection_type": "snowflake"}}}]
    )
    client.report = report
    assert client.get_table("MCON++a++b++table++c") is None
    assert len(report.warnings) == 1


def test_get_monitors_skips_malformed_row_and_continues() -> None:
    """A single malformed monitor row (one that raises during MonteCarloAssertionDef
    construction) must be skipped with a warning, not abort the whole phase."""
    report = MonteCarloSourceReport()
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = report

    client._call = lambda query, variables: {  # type: ignore[method-assign]
        "get_monitors": [
            {"uuid": "good-1", "monitor_type": "FRESHNESS"},
            {"uuid": "bad-1", "monitor_type": "FRESHNESS"},
            {"uuid": "good-2", "monitor_type": "VOLUME"},
        ]
    }
    real_init = MonteCarloAssertionDef.__init__
    call_count = {"n": 0}

    def flaky_init(self, **kwargs):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise ValueError("malformed row")
        real_init(self, **kwargs)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(MonteCarloAssertionDef, "__init__", flaky_init)
    try:
        uuids = [m.uuid for m in client.get_monitors()]
    finally:
        monkey.undo()

    assert uuids == ["good-1", "good-2"]
    # The malformed row surfaces as a warning, not a phase failure.
    assert any(
        w.title is not None and "malformed monitor" in w.title for w in report.warnings
    )
    assert len(report.failures) == 0


def test_get_alerts_skips_malformed_row_and_continues() -> None:
    """A malformed alert row is skipped with a warning, not aborting the phase."""
    report = MonteCarloSourceReport()
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = report
    client._call = lambda query, variables: {  # type: ignore[method-assign]
        "get_alerts": {
            "edges": [
                {"node": {"id": "good-1", "monitor_uuids": ["m1"]}},
                {"node": {"id": "bad-1", "monitor_uuids": ["m1"]}},
                {"node": {"id": "good-2", "monitor_uuids": ["m2"]}},
            ],
            "page_info": {"has_next_page": False},
        }
    }
    real_init = MonteCarloAlert.__init__
    call_count = {"n": 0}

    def flaky_init(self, **kwargs):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise ValueError("malformed alert")
        real_init(self, **kwargs)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(MonteCarloAlert, "__init__", flaky_init)
    try:
        ids = [a.uuid for a in client.get_alerts()]
    finally:
        monkey.undo()

    assert ids == ["good-1", "good-2"]
    assert any(
        w.title is not None and "malformed alert" in w.title for w in report.warnings
    )
    assert len(report.failures) == 0


def test_get_monitors_propagates_fatal_from_row_construction() -> None:
    """A run-level fatal (DailyCallBudgetExceeded) raised during row construction
    must propagate, not be demoted to a per-row warning."""
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 100
    client.report = MonteCarloSourceReport()
    client._call = lambda query, variables: {  # type: ignore[method-assign]
        "get_monitors": [{"uuid": "m1", "monitor_type": "FRESHNESS"}]
    }

    def fatal_init(self, **kwargs):  # type: ignore[no-untyped-def]
        raise DailyCallBudgetExceeded("budget exhausted")

    monkey = pytest.MonkeyPatch()
    monkey.setattr(MonteCarloAssertionDef, "__init__", fatal_init)
    try:
        with pytest.raises(DailyCallBudgetExceeded):
            list(client.get_monitors())
    finally:
        monkey.undo()


def test_call_retries_on_transient_network_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient network failure (a ConnectionError, as raised by the requests
    transport pycarlo uses) is retried with backoff, not treated as a fatal
    phase error. The predicate matches by class name so the connector depends
    only on pycarlo — no direct requests/gql import here."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    class ConnectionError(OSError):
        pass

    report = MonteCarloSourceReport()
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = report
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None
    attempts = {"n": 0}

    def flaky(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise ConnectionError("connection reset")
        return {"ok": True}

    client._client = flaky  # type: ignore[assignment]
    result = client._call("query {ok}", {})
    assert result == {"ok": True}
    assert attempts["n"] == 2
    # The retry surfaces as a warning with the transient-network reason.
    assert any("transient network error" in w.message for w in report.warnings)


def test_call_retries_when_network_error_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-network exception that wraps a transient network error (via
    __cause__) is still retried — the predicate unwraps __cause__/__context__ to
    distinguish a genuine transport blip from a permanent application error."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    class ConnectionError(OSError):
        pass

    class TransportLayerError(RuntimeError):
        pass

    report = MonteCarloSourceReport()
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = report
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None
    attempts = {"n": 0}

    def flaky(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        attempts["n"] += 1
        if attempts["n"] < 2:
            try:
                raise ConnectionError("connection reset")
            except ConnectionError as exc:
                raise TransportLayerError("transport failure") from exc
        return {"ok": True}

    client._client = flaky  # type: ignore[assignment]
    result = client._call("query {ok}", {})
    assert result == {"ok": True}
    assert attempts["n"] == 2
    assert any("transient network error" in w.message for w in report.warnings)


def test_call_does_not_retry_permanent_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent, non-network error (e.g. a malformed GraphQL application
    error) is NOT retried — retrying would burn the daily call budget. It
    propagates (wrapped as RuntimeError) on the first attempt."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    client = MonteCarloClient.__new__(MonteCarloClient)
    client.report = None
    client.page_size = 100
    client._token_bucket = None
    client._daily_budget = None
    attempts = {"n": 0}

    def flaky(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        attempts["n"] += 1
        raise ValueError("permanent graphql application error")

    client._client = flaky  # type: ignore[assignment]
    with pytest.raises(RuntimeError):
        client._call("query {ok}", {})
    # Not retryable → exactly one attempt, no backoff.
    assert attempts["n"] == 1
