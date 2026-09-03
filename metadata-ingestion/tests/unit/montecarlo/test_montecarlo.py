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
    MonteCarloJobExecution,
    MonteCarloMetricPoint,
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


def test_resolver_falls_back_to_connection_type() -> None:
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
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None and "bigquery" in urn


def test_resolver_uses_default_platform() -> None:
    """default_platform is used when the connection type has no auto-mapping."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-x++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="exotic-db"
            )
        }
    )
    cfg = make_config(default_platform="postgres")
    resolver = MconResolver(cfg, client, report)
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "postgres" in urn
    assert report.mcons_resolved == 1


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
    """Exceptions from client.get_table are caught, warned, and NOT cached —
    the next call retries getTable instead of inheriting a stale None. The
    failure is recorded as a build_failure (transient) so the partial-run
    guard in source.py trips the soft-delete interlock."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-err++table++db.sch.tbl"

    class ErrorClient:
        calls = 0

        def get_table(self, mcon: str) -> Optional[ResolvedTable]:
            ErrorClient.calls += 1
            raise RuntimeError("network error")

    resolver = MconResolver(make_config(), ErrorClient(), report)
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert report.build_failures == 1
    # Transient failures are not cached — the second call retries getTable.
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert ErrorClient.calls == 2
    assert report.build_failures == 2


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


def test_resolver_caches_permanent_none() -> None:
    """When getTable returns None (table genuinely gone), the result is
    permanent and IS cached — the next call for the same MCON must not
    re-call getTable. This is distinct from a transient exception, which
    must not be cached."""
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-gone++table++db.sch.tbl"

    class NoneClient:
        calls = 0

        def get_table(self, mcon: str) -> Optional[ResolvedTable]:
            NoneClient.calls += 1
            return None

    resolver = MconResolver(make_config(), NoneClient(), report)
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert NoneClient.calls == 1
    assert report.mcons_resolution_failed == 1
    assert report.build_failures == 0


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
    cfg = make_config()
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
    cfg = make_config()
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
    resolver = MconResolver(make_config(), client, MonteCarloSourceReport())
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
    resolver = MconResolver(make_config(), client, MonteCarloSourceReport())
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
    resolver = MconResolver(make_config(), client, MonteCarloSourceReport())
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
    cfg = make_config(convert_urns_to_lowercase=True)
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "proj.dataset.events" in urn and "Proj.Dataset.Events" not in urn


def test_resolver_fallback_uses_target_platform_instance_not_mc_own() -> None:
    # Auto-mapped warehouses must NOT inherit Monte Carlo's own platform_instance
    # (that would stamp the MC instance onto warehouse dataset URNs and attach
    # assertions to datasets that do not exist). target_platform_instance is the
    # warehouse's instance; MC's platform_instance stays on the assertion entity.
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    cfg = make_config(platform_instance="mc_prod", target_platform_instance="wh_prod")
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "wh_prod" in urn
    assert "mc_prod" not in urn


def test_resolver_fallback_omits_platform_instance_when_unset() -> None:
    # With no target_platform_instance, auto-mapped warehouse URNs carry no
    # platform instance rather than MC's own.
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    cfg = make_config(platform_instance="mc_prod")
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "mc_prod" not in urn


def test_resolver_fallback_uses_target_env_override() -> None:
    # target_env overrides the warehouse URN env independently of MC's own env.
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="db.sch.tbl", connection_type="bigquery"
            )
        }
    )
    cfg = make_config(env="DEV", target_env="PROD")
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "PROD" in urn


def test_resolver_per_warehouse_override_preserves_snowflake_case() -> None:
    # A case-preserving Snowflake deployment (warehouse source runs with
    # convert_urns_to_lowercase=false) can now match via the per-warehouse
    # override — previously no config could preserve case for Snowflake/Redshift.
    mcon = "MCON++acct++wh-1++table++DB.SCH.TBL"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="DB.SCH.TBL", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(
        connection_to_platform_map={
            "wh-1": {"platform": "snowflake", "convert_urns_to_lowercase": False}
        }
    )
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "DB.SCH.TBL" in urn and "db.sch.tbl" not in urn


def test_resolver_per_warehouse_override_forces_lowercase_bigquery() -> None:
    # The per-warehouse override can also force lowercase for a normally
    # case-preserving platform.
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
        connection_to_platform_map={
            "wh-1": {"platform": "bigquery", "convert_urns_to_lowercase": True}
        }
    )
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "proj.dataset.events" in urn and "Proj.Dataset.Events" not in urn


def test_resolver_rejects_malformed_full_table_id() -> None:
    # A full_table_id that does not resolve to database.schema.table (3 segments)
    # must not produce a phantom URN for a nonexistent dataset.
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-1++table++justatable"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="justatable", connection_type="bigquery"
            )
        }
    )
    resolver = MconResolver(make_config(), client, report)
    assert resolver.dataset_urn_for_mcon(mcon) is None
    assert report.mcons_resolution_failed == 1


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
    cfg = make_config()
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
    cfg = make_config()
    resolver = MconResolver(cfg, client, report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    _build_assertion_workunits(
        builder, MonteCarloAssertionDef(uuid="mon-1", entity_mcons=[mcon])
    )

    alert = MonteCarloAlert(
        uuid="alert-1",
        monitor_uuid="mon-1",
        severity="SEV-2",
        created_time="2026-05-01T00:00:00+00:00",
    )
    wus = list(builder.build_run_event(alert))
    assert len(wus) == 1
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.runId == "alert-1"
    assert run_event.result is not None and run_event.result.type == "FAILURE"
    assert report.run_events_emitted == 1


def test_build_run_event_skips_unknown_monitor() -> None:
    report = MonteCarloSourceReport()
    cfg = make_config()
    resolver = MconResolver(cfg, FakeResolverClient({}), report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    alert = MonteCarloAlert(
        uuid="alert-2", monitor_uuid="ghost", created_time="2026-05-01T00:00:00+00:00"
    )
    assert list(builder.build_run_event(alert)) == []
    assert report.run_events_emitted == 0


# --- Client parsing / pagination (against recorded GraphQL dicts) ---


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
    """Every auto-mapped connection type resolves to a real DataHub platform.

    Table-driven over CONNECTION_TYPE_TO_PLATFORM so a new entry that maps to
    a non-platform value (empty, whitespace, or mixed-case) is caught, and so a
    removed entry is noticed. Asserts the platform name appears in the resolved
    URN for each mapped connection type.
    """
    from datahub.ingestion.source.montecarlo.constants import (
        CONNECTION_TYPE_TO_PLATFORM,
    )

    assert CONNECTION_TYPE_TO_PLATFORM, "map should not be empty"
    for connection_type, platform in CONNECTION_TYPE_TO_PLATFORM.items():
        assert platform, f"{connection_type} maps to empty platform"
        assert platform == platform.lower(), f"{platform} must be lowercase"
        assert " " not in platform, f"{platform} must have no spaces"
        mcon = "MCON++acct++wh++table++db.sch.tbl"
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
            make_config(), client, MonteCarloSourceReport()
        ).dataset_urn_for_mcon(mcon)
        assert urn is not None, f"{connection_type} did not resolve"
        assert f":{platform}," in urn or urn.endswith(f":{platform}"), (
            f"{connection_type} -> URN {urn} missing platform {platform}"
        )


def test_resolver_new_connection_types_resolve() -> None:
    """clickhouse, dremio, db2, and starburst (-> trino) auto-map correctly."""
    cases = {
        "clickhouse": "clickhouse",
        "dremio": "dremio",
        "db2": "db2",
        "starburst_enterprise": "trino",
        "starburst_galaxy": "trino",
    }
    for connection_type, platform in cases.items():
        mcon = "MCON++acct++wh++table++db.sch.tbl"
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
            make_config(), client, MonteCarloSourceReport()
        ).dataset_urn_for_mcon(mcon)
        assert urn is not None, f"{connection_type} did not resolve"
        assert platform in urn, f"{connection_type} should resolve to {platform}"


def test_resolver_transactional_db_is_unmapped_not_guessed() -> None:
    """TRANSACTIONAL_DB (the real enum value, underscore) is a category spanning
    postgres/sql-server/synapse/sap-hana/azure-sql and must NOT auto-map to a
    single platform. It warns and skips so the user configures
    connection_to_platform_map for the specific warehouse.
    """
    from datahub.ingestion.source.montecarlo.constants import (
        CONNECTION_TYPE_TO_PLATFORM,
    )

    # The real enum value lowercased is transactional_db (underscore), which
    # must not be in the map. The old transactional-db (hyphen) key was dead
    # code that could never match and is now removed.
    assert "transactional_db" not in CONNECTION_TYPE_TO_PLATFORM
    assert "transactional-db" not in CONNECTION_TYPE_TO_PLATFORM

    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh++table++db.sch.tbl"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon,
                full_table_id="db.sch.tbl",
                connection_type="transactional_db",
            )
        }
    )
    resolver = MconResolver(make_config(), client, report)
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is None, "transactional_db must not auto-map to a guessed platform"
    assert report.mcons_unmapped_platform, "should record unmapped platform"
    assert report.mcons_unmapped_platform[-1] == mcon


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
    assert source.report.build_failures == 1


def test_partial_build_failures_trip_soft_delete_interlock() -> None:
    """When some monitors fail to build (transient errors), the source records
    a failure so stale_entity_removal_handler skips soft-deletion — preventing
    the scenario where 40/100 monitors hit a transient getTable error and the
    handler soft-deletes the 40 absent URNs even though those monitors still
    exist in Monte Carlo."""
    source = _bare_source()
    # Simulate two monitors: one builds fine, one raises a transient error.
    items = [MonteCarloAssertionDef(uuid="ok"), MonteCarloAssertionDef(uuid="bad")]

    def build(item: MonteCarloAssertionDef) -> Iterator[MetadataWorkUnit]:
        if item.uuid == "bad":
            raise RuntimeError("transient getTable error")
        yield cast(MetadataWorkUnit, "wu-ok")

    source.config = make_config()
    source.config.include_assertions = True
    source.config.include_alerts = False
    source.client = cast(MonteCarloClient, None)
    source.builder = cast(MonteCarloAssertionBuilder, None)
    source.resolver = cast(MconResolver, None)

    # Monkeypatch the client/builder to use our _emit directly.
    from unittest.mock import MagicMock

    source.client = MagicMock()
    source.client.get_monitors = lambda: iter(items)
    source.client.get_custom_rules = lambda: iter([])
    source.client.get_alerts = lambda: iter([])
    source.builder = MagicMock()
    source.builder.build_assertion = build
    source.builder.build_run_event = lambda alert: iter(())
    source.builder.iter_ingested_monitors = lambda: iter(())

    list(source.get_workunits_internal())

    # The partial-failure guard should have recorded a source-level failure.
    assert source.report.build_failures == 1
    assert len(source.report.failures) >= 1
    failure_titles = [f.title for f in source.report.failures]
    assert any("Partial build failures" in t for t in failure_titles)


def test_no_build_failures_does_not_trip_interlock() -> None:
    """When all monitors build successfully, no partial-failure is recorded
    and the soft-delete interlock is not tripped."""
    source = _bare_source()
    items = [MonteCarloAssertionDef(uuid="ok1"), MonteCarloAssertionDef(uuid="ok2")]

    def build(item: MonteCarloAssertionDef) -> Iterator[MetadataWorkUnit]:
        yield cast(MetadataWorkUnit, f"wu-{item.uuid}")

    from unittest.mock import MagicMock

    source.config = make_config()
    source.config.include_assertions = True
    source.config.include_alerts = False
    source.client = MagicMock()
    source.client.get_monitors = lambda: iter(items)
    source.client.get_custom_rules = lambda: iter([])
    source.client.get_alerts = lambda: iter([])
    source.builder = MagicMock()
    source.builder.build_assertion = build
    source.builder.build_run_event = lambda alert: iter(())
    source.builder.iter_ingested_monitors = lambda: iter(())

    list(source.get_workunits_internal())

    assert source.report.build_failures == 0
    assert len(source.report.failures) == 0


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
    assert report.warnings[0].title == "Monte Carlo API rate limited"
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


def test_paginate_stops_on_null_end_cursor() -> None:
    # has_next_page True but a null end_cursor must terminate, not loop forever
    # (a second page request would IndexError past the single recorded response).
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
    assert [r.uuid for r in client.get_custom_rules()] == ["r1"]


def test_get_table_missing_full_table_id_warns_and_returns_none() -> None:
    report = MonteCarloSourceReport()
    client = _client_with_responses(
        [{"get_table": {"mcon": "m", "warehouse": {"connection_type": "snowflake"}}}]
    )
    client.report = report
    assert client.get_table("MCON++a++b++table++c") is None
    assert len(report.warnings) == 1


# --- Run events (measured metrics) ---


def test_run_events_lookback_days_defaults_to_disabled() -> None:
    cfg = make_config()
    assert cfg.run_events_lookback_days is None
    assert cfg.run_events_first == 5


def test_run_events_lookback_days_requires_include_assertions() -> None:
    # Run events attach to assertions, same as alerts.
    with pytest.raises(ValueError):
        make_config(run_events_lookback_days=7, include_assertions=False)


def test_run_events_lookback_days_allows_without_alerts() -> None:
    # Independent of include_alerts — operator may want only the success signal.
    cfg = make_config(run_events_lookback_days=7, include_alerts=False)
    assert cfg.run_events_lookback_days == 7


def _ingest(
    builder: MonteCarloAssertionBuilder, definition: MonteCarloAssertionDef
) -> None:
    """Run build_assertion so _ingested_by_monitor is populated for run-event tests."""
    list(builder.build_assertion(definition))


def test_metric_names_for_monitor_table_keeps_only_total_row_count() -> None:
    # TABLE monitors declare four comparison metrics, but only total_row_count
    # returns points from getMetricsV4; the other three are non-standard names.
    builder = MonteCarloAssertionBuilder(
        make_config(),
        MonteCarloSourceReport(),
        None,  # type: ignore[arg-type]
    )
    definition = MonteCarloAssertionDef(
        uuid="mon-tbl",
        monitor_type="TABLE",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[
            MonteCarloComparison(metric="last_updated_on"),
            MonteCarloComparison(metric="schema"),
            MonteCarloComparison(metric="total_row_count"),
            MonteCarloComparison(metric="total_row_count_last_changed_on"),
        ],
    )
    assert builder.metric_names_for_monitor(definition) == ["total_row_count"]


def test_metric_names_for_monitor_skips_custom_value_based_metric() -> None:
    # CUSTOM_SQL monitors declare custom_value_based_metric_<uuid> which returns
    # zero points from getMetricsV4 (custom metrics use a separate surface).
    builder = MonteCarloAssertionBuilder(
        make_config(),
        MonteCarloSourceReport(),
        None,  # type: ignore[arg-type]
    )
    definition = MonteCarloAssertionDef(
        uuid="mon-sql",
        monitor_type="CUSTOM_SQL",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[MonteCarloComparison(metric="custom_value_based_metric_abc-123")],
    )
    assert builder.metric_names_for_monitor(definition) == []


def test_metric_names_for_monitor_stats_keeps_standard_names() -> None:
    builder = MonteCarloAssertionBuilder(
        make_config(),
        MonteCarloSourceReport(),
        None,  # type: ignore[arg-type]
    )
    definition = MonteCarloAssertionDef(
        uuid="mon-stats",
        monitor_type="STATS",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[
            MonteCarloComparison(metric="null_rate", field="email"),
        ],
    )
    assert builder.metric_names_for_monitor(definition) == ["null_rate"]


def test_metric_names_for_monitor_dedupes_repeated_metrics() -> None:
    builder = MonteCarloAssertionBuilder(
        make_config(),
        MonteCarloSourceReport(),
        None,  # type: ignore[arg-type]
    )
    definition = MonteCarloAssertionDef(
        uuid="mon-dup",
        monitor_type="METRIC",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[
            MonteCarloComparison(metric="row_count"),
            MonteCarloComparison(metric="row_count"),
        ],
    )
    assert builder.metric_names_for_monitor(definition) == ["row_count"]


def test_metric_names_for_monitor_no_comparisons_returns_empty() -> None:
    builder = MonteCarloAssertionBuilder(
        make_config(),
        MonteCarloSourceReport(),
        None,  # type: ignore[arg-type]
    )
    definition = MonteCarloAssertionDef(
        uuid="mon-empty", entity_mcons=["MCON++a++b++table++c"]
    )
    assert builder.metric_names_for_monitor(definition) == []


def test_field_for_metric_returns_field_from_comparison() -> None:
    definition = MonteCarloAssertionDef(
        uuid="mon-stats",
        monitor_type="STATS",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[
            MonteCarloComparison(metric="null_rate", field="email"),
        ],
    )
    assert (
        MonteCarloAssertionBuilder.field_for_metric(definition, "null_rate") == "email"
    )


def test_field_for_metric_returns_first_field_when_no_single_field() -> None:
    definition = MonteCarloAssertionDef(
        uuid="mon-multi",
        monitor_type="METRIC",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[
            MonteCarloComparison(metric="distinct_count", fields=["col_a", "col_b"]),
        ],
    )
    assert (
        MonteCarloAssertionBuilder.field_for_metric(definition, "distinct_count")
        == "col_a"
    )


def test_field_for_metric_returns_none_for_table_level_metric() -> None:
    definition = MonteCarloAssertionDef(
        uuid="mon-tbl",
        monitor_type="TABLE",
        entity_mcons=["MCON++a++b++table++c"],
        comparisons=[MonteCarloComparison(metric="total_row_count")],
    )
    assert (
        MonteCarloAssertionBuilder.field_for_metric(definition, "total_row_count")
        is None
    )


def test_build_run_events_from_execution_maps_row_count_to_typed_slot() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-vol", monitor_type="VOLUME", entity_mcons=[mcon]
        ),
    )
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-1",
        monitor_uuid="mon-vol",
        start_time="2026-05-01T00:00:00+00:00",
        status="SUCCESS",
    )
    points = [
        MonteCarloMetricPoint(metric="total_row_count", value=500.0),
    ]
    wus = list(builder.build_run_events_from_execution(execution, points))
    assert len(wus) == 1
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.result is not None
    assert run_event.result.type == "SUCCESS"
    assert run_event.result.rowCount == 500
    assert run_event.runId == "je-1"
    assert report.run_events_emitted == 1


def test_build_run_events_from_execution_maps_null_rate_to_actual_agg() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-stats", monitor_type="STATS", entity_mcons=[mcon]
        ),
    )
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-2",
        monitor_uuid="mon-stats",
        start_time="2026-05-01T00:00:00+00:00",
        status="SUCCESS",
    )
    points = [MonteCarloMetricPoint(metric="null_rate", value=0.05)]
    wus = list(builder.build_run_events_from_execution(execution, points))
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.result is not None
    assert run_event.result.actualAggValue == 0.05


def test_build_run_events_from_execution_byte_count_lands_in_native_results() -> None:
    # No typed byte slot on AssertionResult -> degrades to nativeResults string map.
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-vol", monitor_type="VOLUME", entity_mcons=[mcon]
        ),
    )
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-3",
        monitor_uuid="mon-vol",
        start_time="2026-05-01T00:00:00+00:00",
        status="SUCCESS",
    )
    points = [MonteCarloMetricPoint(metric="total_byte_count", value=1024.0)]
    wus = list(builder.build_run_events_from_execution(execution, points))
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.result is not None
    assert run_event.result.nativeResults == {"total_byte_count": "1024.0"}


def test_build_run_events_from_execution_includes_thresholds_and_execution_meta() -> (
    None
):
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-vol", monitor_type="VOLUME", entity_mcons=[mcon]
        ),
    )
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-4",
        monitor_uuid="mon-vol",
        start_time="2026-05-01T00:00:00+00:00",
        status="SUCCESS",
        exceptions="some warning",
        total_result_count=42,
        evaluated_record_count=1000,
    )
    points = [
        MonteCarloMetricPoint(
            metric="total_row_count", value=500.0, upper_threshold=1000.0
        ),
    ]
    wus = list(builder.build_run_events_from_execution(execution, points))
    run_event = _aspect(wus[0])
    assert isinstance(run_event, AssertionRunEventClass)
    assert run_event.result is not None
    assert run_event.result.rowCount == 500
    assert run_event.result.nativeResults is not None
    assert run_event.result.nativeResults["total_row_count_threshold_upper"] == "1000.0"
    assert run_event.result.nativeResults["exceptions"] == "some warning"
    assert run_event.result.nativeResults["totalResultCount"] == "42"
    assert run_event.result.nativeResults["evaluatedRecordCount"] == "1000"


def test_build_run_events_from_execution_skips_unknown_monitor() -> None:
    report = MonteCarloSourceReport()
    cfg = make_config()
    resolver = MconResolver(cfg, FakeResolverClient({}), report)
    builder = MonteCarloAssertionBuilder(cfg, report, resolver)
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-ghost",
        monitor_uuid="ghost",
        start_time="2026-05-01T00:00:00+00:00",
        status="SUCCESS",
    )
    assert list(builder.build_run_events_from_execution(execution, [])) == []
    assert report.run_events_emitted == 0


def test_build_run_events_from_execution_skips_missing_timestamp() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-vol", monitor_type="VOLUME", entity_mcons=[mcon]
        ),
    )
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-nots",
        monitor_uuid="mon-vol",
        status="SUCCESS",
    )
    assert list(builder.build_run_events_from_execution(execution, [])) == []
    assert len(report.warnings) == 1
    assert report.run_events_emitted == 0


def test_build_run_events_from_execution_is_not_primary_source() -> None:
    # is_primary_source=False routes the URN to urns_to_skip so stale entity
    # removal never touches run events — matches the alert path.
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-vol", monitor_type="VOLUME", entity_mcons=[mcon]
        ),
    )
    execution = MonteCarloJobExecution(
        job_execution_uuid="je-1",
        monitor_uuid="mon-vol",
        start_time="2026-05-01T00:00:00+00:00",
        status="SUCCESS",
    )
    wus = list(builder.build_run_events_from_execution(execution, []))
    assert wus and wus[0].is_primary_source is False


def test_iter_ingested_monitors_yields_pairs() -> None:
    report = MonteCarloSourceReport()
    mcon = "MCON++acct++wh-2++table++db.sch.tbl"
    builder = _builder_with_resolved_snowflake(report, mcon)
    _ingest(
        builder,
        MonteCarloAssertionDef(
            uuid="mon-a", monitor_type="VOLUME", entity_mcons=[mcon]
        ),
    )
    pairs = list(builder.iter_ingested_monitors())
    assert len(pairs) == 1
    monitor_uuid, ingested = pairs[0]
    assert monitor_uuid == "mon-a"
    assert ingested.mcon == mcon
    assert ingested.definition.uuid == "mon-a"
