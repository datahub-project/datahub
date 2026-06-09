from typing import Any, Dict, List, Optional

import pytest

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
    MonteCarloClient,
    ResolvedTable,
)
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.mcon_resolver import (
    MconResolver,
    parse_mcon,
)
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionRunEventClass,
    AssertionTypeClass,
    DataPlatformInstanceClass,
)


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
    assert cfg.emit_alerts is True


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


def test_resolver_lowercases_urn_when_configured() -> None:
    mcon = "MCON++acct++wh-1++table++DB.SCH.TBL"
    client = FakeResolverClient(
        {
            mcon: ResolvedTable(
                mcon=mcon, full_table_id="DB.SCH.TBL", connection_type="snowflake"
            )
        }
    )
    cfg = make_config(convert_urns_to_lowercase=True)
    resolver = MconResolver(cfg, client, MonteCarloSourceReport())
    urn = resolver.dataset_urn_for_mcon(mcon)
    assert urn is not None
    assert "db.sch.tbl" in urn and "DB.SCH.TBL" not in urn


def test_get_monitors_paginates_with_offset() -> None:
    # getMonitors returns a plain list, so the client must walk it with
    # limit/offset; verify monitors past the first page are still fetched.
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = make_config()
    client.page_size = 2
    pages: Dict[int, List[Dict[str, Any]]] = {
        0: [{"uuid": "m1"}, {"uuid": "m2"}],
        2: [{"uuid": "m3"}],
    }
    seen_offsets: List[int] = []

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        offset = variables["offset"]
        seen_offsets.append(offset)
        return {"getMonitors": pages.get(offset, [])}

    client._call = fake_call  # type: ignore[method-assign]
    uuids = [m.uuid for m in client.get_monitors()]
    assert uuids == ["m1", "m2", "m3"]
    # Stops after the short second page rather than requesting a third.
    assert seen_offsets == [0, 2]


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
                "getMonitors": [
                    {
                        "uuid": "m1",
                        "name": "n1",
                        "monitorType": "FRESHNESS",
                        "entityMcons": ["MCON++a++b++table++c"],
                        "resourceId": "wh-1",
                    }
                ]
            }
        ]
    )
    monitors = list(client.get_monitors())
    assert len(monitors) == 1
    assert monitors[0].uuid == "m1"
    assert monitors[0].native_type == "FRESHNESS"
    assert not monitors[0].is_custom_rule


def test_client_get_monitors_passes_filter_variables() -> None:
    """monitor_types_allow and domain_ids are forwarded as GraphQL variables."""
    captured: Dict[str, Any] = {}

    def fake_call(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        captured.update(variables)
        return {"getMonitors": []}

    cfg = make_config(monitor_types_allow=["FRESHNESS"], domain_ids=["dom-1"])
    client = MonteCarloClient.__new__(MonteCarloClient)
    client.config = cfg
    client.page_size = 100
    client._call = fake_call  # type: ignore[method-assign]

    list(client.get_monitors())
    assert captured.get("monitorTypes") == ["FRESHNESS"]
    assert captured.get("domainIds") == ["dom-1"]


def test_client_get_custom_rules_paginates() -> None:
    page1 = {
        "getCustomRules": {
            "edges": [
                {
                    "node": {
                        "uuid": "r1",
                        "ruleType": "CUSTOM_SQL",
                        "customSql": "select 1",
                    }
                }
            ],
            "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
        }
    }
    page2 = {
        "getCustomRules": {
            "edges": [{"node": {"uuid": "r2", "ruleType": "CUSTOM_SQL"}}],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }
    }
    client = _client_with_responses([page1, page2])
    rules = list(client.get_custom_rules())
    assert [r.uuid for r in rules] == ["r1", "r2"]
    assert rules[0].is_custom_rule
    assert rules[0].custom_sql == "select 1"


def test_client_get_table_parses_connection_type() -> None:
    client = _client_with_responses(
        [
            {
                "getTable": {
                    "mcon": "MCON++a++b++table++db.sch.tbl",
                    "fullTableId": "db.sch.tbl",
                    "warehouse": {"connectionType": "snowflake"},
                }
            }
        ]
    )
    table = client.get_table("MCON++a++b++table++db.sch.tbl")
    assert table is not None
    assert table.full_table_id == "db.sch.tbl"
    assert table.connection_type == "snowflake"


def test_resolver_non_obvious_connection_types() -> None:
    """sql-server and synapse both map to mssql (non-obvious aliases)."""
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
            make_config(), client, MonteCarloSourceReport()
        ).dataset_urn_for_mcon(mcon)
        assert urn is not None
        assert "mssql" in urn, f"{connection_type} should resolve to mssql"
