"""Unit tests for Fabric OneLake cross-item / cross-workspace SQL name resolution."""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.fabric.onelake.config import FabricUsageConfig
from datahub.ingestion.source.fabric.onelake.name_resolver import (
    FabricItemCatalog,
    FabricOneLakeSchemaResolver,
    FabricSqlParsingAggregator,
    drop_unresolved_references,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import (
    DatasetUsageStatisticsClass,
    OperationClass,
    QuerySubjectsClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
    ViewDefinition,
)

PLATFORM = "fabric-onelake"
QUERY_TS = datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone.utc)

WS_ANALYTICS = "ws-analytics"
WS_SHARED = "ws-shared"
GOLD_DB = f"{WS_ANALYTICS}.wh-gold"


def _urn(name: str, platform_instance: Optional[str] = None) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name=name, platform_instance=platform_instance, env="PROD"
    )


def _catalog() -> FabricItemCatalog:
    catalog = FabricItemCatalog()
    catalog.add_workspace(WS_ANALYTICS, "Analytics")
    catalog.add_item(WS_ANALYTICS, "lh-bronze", "bronze_lh", "Lakehouse")
    catalog.add_item(WS_ANALYTICS, "lh-silver", "silver_lh", "Lakehouse")
    catalog.add_item(WS_ANALYTICS, "wh-gold", "gold_wh", "Warehouse")
    catalog.add_workspace(WS_SHARED, "Shared Data")
    # Same display name as the analytics lakehouse, different workspace.
    catalog.add_item(WS_SHARED, "lh-shared-silver", "silver_lh", "Lakehouse")
    catalog.add_item(WS_SHARED, "lh-ref", "ref_lh", "Lakehouse")
    return catalog


def _make_aggregator(
    platform_instance: Optional[str] = None,
    generate_usage: bool = False,
    generate_queries: bool = False,
) -> Tuple[
    FabricSqlParsingAggregator, FabricOneLakeSchemaResolver, FabricOneLakeSourceReport
]:
    """Wire the resolver + aggregator the same way FabricOneLakeSource does."""
    report = FabricOneLakeSourceReport()
    resolver = FabricOneLakeSchemaResolver(
        catalog=_catalog(),
        report=report,
        platform=PLATFORM,
        platform_instance=platform_instance,
        env="PROD",
    )
    aggregator = FabricSqlParsingAggregator(
        schema_resolver=resolver,
        platform=PLATFORM,
        platform_instance=platform_instance,
        env="PROD",
        generate_lineage=True,
        generate_queries=generate_queries,
        generate_query_subject_fields=generate_queries,
        generate_usage_statistics=generate_usage,
        generate_operations=generate_usage,
        usage_config=FabricUsageConfig() if generate_usage else None,
        is_allowed_table=lambda name: (
            not (resolver.is_unresolved_name(name) or resolver.is_system_name(name))
        ),
    )
    return aggregator, resolver, report


def _drain(
    aggregator: FabricSqlParsingAggregator, resolver: FabricOneLakeSchemaResolver
) -> Dict[str, List[object]]:
    """Drain the aggregator like the source does; return aspects by entity URN."""
    out: Dict[str, List[object]] = {}
    for mcp in aggregator.gen_metadata():
        filtered = drop_unresolved_references(mcp, resolver.unresolved_urns).mcp
        if filtered is not None:
            filtered = drop_unresolved_references(filtered, resolver.system_urns).mcp
        if filtered is None:
            continue
        assert filtered.entityUrn is not None
        out.setdefault(filtered.entityUrn, []).append(filtered.aspect)
    return out


def _upstreams_of_view(
    sql: str, default_db: str = GOLD_DB
) -> Tuple[
    Optional[UpstreamLineageClass],
    FabricOneLakeSchemaResolver,
    FabricOneLakeSourceReport,
]:
    aggregator, resolver, report = _make_aggregator()
    view_urn = _urn(f"{default_db}.dbo.v_test")
    aggregator.add_view_definition(
        view_urn=view_urn,
        view_definition=sql,
        default_db=default_db,
        default_schema="dbo",
    )
    aspects = _drain(aggregator, resolver).get(view_urn, [])
    lineage = [a for a in aspects if isinstance(a, UpstreamLineageClass)]
    aggregator.close()
    return (lineage[0] if lineage else None), resolver, report


def _upstream_datasets(lineage: Optional[UpstreamLineageClass]) -> List[str]:
    assert lineage is not None
    return sorted(u.dataset for u in lineage.upstreams)


def test_catalog_resolves_names_case_insensitively_and_strips_brackets() -> None:
    catalog = _catalog()
    for token in ["silver_lh", "SILVER_LH", "[Silver_LH]", '"silver_lh"']:
        resolution = catalog.resolve_item(WS_ANALYTICS, token)
        assert resolution.item is not None
        assert resolution.item.default_db == f"{WS_ANALYTICS}.lh-silver"

    # Items are also addressable by GUID.
    assert catalog.resolve_item(WS_SHARED, "LH-REF").item is not None
    assert catalog.resolve_workspace("[Shared Data]") == (WS_SHARED, None)
    assert catalog.resolve_workspace(WS_SHARED) == (WS_SHARED, None)
    assert catalog.resolve_workspace("nope")[0] is None


def test_catalog_reports_ambiguous_names() -> None:
    catalog = _catalog()
    catalog.add_item(WS_ANALYTICS, "wh-silver", "silver_lh", "Warehouse")
    resolution = catalog.resolve_item(WS_ANALYTICS, "silver_lh")
    assert resolution.item is None
    assert resolution.reason is not None and "ambiguous" in resolution.reason

    catalog.add_workspace("ws-other", "Shared Data")
    workspace_id, reason = catalog.resolve_workspace("Shared Data")
    assert workspace_id is None
    assert reason is not None and "ambiguous" in reason


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT id, amount FROM dbo.orders", id="two-part"),
        pytest.param("SELECT id, amount FROM orders", id="unqualified"),
        pytest.param("SELECT id, amount FROM gold_wh.dbo.orders", id="own-item-3-part"),
    ],
)
def test_same_item_references(sql: str) -> None:
    lineage, _, report = _upstreams_of_view(sql)
    assert _upstream_datasets(lineage) == [_urn(f"{GOLD_DB}.dbo.orders")]
    assert report.num_cross_item_references_unresolved == 0


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(
            "CREATE VIEW dbo.v_test AS SELECT c.id, c.name FROM silver_lh.dbo.customers AS c",
            id="three-part",
        ),
        pytest.param(
            "SELECT c.id, c.name FROM [Silver_LH].[dbo].[customers] AS c",
            id="bracketed-mixed-case",
        ),
        pytest.param(
            "SELECT c.id, c.name FROM [lh-silver].dbo.customers AS c",
            id="item-guid",
        ),
    ],
)
def test_cross_item_three_part_reference(sql: str) -> None:
    lineage, _, report = _upstreams_of_view(sql)
    upstream = _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers")
    assert _upstream_datasets(lineage) == [upstream]
    assert report.num_cross_item_references_resolved == 1

    # Column-level lineage lands on the resolved URN too.
    assert lineage is not None and lineage.fineGrainedLineages
    upstream_fields = {
        SchemaFieldUrn.from_string(f).parent
        for fgl in lineage.fineGrainedLineages
        for f in fgl.upstreams or []
    }
    assert upstream_fields == {upstream}


def test_three_part_reference_is_scoped_to_the_referencing_workspace() -> None:
    sql = "SELECT id FROM silver_lh.dbo.customers"
    analytics, _, _ = _upstreams_of_view(sql, default_db=GOLD_DB)
    shared, _, _ = _upstreams_of_view(sql, default_db=f"{WS_SHARED}.lh-ref")

    assert _upstream_datasets(analytics) == [
        _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers")
    ]
    assert _upstream_datasets(shared) == [
        _urn(f"{WS_SHARED}.lh-shared-silver.dbo.customers")
    ]


@pytest.mark.parametrize(
    "table_ref",
    [
        pytest.param("[Shared Data].[ref_lh].[dbo].[regions]", id="display-names"),
        pytest.param("[ws-shared].[lh-ref].dbo.regions", id="guids"),
    ],
)
def test_cross_workspace_four_part_reference(table_ref: str) -> None:
    lineage, _, report = _upstreams_of_view(
        f"SELECT c.id, r.region_name FROM silver_lh.dbo.customers AS c "
        f"JOIN {table_ref} AS r ON c.region_id = r.region_id"
    )
    assert _upstream_datasets(lineage) == [
        _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers"),
        _urn(f"{WS_SHARED}.lh-ref.dbo.regions"),
    ]
    assert report.num_cross_item_references_unresolved == 0


def test_unknown_item_is_dropped_and_warned() -> None:
    lineage, resolver, report = _upstreams_of_view(
        "SELECT c.id, x.score FROM silver_lh.dbo.customers AS c "
        "JOIN missing_lh.dbo.scores AS x ON c.id = x.id"
    )
    # Known upstream kept, unknown one dropped from table and column lineage.
    assert _upstream_datasets(lineage) == [
        _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers")
    ]
    assert lineage is not None
    for fgl in lineage.fineGrainedLineages or []:
        assert all("missing_lh" not in f for f in fgl.upstreams or [])
    assert resolver.unresolved_urns == {_urn("missing_lh.dbo.scores")}
    assert report.num_cross_item_references_unresolved == 1
    warnings = [
        w for w in report.warnings if w.title == "Unresolved Cross-Item SQL Reference"
    ]
    assert len(warnings) == 1
    assert any("missing_lh.dbo.scores" in c for c in warnings[0].context)


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT id FROM missing_lh.dbo.scores", id="unknown-item"),
        pytest.param(
            "SELECT id FROM [Other WS].ref_lh.dbo.regions", id="unknown-workspace"
        ),
        pytest.param(
            "SELECT id FROM Analytics.missing_lh.dbo.scores",
            id="unknown-item-in-known-workspace",
        ),
    ],
)
def test_only_unknown_references_emit_no_lineage(sql: str) -> None:
    lineage, _, report = _upstreams_of_view(sql)
    assert lineage is None
    assert report.num_cross_item_references_unresolved == 1


def test_unresolved_name_matching_ignores_platform_instance() -> None:
    aggregator, resolver, _ = _make_aggregator(platform_instance="tenant1")
    urn = resolver.get_urn_for_table(
        _TableName(database="missing_lh", db_schema="dbo", table="scores")
    )
    assert urn == _urn("missing_lh.dbo.scores", platform_instance="tenant1")
    assert resolver.is_unresolved_name("missing_lh.dbo.scores")
    assert not aggregator.is_allowed_table(urn)
    aggregator.close()


def test_observed_queries_resolve_cross_item_with_column_lineage() -> None:
    aggregator, resolver, report = _make_aggregator(generate_usage=True)
    queries = [
        "INSERT INTO dbo.customer_totals (customer_id, total_amount) "
        "SELECT o.customer_id, SUM(o.amount) FROM bronze_lh.dbo.raw_orders AS o "
        "GROUP BY o.customer_id",
        "CREATE TABLE dbo.customer_snapshot AS "
        "SELECT customer_id, name FROM [Silver_LH].dbo.customers",
        "SELECT * FROM missing_lh.dbo.scores",
    ]
    for query in queries:
        aggregator.add_observed_query(
            ObservedQuery(
                query=query,
                timestamp=QUERY_TS,
                default_db=GOLD_DB,
                default_schema="dbo",
            )
        )
    aspects = _drain(aggregator, resolver)
    aggregator.close()

    expected = {
        f"{GOLD_DB}.dbo.customer_totals": (
            f"{WS_ANALYTICS}.lh-bronze.dbo.raw_orders",
            {("customer_id", "customer_id"), ("amount", "total_amount")},
        ),
        f"{GOLD_DB}.dbo.customer_snapshot": (
            f"{WS_ANALYTICS}.lh-silver.dbo.customers",
            {("customer_id", "customer_id"), ("name", "name")},
        ),
    }
    for downstream, (upstream, column_pairs) in expected.items():
        lineage = [
            a for a in aspects[_urn(downstream)] if isinstance(a, UpstreamLineageClass)
        ]
        assert len(lineage) == 1
        assert _upstream_datasets(lineage[0]) == [_urn(upstream)]
        pairs = {
            (
                SchemaFieldUrn.from_string(up).field_path,
                SchemaFieldUrn.from_string(down).field_path,
            )
            for fgl in lineage[0].fineGrainedLineages or []
            for up in fgl.upstreams or []
            for down in fgl.downstreams or []
        }
        assert pairs == column_pairs
        assert any(isinstance(a, OperationClass) for a in aspects[_urn(downstream)])

    # Usage lands on the resolved upstreams; nothing for the unknown item.
    for upstream in (
        f"{WS_ANALYTICS}.lh-bronze.dbo.raw_orders",
        f"{WS_ANALYTICS}.lh-silver.dbo.customers",
    ):
        assert any(
            isinstance(a, DatasetUsageStatisticsClass) for a in aspects[_urn(upstream)]
        )
    assert not any("missing_lh" in urn for urn in aspects)
    assert report.num_cross_item_references_unresolved == 1


def test_more_than_four_part_names_are_unresolved() -> None:
    """A 5-part (linked-server style) name can never match a GUID-keyed
    dataset, so it is dropped and reported instead of emitted as a dangling URN."""
    lineage, resolver, report = _upstreams_of_view(
        "SELECT c.id, r.region_name FROM silver_lh.dbo.customers AS c "
        "JOIN srv.[Shared Data].ref_lh.dbo.regions AS r ON c.id = r.id"
    )
    assert _upstream_datasets(lineage) == [
        _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers")
    ]
    assert report.num_cross_item_references_unresolved == 1
    assert any("srv" in urn for urn in resolver.unresolved_urns)


def test_quoting_and_case_variants_count_as_one_reference() -> None:
    lineage, _, report = _upstreams_of_view(
        "SELECT a.id FROM silver_lh.dbo.customers AS a "
        "JOIN [Silver_LH].[dbo].[customers] AS b ON a.id = b.id"
    )
    assert _upstream_datasets(lineage) == [
        _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers")
    ]
    assert report.num_cross_item_references_resolved == 1


def test_unresolved_downstream_gets_no_lineage_or_operation() -> None:
    """INSERT into an unknown item: nothing is emitted *for* the phantom
    dataset, and it is stripped from the query's subjects."""
    aggregator, resolver, report = _make_aggregator(
        generate_usage=True, generate_queries=True
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="INSERT INTO missing_lh.dbo.scores (id) "
            "SELECT customer_id FROM dbo.customer_totals",
            timestamp=QUERY_TS,
            default_db=GOLD_DB,
            default_schema="dbo",
        )
    )
    aspects = _drain(aggregator, resolver)
    aggregator.close()

    phantom = _urn("missing_lh.dbo.scores")
    assert phantom in resolver.unresolved_urns
    assert phantom not in aspects
    subjects = [
        subject.entity
        for per_entity in aspects.values()
        for aspect in per_entity
        if isinstance(aspect, QuerySubjectsClass)
        for subject in aspect.subjects
    ]
    assert subjects, "the query itself is still emitted"
    assert not any("missing_lh" in subject for subject in subjects)
    assert report.num_cross_item_references_unresolved == 1


def test_drop_counts_stripped_upstreams() -> None:
    aggregator, resolver, _ = _make_aggregator()
    view_urn = _urn(f"{GOLD_DB}.dbo.v_test")
    aggregator.add_view_definition(
        view_urn=view_urn,
        view_definition="SELECT c.id, x.score FROM silver_lh.dbo.customers AS c "
        "JOIN missing_lh.dbo.scores AS x ON c.id = x.id",
        default_db=GOLD_DB,
        default_schema="dbo",
    )
    results = [
        drop_unresolved_references(mcp, resolver.unresolved_urns)
        for mcp in aggregator.gen_metadata()
    ]
    aggregator.close()

    lineage_results = [
        r
        for r in results
        if r.mcp is not None and isinstance(r.mcp.aspect, UpstreamLineageClass)
    ]
    assert len(lineage_results) == 1
    assert lineage_results[0].num_upstreams_dropped == 1


def test_view_parsing_hook_is_scoped() -> None:
    """Pins the private SqlParsingAggregator hook FabricSqlParsingAggregator
    overrides: gen_metadata must parse each view through
    ``_process_view_definition``, with the resolver scoped to that view's
    workspace. If the aggregator renames or bypasses the hook, this fails
    instead of every 3-part view reference silently becoming unresolved."""
    assert hasattr(SqlParsingAggregator, "_process_view_definition")

    aggregator, resolver, _ = _make_aggregator()
    scopes: List[Optional[str]] = []
    original = SqlParsingAggregator._process_view_definition

    def spy(
        self: SqlParsingAggregator, view_urn: str, view_definition: ViewDefinition
    ) -> None:
        scopes.append(resolver._scope_workspace_id)
        original(self, view_urn, view_definition)

    for default_db in (GOLD_DB, f"{WS_SHARED}.lh-ref"):
        aggregator.add_view_definition(
            view_urn=_urn(f"{default_db}.dbo.v_test"),
            view_definition="SELECT id FROM silver_lh.dbo.customers",
            default_db=default_db,
            default_schema="dbo",
        )
    with patch.object(SqlParsingAggregator, "_process_view_definition", spy):
        list(aggregator.gen_metadata())
    aggregator.close()

    assert sorted(s for s in scopes if s is not None) == [WS_ANALYTICS, WS_SHARED]
    assert resolver._scope_workspace_id is None


@pytest.mark.parametrize(
    "table_ref",
    [
        "sys.databases",
        "[sys].[spt_datatype_info_view]",
        "INFORMATION_SCHEMA.COLUMNS",
        "information_schema.tables",
        "queryinsights.exec_requests_history",
        # Qualified with another item / workspace: still a system object.
        "silver_lh.sys.objects",
        "[Shared Data].[ref_lh].[sys].[tables]",
    ],
)
def test_system_objects_emit_no_datasets_usage_or_queries(table_ref: str) -> None:
    """Catalog queries (e.g. the ODBC driver's `sp_*` procedure bodies, or the
    connector's own INFORMATION_SCHEMA reads) must not create datasets."""
    aggregator, resolver, report = _make_aggregator(
        generate_usage=True, generate_queries=True
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query=f"SELECT * FROM {table_ref}",
            timestamp=QUERY_TS,
            default_db=GOLD_DB,
            default_schema="dbo",
        )
    )
    aspects = _drain(aggregator, resolver)
    aggregator.close()

    assert resolver.system_urns
    assert not any(urn.startswith("urn:li:dataset:") for urn in aspects), aspects
    assert not any(
        isinstance(aspect, QuerySubjectsClass)
        for per_entity in aspects.values()
        for aspect in per_entity
    )
    assert report.num_system_object_references_filtered == 1
    # System objects are never treated as unresolved cross-item references.
    assert report.num_cross_item_references_unresolved == 0
    assert not report.warnings


def test_system_object_upstream_is_stripped_from_lineage_and_subjects() -> None:
    """A query that mixes user tables and system objects keeps the user-table
    lineage / usage and loses only the system-object edges."""
    aggregator, resolver, report = _make_aggregator(
        generate_usage=True, generate_queries=True
    )
    aggregator.add_observed_query(
        ObservedQuery(
            query="INSERT INTO dbo.customer_totals (customer_id, db_name) "
            "SELECT c.customer_id, d.name FROM silver_lh.dbo.customers AS c "
            "CROSS JOIN sys.databases AS d",
            timestamp=QUERY_TS,
            default_db=GOLD_DB,
            default_schema="dbo",
        )
    )
    raw = list(aggregator.gen_metadata())
    aggregator.close()

    system_urn = _urn(f"{GOLD_DB}.sys.databases")
    assert resolver.system_urns == {system_urn}
    kept = []
    dropped_upstreams = 0
    for mcp in raw:
        result = drop_unresolved_references(mcp, resolver.system_urns)
        dropped_upstreams += result.num_upstreams_dropped
        if result.mcp is not None:
            kept.append(result.mcp)
    assert dropped_upstreams == 1
    assert system_urn not in {mcp.entityUrn for mcp in kept}

    lineage = [
        mcp.aspect
        for mcp in kept
        if isinstance(mcp.aspect, UpstreamLineageClass)
        and mcp.entityUrn == _urn(f"{GOLD_DB}.dbo.customer_totals")
    ]
    assert len(lineage) == 1
    assert _upstream_datasets(lineage[0]) == [
        _urn(f"{WS_ANALYTICS}.lh-silver.dbo.customers")
    ]
    for fgl in lineage[0].fineGrainedLineages or []:
        assert not any("sys.databases" in up for up in fgl.upstreams or [])
    for mcp in kept:
        if isinstance(mcp.aspect, QuerySubjectsClass):
            assert not any(
                "sys.databases" in subject.entity for subject in mcp.aspect.subjects
            )
    assert report.num_system_object_references_filtered == 1
    assert list(report.filtered_system_objects) == [f"{GOLD_DB}.sys.databases"]


def test_non_system_schema_named_like_a_prefix_is_kept() -> None:
    """Only the exact system schema names are filtered (e.g. `system` or
    `sys_archive` are ordinary user schemas)."""
    lineage, resolver, report = _upstreams_of_view(
        "SELECT a.id FROM sys_archive.events AS a JOIN system.logs AS b ON a.id = b.id"
    )
    assert _upstream_datasets(lineage) == [
        _urn(f"{GOLD_DB}.sys_archive.events"),
        _urn(f"{GOLD_DB}.system.logs"),
    ]
    assert not resolver.system_urns
    assert report.num_system_object_references_filtered == 0
