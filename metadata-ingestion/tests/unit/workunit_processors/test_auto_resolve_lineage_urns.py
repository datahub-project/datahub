import logging
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest import mock

import pydantic
import pytest

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline_config import (
    AutoResolveLineageUrnsConfig,
    UpstreamPlatformCasing,
)
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns import (
    AutoResolveLineageUrnsProcessor,
)
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataJobInputOutputClass,
    DatasetSnapshotClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    LineageMatchTypeClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver

# Snowflake convention: uppercase. BI-tool convention: lowercase.
UPPER = make_dataset_urn("snowflake", "DB.SCHEMA.TABLE")
LOWER = make_dataset_urn("snowflake", "db.schema.table")
MIXED = make_dataset_urn("snowflake", "Db.Schema.Table")
DOWNSTREAM = make_dataset_urn("looker", "explore.orders")

# Mixed-case identifier variants that all share the lowercase form "db.schema.datahub".
WH_MIXED = make_dataset_urn("snowflake", "db.schema.DataHub")
WH_LOWER = make_dataset_urn("snowflake", "db.schema.datahub")
WH_UPPER = make_dataset_urn("snowflake", "db.schema.DATAHUB")

# Deferred-imported inside the processor, so patch it at its source module.
_PATCH_TARGET = "datahub.sql_parsing.schema_resolver_provider.provide_schema_resolver"


def _resolver(schemas: Dict[str, Dict[str, str]]) -> SchemaResolver:
    """A graph-less resolver pre-populated with {urn: {column: type}}."""
    resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    for urn, schema in schemas.items():
        resolver.add_raw_schema_info(urn, schema)
    return resolver


def _make_processor(
    schemas: Dict[str, Dict[str, str]],
) -> Tuple[AutoResolveLineageUrnsProcessor, mock.MagicMock, Any]:
    """Patch provide_schema_resolver to a single seeded snowflake resolver.

    `schemas` maps existing URN -> column schema. The processor builds its
    case-insensitive index from the resolver's schema-bearing entities (`get_urns()`),
    so the keys of `schemas` are exactly the URNs membership/casing resolution sees.
    """
    resolver = _resolver(schemas)
    provide_mock = mock.MagicMock(return_value=resolver)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    # The processor imports provide_schema_resolver once in __init__ (a single sqlglot
    # chokepoint) and caches it, so the patch must be active *before* construction.
    patcher = mock.patch(_PATCH_TARGET, provide_mock)
    patcher.start()
    processor = AutoResolveLineageUrnsProcessor.create(ctx)
    return processor, provide_mock, patcher


def _upstream_wu(
    upstream_urn: str,
    fine_grained_field: Optional[str] = None,
) -> MetadataWorkUnit:
    fgl = None
    if fine_grained_field is not None:
        fgl = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[make_schema_field_urn(upstream_urn, fine_grained_field)],
                downstreams=[make_schema_field_urn(DOWNSTREAM, "amount")],
            )
        ]
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=upstream_urn, type="TRANSFORMED")],
        fineGrainedLineages=fgl,
    )
    return MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()


def _run(
    schemas: Dict[str, Dict[str, str]],
    wu: MetadataWorkUnit,
) -> MetadataWorkUnit:
    processor, _provide, patcher = _make_processor(schemas)
    try:
        [out] = list(processor.process(iter([wu])))
        return out
    finally:
        patcher.stop()


def _upstream_aspect(wu: MetadataWorkUnit) -> UpstreamLineageClass:
    aspect = wu.get_aspect_of_type(UpstreamLineageClass)
    assert aspect is not None
    return aspect


def _dashboard_aspect(wu: MetadataWorkUnit) -> DashboardInfoClass:
    aspect = wu.get_aspect_of_type(DashboardInfoClass)
    assert aspect is not None
    return aspect


def _fine_grained(wu: MetadataWorkUnit) -> FineGrainedLineageClass:
    fine_grained = _upstream_aspect(wu).fineGrainedLineages
    assert fine_grained is not None
    return fine_grained[0]


def _stored_upstream(wu: MetadataWorkUnit) -> str:
    return _upstream_aspect(wu).upstreams[0].dataset


# --- table-level dataset URN casing -----------------------------------------------


@pytest.mark.parametrize(
    "stored,emitted",
    [
        (LOWER, UPPER),  # BI emits uppercase, warehouse stores lowercase
        (UPPER, LOWER),  # doc headline: BI emits lowercase, warehouse stores uppercase
        (WH_MIXED, WH_LOWER),  # mixed stored, lowercase emitted
        (WH_LOWER, WH_MIXED),  # lowercase stored, mixed emitted
        (WH_MIXED, WH_UPPER),  # mixed stored, uppercase emitted
    ],
)
def test_heals_to_stored_casing(stored: str, emitted: str) -> None:
    # An upstream reference is reconciled to whatever casing the warehouse already
    # stores, in any direction (upper/lower/mixed) — the property is symmetric.
    out = _run({stored: {"amount": "int"}}, _upstream_wu(emitted))
    assert _stored_upstream(out) == stored


def test_keeps_exact_when_exact_entity_exists():
    out = _run(
        {UPPER: {"amount": "int"}, LOWER: {"amount": "int"}}, _upstream_wu(UPPER)
    )
    assert _stored_upstream(out) == UPPER


def test_ambiguous_collision_left_unchanged():
    out = _run(
        {UPPER: {"amount": "int"}, LOWER: {"amount": "int"}}, _upstream_wu(MIXED)
    )
    assert _stored_upstream(out) == MIXED


def test_leaves_unchanged_when_no_entity_matches():
    out = _run({}, _upstream_wu(UPPER))
    assert _stored_upstream(out) == UPPER


def test_unconfigured_platform_left_unchanged():
    # Upstream is bigquery, but only snowflake is configured -> out of scope: the URN
    # is untouched and no matchType verdict is stamped (absence == not processed).
    bq = make_dataset_urn("bigquery", "PROJ.DS.T")
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(bq))
    assert _stored_upstream(out) == bq
    assert _upstream_aspect(out).upstreams[0].matchType is None


# --- mixed-casing identifiers (e.g. `DataHub` vs `datahub`) ------------------------


def test_exact_mixedcase_wins_and_does_not_misroute():
    # Both `DataHub` and `datahub` genuinely exist (case-sensitive platform). BI emits
    # `datahub`, which matches one exactly -> keep it, never re-route to `DataHub`.
    out = _run(
        {WH_MIXED: {"amount": "int"}, WH_LOWER: {"amount": "int"}},
        _upstream_wu(WH_LOWER),
    )
    assert _stored_upstream(out) == WH_LOWER
    upstream = _upstream_aspect(out).upstreams[0]
    assert upstream.matchType == LineageMatchTypeClass.EXACT


def test_mixedcase_ambiguous_third_casing_left_unchanged():
    # Both `DataHub` and `datahub` exist; BI emits a third casing `DATAHUB` that matches
    # neither exactly -> ambiguous (two share the lowercase form) -> leave unchanged but
    # flag UNRESOLVED.
    out = _run(
        {WH_MIXED: {"amount": "int"}, WH_LOWER: {"amount": "int"}},
        _upstream_wu(WH_UPPER),
    )
    assert _stored_upstream(out) == WH_UPPER
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.UNRESOLVED
    )


# --- match type discriminator -----------------------------------------------------


def test_match_type_normalized_when_rewritten():
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(UPPER))
    upstream = _upstream_aspect(out).upstreams[0]
    assert upstream.matchType == LineageMatchTypeClass.NORMALIZED


def test_match_type_exact_when_exact_match():
    out = _run({UPPER: {"amount": "int"}}, _upstream_wu(UPPER))
    upstream = _upstream_aspect(out).upstreams[0]
    assert upstream.matchType == LineageMatchTypeClass.EXACT


def test_match_type_unresolved_when_no_match():
    # Configured platform but the entity doesn't exist under any casing -> flag the
    # reference UNRESOLVED (left unchanged) so potentially broken lineage is visible.
    out = _run({}, _upstream_wu(UPPER))
    upstream = _upstream_aspect(out).upstreams[0]
    assert _stored_upstream(out) == UPPER
    assert upstream.matchType == LineageMatchTypeClass.UNRESOLVED
    assert out.get_aspect_of_type(UpstreamLineageClass) is not None


def test_fine_grained_match_type_normalized():
    out = _run(
        {LOWER: {"amount": "int"}}, _upstream_wu(UPPER, fine_grained_field="AMOUNT")
    )
    fg = _fine_grained(out)
    assert fg.matchType == LineageMatchTypeClass.NORMALIZED


def test_fine_grained_match_type_unresolved_when_no_match():
    # Configured platform, no matching entity -> field flagged UNRESOLVED in aggregate.
    out = _run({}, _upstream_wu(UPPER, fine_grained_field="amount"))
    fg = _fine_grained(out)
    assert fg.matchType == LineageMatchTypeClass.UNRESOLVED


# --- column-level (fine-grained) casing -------------------------------------------


def test_fine_grained_fixes_dataset_and_column_casing():
    # Existing entity is lowercase table with lowercase column "amount";
    # BI tool emitted uppercase table + uppercase column "AMOUNT".
    out = _run(
        {LOWER: {"amount": "int"}}, _upstream_wu(UPPER, fine_grained_field="AMOUNT")
    )
    fg = _fine_grained(out)
    assert fg.upstreams == [make_schema_field_urn(LOWER, "amount")]
    # Downstream field belongs to the entity itself and must never be touched.
    assert fg.downstreams == [make_schema_field_urn(DOWNSTREAM, "amount")]


def test_fine_grained_heals_pascalcase_upstream_column_cross_platform():
    # Mirrors a BI dataset (e.g. Power BI) whose column lineage points at a warehouse
    # (e.g. MSSQL): the BI side emits the upstream column lowercased ("orgid"), but the
    # warehouse stores it PascalCase ("OrgID"). The upstream field URN should be healed
    # to the warehouse's actual casing so the column-level edge connects, while the BI
    # dataset's own downstream column is left untouched.
    mssql_table = make_dataset_urn("mssql", "db.dbo.OrgSettings")
    pbi_dataset = make_dataset_urn("powerbi", "ws.model.org_settings")

    resolver = SchemaResolver(platform="mssql", env="PROD", graph=None)
    resolver.add_raw_schema_info(mssql_table, {"OrgID": "int"})
    provide_mock = mock.MagicMock(return_value=resolver)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="mssql", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.return_value = [mssql_table]
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    wu = MetadataChangeProposalWrapper(
        entityUrn=pbi_dataset,
        aspect=UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=mssql_table, type="TRANSFORMED")],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[make_schema_field_urn(mssql_table, "orgid")],
                    downstreams=[make_schema_field_urn(pbi_dataset, "OrgID")],
                )
            ],
        ),
    ).as_workunit()

    with mock.patch(_PATCH_TARGET, provide_mock):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([wu])))

    fg = _fine_grained(out)
    assert fg.upstreams == [make_schema_field_urn(mssql_table, "OrgID")]
    # Downstream (the BI dataset's own column) is never touched.
    assert fg.downstreams == [make_schema_field_urn(pbi_dataset, "OrgID")]


def test_fine_grained_fixes_column_casing_even_when_dataset_exact():
    # Dataset casing already correct, but column casing is wrong.
    out = _run(
        {UPPER: {"amount": "int"}}, _upstream_wu(UPPER, fine_grained_field="AMOUNT")
    )
    fg = _fine_grained(out)
    assert fg.upstreams == [make_schema_field_urn(UPPER, "amount")]
    # The parent matched exactly, but a corrected column path is still a normalization.
    assert fg.matchType == LineageMatchTypeClass.NORMALIZED


# --- multiple upstream platforms in one aspect ------------------------------------


def test_multi_platform_upstreams_both_healed():
    # A BI dataset (e.g. Hex) whose lineage references TWO warehouses; both are
    # configured. Each upstream is routed to the resolver for its own platform and
    # healed independently within the same aspect.
    sf_real = make_dataset_urn("snowflake", "DB.SCHEMA.Orders")
    rs_real = make_dataset_urn("redshift", "db.public.Customers")
    hex_dataset = make_dataset_urn("hex", "project.cell.combined")

    sf_resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    sf_resolver.add_raw_schema_info(sf_real, {"amount": "int"})
    rs_resolver = SchemaResolver(platform="redshift", env="PROD", graph=None)
    rs_resolver.add_raw_schema_info(rs_real, {"id": "int"})

    def fake_provide(graph, platform, platform_instance, env, batch_size=100):
        return sf_resolver if platform == "snowflake" else rs_resolver

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(platform="snowflake", env="PROD"),
            UpstreamPlatformCasing(platform="redshift", env="PROD"),
        ],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    # BI emits both upstreams with the wrong casing (snowflake lower, redshift upper).
    wu = MetadataChangeProposalWrapper(
        entityUrn=hex_dataset,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=make_dataset_urn("snowflake", "db.schema.orders"),
                    type="TRANSFORMED",
                ),
                UpstreamClass(
                    dataset=make_dataset_urn("redshift", "DB.PUBLIC.CUSTOMERS"),
                    type="TRANSFORMED",
                ),
            ],
        ),
    ).as_workunit()

    with mock.patch(_PATCH_TARGET, side_effect=fake_provide):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([wu])))

    healed = {u.dataset for u in _upstream_aspect(out).upstreams}
    assert sf_real in healed  # snowflake lower -> mixed
    assert rs_real in healed  # redshift upper -> mixed


def test_platform_urn_form_in_config_is_normalized():
    # Config may specify the platform as a full URN; it must still match the
    # normalized platform parsed from the dataset URN (else: silent no-op).
    resolver = _resolver({LOWER: {"amount": "int"}})
    provide_mock = mock.MagicMock(return_value=resolver)
    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(platform="urn:li:dataPlatform:snowflake", env="PROD")
        ],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.return_value = [LOWER]
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    with mock.patch(_PATCH_TARGET, provide_mock):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    assert _stored_upstream(out) == LOWER


def test_platform_instance_is_threaded_through_and_heals():
    # When an upstream platform is configured with a platform_instance, that instance
    # must be passed to the resolver provider, and an instance-qualified reference must
    # heal against the stored instance-qualified URN.
    stored = make_dataset_urn_with_platform_instance(
        "snowflake", "DB.SCHEMA.TABLE", "my_instance", "PROD"
    )
    referenced = make_dataset_urn_with_platform_instance(
        "snowflake", "db.schema.table", "my_instance", "PROD"
    )
    resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    resolver.add_raw_schema_info(stored, {"amount": "int"})
    provide_mock = mock.MagicMock(return_value=resolver)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(
                platform="snowflake", platform_instance="my_instance", env="PROD"
            )
        ],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.return_value = [stored]
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    with mock.patch(_PATCH_TARGET, provide_mock):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(referenced)])))

    assert _stored_upstream(out) == stored
    # The instance is threaded through to the resolver provider (membership + schema now
    # come from its single bulk scroll, so there is no separate URN-membership query).
    assert provide_mock.call_args.kwargs["platform_instance"] == "my_instance"


# --- dashboardInfo ----------------------------------------------------------------


def test_dashboard_info_dataset_refs_are_healed():
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        wu = MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn("looker", "dashboard.x"),
            aspect=DashboardInfoClass(
                title="x",
                description="",
                lastModified=ChangeAuditStampsClass(),
                datasets=[UPPER],
            ),
        ).as_workunit()
        [out] = list(processor.process(iter([wu])))
        assert _dashboard_aspect(out).datasets == [LOWER]
    finally:
        patcher.stop()


def test_dashboard_info_unresolved_ref_is_counted():
    # A dashboard pointing at a dataset that doesn't exist on a configured platform
    # must be counted as unresolved (not silently "unchanged"), even though a bare
    # dataset URN has no matchType field to stamp.
    processor, _provide, patcher = _make_processor({})  # nothing to match against
    try:
        wu = MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn("looker", "dashboard.z"),
            aspect=DashboardInfoClass(
                title="z",
                description="",
                lastModified=ChangeAuditStampsClass(),
                datasets=[UPPER],
            ),
        ).as_workunit()
        [out] = list(processor.process(iter([wu])))
        assert _dashboard_aspect(out).datasets == [UPPER]  # left unchanged
        assert processor.report.num_refs_unresolved == 1
        assert processor.report.num_refs_unchanged == 0
    finally:
        patcher.stop()


def test_chart_info_inputs_and_edges_are_healed():
    # Direct-query BI tools (Superset/Mode/Redash) point charts straight at warehouse
    # tables, so chartInfo inputs/inputEdges are upstream refs and get healed.
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        wu = MetadataChangeProposalWrapper(
            entityUrn="urn:li:chart:(superset,chart_1)",
            aspect=ChartInfoClass(
                title="c",
                description="",
                lastModified=ChangeAuditStampsClass(),
                inputs=[UPPER],
                inputEdges=[EdgeClass(destinationUrn=UPPER)],
            ),
        ).as_workunit()
        [out] = list(processor.process(iter([wu])))
        chart = out.get_aspect_of_type(ChartInfoClass)
        assert chart is not None
        assert chart.inputs == [LOWER]  # input dataset healed
        edges = chart.inputEdges
        assert (
            edges is not None and edges[0].destinationUrn == LOWER
        )  # input edge healed
    finally:
        patcher.stop()


def test_dashboard_info_dataset_edges_are_healed():
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        wu = MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn("looker", "dashboard.y"),
            aspect=DashboardInfoClass(
                title="y",
                description="",
                lastModified=ChangeAuditStampsClass(),
                datasetEdges=[EdgeClass(destinationUrn=UPPER)],
            ),
        ).as_workunit()
        [out] = list(processor.process(iter([wu])))
        edges = _dashboard_aspect(out).datasetEdges
        assert edges is not None
        assert edges[0].destinationUrn == LOWER
    finally:
        patcher.stop()


# --- safety / enablement ----------------------------------------------------------


def test_malformed_fine_grained_field_left_unchanged():
    # A field reference that can't be parsed is passed through, not crashed on.
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPPER, type="TRANSFORMED")],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=["not-a-valid-urn"],
                downstreams=[make_schema_field_urn(DOWNSTREAM, "amount")],
            )
        ],
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()
    out = _run({LOWER: {"amount": "int"}}, wu)
    assert _fine_grained(out).upstreams == ["not-a-valid-urn"]


def test_field_helpers_reject_non_schemafield_urns():
    # A dataset URN is not a schemaField URN -> both helpers return None. Previously
    # _field_path returned the dataset *name* as a bogus field path (positional
    # entity_ids[1]); SchemaFieldUrn parsing rejects it.
    from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns import (
        _field_path,
        _parent_dataset_urn,
    )

    assert _parent_dataset_urn(UPPER) is None
    assert _field_path(UPPER) is None
    sf = make_schema_field_urn(UPPER, "OrgID")
    assert _parent_dataset_urn(sf) == UPPER
    assert _field_path(sf) == "OrgID"


def test_non_dataset_upstream_ref_is_skipped():
    # A non-dataset upstream URN (e.g. a dataJob) is ignored, not resolved.
    datajob = "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,prod),task)"
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(datajob))
    assert _stored_upstream(out) == datajob


def test_malformed_upstream_ref_does_not_block_valid_sibling():
    # guess_entity_type() raises on a non-URN string; a malformed reference must be
    # skipped, NOT abort resolution of the valid siblings in the same aspect.
    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset="garbage", type="TRANSFORMED"),
            UpstreamClass(dataset=LOWER, type="TRANSFORMED"),
        ],
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()
    processor, _provide, patcher = _make_processor({UPPER: {"amount": "int"}})
    try:
        [out] = list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    upstreams = _upstream_aspect(out).upstreams
    assert upstreams[0].dataset == "garbage"  # malformed left untouched
    assert upstreams[1].dataset == UPPER  # valid sibling still healed
    assert processor.report.num_exceptions == 0


def test_empty_dataset_ref_does_not_block_valid_sibling():
    # An empty-string reference in a plain dataset list is skipped, not fatal, so the
    # valid sibling in the same list is still healed.
    aspect = DashboardInfoClass(
        title="t",
        description="d",
        lastModified=ChangeAuditStampsClass(),
        datasets=["", LOWER],
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()
    processor, _provide, patcher = _make_processor({UPPER: {"amount": "int"}})
    try:
        [out] = list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    datasets = _dashboard_aspect(out).datasets
    assert datasets == ["", UPPER]
    assert processor.report.num_exceptions == 0


def test_configured_platform_matching_nothing_warns(caplog):
    # Platform names are compared case-sensitively; a config typo like `Snowflake`
    # heals nothing. Surface that as an end-of-run warning instead of silently no-op.
    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="Snowflake", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    provide_mock = mock.MagicMock(return_value=_resolver({UPPER: {"amount": "int"}}))
    with mock.patch(_PATCH_TARGET, provide_mock):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        with caplog.at_level(logging.WARNING):
            [out] = list(processor.process(iter([_upstream_wu(LOWER)])))

    assert _stored_upstream(out) == LOWER  # not healed (platform-name mismatch)
    assert any(
        "matched no lineage references" in r.message and "Snowflake" in r.message
        for r in caplog.records
    )


def test_configured_platform_that_matches_does_not_warn(caplog):
    # The unmatched-platform warning must not fire when the configured platform is
    # actually used, so it doesn't cry wolf on healthy runs.
    with caplog.at_level(logging.WARNING):
        out = _run({UPPER: {"amount": "int"}}, _upstream_wu(LOWER))
    assert _stored_upstream(out) == UPPER
    assert not any("matched no lineage references" in r.message for r in caplog.records)


def test_exception_is_recorded_and_workunit_passed_through():
    # If resolution raises, the workunit is passed through unchanged and counted.
    provide_mock = mock.MagicMock(side_effect=RuntimeError("boom"))
    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    with mock.patch(_PATCH_TARGET, provide_mock):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))

    assert _stored_upstream(out) == UPPER  # unchanged
    assert processor.report.num_exceptions == 1  # counter kept
    # ...and surfaced to the pipeline report, not just the sub-report/logger.
    ctx.source_report.warning.assert_called_once()


def test_unresolved_refs_surface_one_aggregated_warning():
    # Configured platform, no matching entity -> UNRESOLVED -> ONE aggregated end-of-run
    # warning in the pipeline report (not per reference), with the count + a sample.
    processor, _provide, patcher = _make_processor(
        {}
    )  # empty catalog -> all UNRESOLVED
    try:
        list(processor.process(iter([_upstream_wu(UPPER), _upstream_wu(LOWER)])))
    finally:
        patcher.stop()

    assert processor.report.num_refs_unresolved == 2
    report = cast(mock.MagicMock, processor.ctx.source_report)
    report.warning.assert_called_once()
    kwargs = report.warning.call_args.kwargs
    assert "not resolved" in kwargs["title"].lower()
    assert "2 reference" in kwargs["context"]


def test_no_warning_when_all_refs_resolve():
    # A clean run (everything heals) emits no pipeline warning.
    processor, _provide, patcher = _make_processor({UPPER: {"amount": "int"}})
    try:
        list(processor.process(iter([_upstream_wu(LOWER)])))  # heals to UPPER
    finally:
        patcher.stop()

    assert processor.report.num_refs_unresolved == 0
    cast(mock.MagicMock, processor.ctx.source_report).warning.assert_not_called()


def test_entity_urn_is_never_rewritten():
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(UPPER))
    assert out.get_urn() == DOWNSTREAM


def test_non_lineage_workunits_pass_through_without_resolution():
    processor, provide_mock, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        status_wu = MetadataChangeProposalWrapper(
            entityUrn=DOWNSTREAM, aspect=StatusClass(removed=False)
        ).as_workunit()
        assert len(list(processor.process(iter([status_wu])))) == 1
        provide_mock.assert_not_called()
    finally:
        patcher.stop()


def test_raw_mcp_aspect_is_healed_and_written_back():
    # The file source emits raw MetadataChangeProposals (mcp_raw). get_aspect_of_type
    # returns a throwaway deserialized copy for those, so the in-place mutation must be
    # re-serialized back into the proposal — otherwise the rewrite is silently dropped.
    raw_mcp = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM,
        aspect=UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=UPPER, type="TRANSFORMED")],
        ),
    ).make_mcp()
    wu = MetadataWorkUnit(id="raw-mcp-test", mcp_raw=raw_mcp)

    out = _run({LOWER: {"amount": "int"}}, wu)

    healed = out.get_aspect_of_type(UpstreamLineageClass)
    assert healed is not None
    assert healed.upstreams[0].dataset == LOWER


def test_unchanged_raw_mcp_is_not_reserialized():
    # A raw MCP whose only upstream is on an unconfigured platform (out of scope) is not
    # mutated, so the processor must skip the (expensive) re-serialization: the
    # proposal's generic aspect payload is left as the very same object. This guards the
    # "only pay the deser/reser cost when something is actually fixed" optimization.
    bigquery_upstream = make_dataset_urn("bigquery", "PROJ.DS.T")
    raw_mcp = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM,
        aspect=UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=bigquery_upstream, type="TRANSFORMED")],
        ),
    ).make_mcp()
    wu = MetadataWorkUnit(id="raw-mcp-unchanged", mcp_raw=raw_mcp)
    assert isinstance(wu.metadata, MetadataChangeProposalClass)
    original_generic_aspect = wu.metadata.aspect

    out = _run({LOWER: {"amount": "int"}}, wu)

    assert isinstance(out.metadata, MetadataChangeProposalClass)
    assert out.metadata.aspect is original_generic_aspect


def test_workunit_level_counters_track_lineage_and_modified():
    # The deser/reser cost ratio is per-workunit: both workunits carry a lineage aspect
    # (deserialization paid), but only the in-scope one is mutated (re-serialization).
    processor, _provide, patcher = _make_processor({UPPER: {"amount": "int"}})
    try:
        healed = _upstream_wu(LOWER)  # snowflake, configured -> normalized -> modified
        out_of_scope = _upstream_wu(
            make_dataset_urn("bigquery", "P.D.T")
        )  # not configured
        list(processor.process(iter([healed, out_of_scope])))
    finally:
        patcher.stop()

    assert processor.report.num_workunits_with_lineage_aspect == 2
    assert processor.report.num_workunits_modified == 1


def test_module_import_does_not_pull_sqlglot():
    # Importing this module (e.g. via the workunit_processors package) must not drag
    # in sqlglot, or connectors that don't declare it would break. The invariant rests
    # on deferred imports + `from __future__ import annotations`; assert it in a fresh
    # interpreter, since this test session may already have sqlglot loaded.
    code = (
        "import sys; "
        "import datahub.ingestion.workunit_processors.auto_resolve_lineage_urns; "
        "assert 'sqlglot' not in sys.modules, 'sqlglot imported at module load'"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_mce_aspect_is_healed():
    # The legacy MCE path carries aspects as live objects in proposedSnapshot.aspects,
    # so in-place mutation lands directly (no write-back needed).
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=DOWNSTREAM,
            aspects=[
                UpstreamLineageClass(
                    upstreams=[UpstreamClass(dataset=UPPER, type="TRANSFORMED")],
                )
            ],
        )
    )
    wu = MetadataWorkUnit(id="mce-test", mce=mce)

    out = _run({LOWER: {"amount": "int"}}, wu)

    healed = out.get_aspect_of_type(UpstreamLineageClass)
    assert healed is not None
    assert healed.upstreams[0].dataset == LOWER


def test_datajob_io_inputs_and_fine_grained_are_healed():
    # dbt / Airflow / Spark warehouse-upstream path: a DataJob's inputs are healed
    # (table, edge, and fine-grained columns); its outputs are left untouched.
    job = "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,prod),task)"
    wu = MetadataChangeProposalWrapper(
        entityUrn=job,
        aspect=DataJobInputOutputClass(
            inputDatasets=[UPPER],
            outputDatasets=[MIXED],
            inputDatasetEdges=[EdgeClass(destinationUrn=UPPER)],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[make_schema_field_urn(UPPER, "AMOUNT")],
                    downstreams=[make_schema_field_urn(DOWNSTREAM, "amount")],
                )
            ],
        ),
    ).as_workunit()

    out = _run({LOWER: {"amount": "int"}}, wu)

    io = out.get_aspect_of_type(DataJobInputOutputClass)
    assert io is not None
    assert io.inputDatasets == [LOWER]  # input table healed
    edges = io.inputDatasetEdges
    assert edges is not None and edges[0].destinationUrn == LOWER  # input edge healed
    assert io.outputDatasets == [MIXED]  # output left untouched
    fgl = io.fineGrainedLineages
    assert fgl is not None
    assert fgl[0].upstreams == [make_schema_field_urn(LOWER, "amount")]


def _ctx(
    enabled: bool,
    graph: object,
    upstream_platforms: Optional[List[UpstreamPlatformCasing]] = None,
) -> mock.MagicMock:
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = graph
    pipeline_ctx.flags.auto_resolve_lineage_urns = AutoResolveLineageUrnsConfig(
        enabled=enabled,
        upstream_platforms=upstream_platforms
        if upstream_platforms is not None
        else [UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    return ctx


def test_disabled_without_graph():
    assert AutoResolveLineageUrnsProcessor.should_enable(_ctx(True, None)) is False


def test_disabled_when_flag_off():
    assert (
        AutoResolveLineageUrnsProcessor.should_enable(_ctx(False, mock.MagicMock()))
        is False
    )


def test_disabled_when_no_upstream_platforms():
    # Enabled but unconfigured = active no-op; should_enable guards against it.
    assert (
        AutoResolveLineageUrnsProcessor.should_enable(
            _ctx(True, mock.MagicMock(), upstream_platforms=[])
        )
        is False
    )


def test_enabled_when_flag_on_with_graph():
    assert (
        AutoResolveLineageUrnsProcessor.should_enable(_ctx(True, mock.MagicMock()))
        is True
    )


def test_env_is_validated_and_normalized():
    # UpstreamPlatformCasing inherits EnvConfigMixin, so env is validated + uppercased
    # (a lowercase value heals instead of silently under-resolving; a typo is rejected).
    assert UpstreamPlatformCasing(platform="snowflake", env="prod").env == "PROD"
    with pytest.raises(pydantic.ValidationError):
        UpstreamPlatformCasing(platform="snowflake", env="not-an-env")


def test_disabled_under_bare_mock_ctx():
    # Regression: the processor is in the shared chain for every source, and some
    # integration tests build a source with a bare Mock() ctx (e.g. salesforce). There
    # cfg.enabled / cfg.upstream_platforms are truthy Mocks and graph is non-None, so a
    # naive check would enable the processor with a Mock config and crash mid-run. It
    # must fail closed.
    assert AutoResolveLineageUrnsProcessor.should_enable(mock.MagicMock()) is False


def test_config_requires_sql_parser_only_when_enabled(monkeypatch):
    # sqlglot is not in the ingestion core. Enabling the feature without it must fail
    # fast at config parse (only when enabled), with an actionable message — not deep in
    # the processor at run time. Simulate the missing dependency by nulling the module.
    monkeypatch.setitem(sys.modules, "sqlglot", None)

    # Disabled: no requirement, config validates fine.
    AutoResolveLineageUrnsConfig(enabled=False)

    # Enabled: the SQL parser is required, so config validation fails.
    with pytest.raises(pydantic.ValidationError, match="sql-parser"):
        AutoResolveLineageUrnsConfig(
            enabled=True,
            upstream_platforms=[
                UpstreamPlatformCasing(platform="snowflake", env="PROD")
            ],
        )
