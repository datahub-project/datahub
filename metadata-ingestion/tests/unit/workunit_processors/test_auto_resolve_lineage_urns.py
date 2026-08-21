import subprocess
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast
from unittest import mock

import pydantic
import pytest

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
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
    ChangeTypeClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataJobInputOutputClass,
    DatasetSnapshotClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GenericAspectClass,
    LineageMatchTypeClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.server_config_util import RestServiceConfig
from datahub.utilities.urn_aliases.resolver import (
    UrnAliasResolver,
    lowercased_urn,
)

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

# Imported at module scope by the processor, so patch it there.
_LOAD_INDEX_TARGET = (
    "datahub.ingestion.workunit_processors.auto_resolve_lineage_urns"
    ".provide_urn_alias_resolver"
)


class _patchers:
    """Several patches behind the one start/stop handle the tests already pass around."""

    def __init__(self, *patches: Any) -> None:
        self._patches = patches

    def start(self) -> None:
        for patch in self._patches:
            patch.start()

    def stop(self) -> None:
        for patch in self._patches:
            patch.stop()


def _resolver(
    schemas: Dict[str, Dict[str, str]], platform: str = "snowflake"
) -> SchemaResolver:
    """A graph-less resolver pre-populated with {urn: {column: type}}."""
    resolver = SchemaResolver(platform=platform, env="PROD", graph=None)
    for urn, schema in schemas.items():
        resolver.add_raw_schema_info(urn, schema)
    return resolver


def _registers_aliases(graph: Any) -> Any:
    """Pin `graph` as a server new enough to maintain the dataset `aliases` aspect."""
    graph.server_config = RestServiceConfig(
        raw_config={"versions": {"acryldata/datahub": {"version": "v1.8.0"}}}
    )
    return graph


def _schema_fetches(graph: mock.MagicMock) -> int:
    """How many schemas were asked of `graph`; the one marker read is not one of them."""
    return sum(
        1
        for call in graph.get_aspect.call_args_list
        if call.args[1] is SchemaMetadataClass
    )


_Region = Tuple[str, Optional[str], str]

_SNOWFLAKE_PROD: _Region = ("snowflake", None, "PROD")


def _seed_index(
    graph: Any,
    urns: List[str],
    regions: Sequence[_Region] = (_SNOWFLAKE_PROD,),
) -> Any:
    """A `provide_urn_alias_resolver` stand-in, plus a graph that knows every seeded URN.

    Each region in `regions` gets a graph-less resolver holding only what its own filter
    would have reached; a region not listed yields None, as a failed load does. Either way a
    miss falls through to the graph, which answers from `urns` as DataHub would.
    """
    _registers_aliases(graph)
    graph.get_dataset_urns_ignoring_case.side_effect = lambda key: [
        urn for urn in urns if lowercased_urn(urn) == key
    ]
    resolvers: Dict[_Region, UrnAliasResolver] = {}
    for region in regions:
        platform, instance, env = region
        resolver = UrnAliasResolver()
        for urn in urns:
            in_region = f"dataPlatform:{platform}," in urn and urn.endswith(f",{env})")
            if in_region and (instance is None or f",{instance}." in urn):
                resolver.add(urn)
        resolvers[region] = resolver

    def _load(
        *,
        graph: Any,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        **kwargs: Any,
    ) -> Optional[UrnAliasResolver]:
        return resolvers.get((platform, platform_instance, env))

    return mock.MagicMock(side_effect=_load)


def _make_processor(
    schemas: Dict[str, Dict[str, str]],
    urns: Optional[List[str]] = None,
) -> Tuple[AutoResolveLineageUrnsProcessor, mock.MagicMock, Any]:
    """Patch provide_schema_resolver to a single seeded snowflake resolver.

    `schemas` maps existing URN -> column schema. `urns` adds entities that exist in
    DataHub with no schemaMetadata: resolvable by URN but with no columns to match
    against.
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
    load_index = _seed_index(pipeline_ctx.graph, [*schemas, *(urns or [])])
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    # The processor imports provide_schema_resolver once in __init__ (a single sqlglot
    # chokepoint) and caches it, so the patch must be active *before* construction.
    patcher = _patchers(
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    )
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
    urns: Optional[List[str]] = None,
) -> MetadataWorkUnit:
    processor, _provide, patcher = _make_processor(schemas, urns)
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
        (WH_LOWER, WH_MIXED),  # BI emits mixed, warehouse stores lowercase
    ],
)
def test_heals_to_stored_casing_when_warehouse_lowercase(
    stored: str, emitted: str
) -> None:
    # SchemaResolver.resolve_table matches the reference's original casing and its
    # lowercased form, so a reference heals when the warehouse stores the entity
    # lowercased (any BI casing -> lowercase). Non-lowercase stored casings are not yet
    # reachable — that is the tracked normalizedUrn SchemaResolver follow-up (see below).
    out = _run({stored: {"amount": "int"}}, _upstream_wu(emitted))
    assert _stored_upstream(out) == stored


@pytest.mark.parametrize(
    "stored,emitted",
    [
        (UPPER, LOWER),  # warehouse uppercase
        (WH_MIXED, WH_LOWER),  # warehouse mixed, lower emitted
        (WH_MIXED, WH_UPPER),  # warehouse mixed, upper emitted
    ],
)
def test_heals_to_stored_casing_when_warehouse_is_not_lowercase(
    stored: str, emitted: str
) -> None:
    # The alias index maps any casing to the stored URN, so a warehouse keeping an
    # UPPER/Mixed identity is reachable from any BI casing.
    out = _run({stored: {"amount": "int"}}, _upstream_wu(emitted))
    assert _stored_upstream(out) == stored
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.NORMALIZED
    )


def test_keeps_exact_when_exact_entity_exists():
    out = _run(
        {UPPER: {"amount": "int"}, LOWER: {"amount": "int"}}, _upstream_wu(UPPER)
    )
    assert _stored_upstream(out) == UPPER


@pytest.mark.parametrize(
    "stored_a,stored_b,emitted",  # stored_b is the lowercase-named entity
    [
        (UPPER, LOWER, MIXED),
        (WH_MIXED, WH_LOWER, WH_UPPER),
    ],
)
def test_heals_a_casing_collision_to_the_lowercase_entity(
    stored_a: str, stored_b: str, emitted: str
) -> None:
    # Two real entities differ only by case and the reference matches neither exactly.
    # The processor asks for prefer_lowercased, so lineage heals to the lowercase-named
    # entity rather than being left broken.
    out = _run(
        {stored_a: {"amount": "int"}, stored_b: {"amount": "int"}},
        _upstream_wu(emitted),
    )
    assert _stored_upstream(out) == stored_b
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.NORMALIZED
    )


def test_reports_unresolved_when_a_collision_has_no_lowercase_entity() -> None:
    # Mixed and UPPER both exist but neither is lowercase-named, so the preference has
    # nothing to pick and the reference is left alone.
    out = _run(
        {WH_MIXED: {"amount": "int"}, WH_UPPER: {"amount": "int"}},
        _upstream_wu(WH_LOWER),
    )
    assert _stored_upstream(out) == WH_LOWER
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.UNRESOLVED
    )


def test_heals_a_reference_to_a_dataset_that_has_no_schema() -> None:
    # The entity exists but has no schemaMetadata, so it is absent from the schema cache.
    # Table-level lineage must still be healed.
    out = _run({}, _upstream_wu(WH_UPPER), urns=[WH_MIXED])
    assert _stored_upstream(out) == WH_MIXED
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.NORMALIZED
    )


def test_does_not_heal_across_environments() -> None:
    # The configured resolver is PROD. A DEV reference must not be rewritten to the
    # same-named PROD entity, which would point lineage at the wrong environment.
    dev_ref = make_dataset_urn("snowflake", "DB.SCHEMA.TABLE", env="DEV")
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(dev_ref))
    assert _stored_upstream(out) == dev_ref
    # snowflake is in scope, so the reference is checked. Matching is on the whole URN, so
    # the PROD entity is not a match and DataHub holds no DEV one.
    assert _upstream_aspect(out).upstreams[0].matchType == (
        LineageMatchTypeClass.UNRESOLVED
    )


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

    resolver = _resolver({mssql_table: {"OrgID": "int"}}, platform="mssql")
    provide_mock = mock.MagicMock(return_value=resolver)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="mssql", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    load_index = _seed_index(
        pipeline_ctx.graph,
        [mssql_table],
        [("mssql", None, "PROD")],
    )
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

    with (
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
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
    sf_real = make_dataset_urn("snowflake", "db.schema.orders")
    rs_real = make_dataset_urn("redshift", "db.public.customers")
    hex_dataset = make_dataset_urn("hex", "project.cell.combined")

    sf_resolver = _resolver({sf_real: {"amount": "int"}})
    rs_resolver = _resolver({rs_real: {"id": "int"}}, platform="redshift")

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
    load_index = _seed_index(
        pipeline_ctx.graph,
        [sf_real, rs_real],
        [
            ("snowflake", None, "PROD"),
            ("redshift", None, "PROD"),
        ],
    )
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    # BI emits both upstreams uppercased; both warehouses store lowercase, so each heals.
    wu = MetadataChangeProposalWrapper(
        entityUrn=hex_dataset,
        aspect=UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=make_dataset_urn("snowflake", "DB.SCHEMA.ORDERS"),
                    type="TRANSFORMED",
                ),
                UpstreamClass(
                    dataset=make_dataset_urn("redshift", "DB.PUBLIC.CUSTOMERS"),
                    type="TRANSFORMED",
                ),
            ],
        ),
    ).as_workunit()

    with (
        mock.patch(_PATCH_TARGET, side_effect=fake_provide),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([wu])))

    healed = {u.dataset for u in _upstream_aspect(out).upstreams}
    assert sf_real in healed  # snowflake upper -> lower
    assert rs_real in healed  # redshift upper -> lower


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
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    load_index = _seed_index(pipeline_ctx.graph, [LOWER])
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    with (
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    assert _stored_upstream(out) == LOWER


def test_platform_instance_is_threaded_through_and_heals():
    # When an upstream platform is configured with a platform_instance, that instance
    # must be passed to the resolver provider, and an instance-qualified reference must
    # heal against the stored instance-qualified URN.
    stored = make_dataset_urn_with_platform_instance(
        "snowflake", "db.schema.table", "my_instance", "PROD"
    )
    referenced = make_dataset_urn_with_platform_instance(
        "snowflake", "DB.SCHEMA.TABLE", "my_instance", "PROD"
    )
    resolver = _resolver({stored: {"amount": "int"}})
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
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    load_index = _seed_index(
        pipeline_ctx.graph,
        [stored],
        [("snowflake", "my_instance", "PROD")],
    )
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    with (
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
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
        assert processor.report.num_refs_verified_exact == 0
        assert processor.report.num_refs_out_of_scope == 0
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
            UpstreamClass(dataset=UPPER, type="TRANSFORMED"),
        ],
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        [out] = list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    upstreams = _upstream_aspect(out).upstreams
    assert upstreams[0].dataset == "garbage"  # malformed left untouched
    assert upstreams[1].dataset == LOWER  # valid sibling still healed
    assert processor.report.num_exceptions == 0


def test_empty_dataset_ref_does_not_block_valid_sibling():
    # An empty-string reference in a plain dataset list is skipped, not fatal, so the
    # valid sibling in the same list is still healed.
    aspect = DashboardInfoClass(
        title="t",
        description="d",
        lastModified=ChangeAuditStampsClass(),
        datasets=["", UPPER],
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        [out] = list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    datasets = _dashboard_aspect(out).datasets
    assert datasets == ["", LOWER]
    assert processor.report.num_exceptions == 0


def _unmatched_platform_warned(source_report: mock.MagicMock) -> bool:
    return any(
        "matched no lineage references" in c.kwargs.get("title", "").lower()
        for c in source_report.warning.call_args_list
    )


def test_configured_platform_matching_nothing_warns():
    # Platform names are compared case-sensitively; a config typo like `Snowflake` heals
    # nothing. Surface it as a structured (UI-visible) pipeline-report warning, not just a
    # log line.
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
    load_index = _seed_index(
        pipeline_ctx.graph,
        [UPPER],
        [("Snowflake", None, "PROD")],
    )
    with (
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(LOWER)])))

    assert _stored_upstream(out) == LOWER  # not healed (platform-name mismatch)
    report = cast(mock.MagicMock, ctx.source_report)
    assert _unmatched_platform_warned(report)
    context = report.warning.call_args_list[-1].kwargs.get("context", "")
    assert "Snowflake" in context


def test_configured_platform_that_matches_does_not_warn():
    # The unmatched-platform warning must not fire when the configured platform is
    # actually used, so it doesn't cry wolf on healthy runs.
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    finally:
        patcher.stop()
    assert _stored_upstream(out) == LOWER
    assert not _unmatched_platform_warned(
        cast(mock.MagicMock, processor.ctx.source_report)
    )


def test_catalog_load_failure_is_reported_and_passes_through():
    # If bulk-loading a platform's catalog fails, the failure is surfaced to the pipeline
    # report and lineage is emitted unchanged rather than crashing the pipeline.
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
    # No region loads, so neither the alias index nor the schema cache is built.
    load_index = _seed_index(pipeline_ctx.graph, [], regions=[])

    with (
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))

    assert _stored_upstream(out) == UPPER  # unchanged: DataHub has no such entity
    # Identity and columns load separately, so each failure is reported on its own. The
    # reference is still checked, by asking, hence the third warning.
    titles = {
        c.kwargs["title"]
        for c in cast(mock.MagicMock, ctx.source_report).warning.call_args_list
    }
    assert titles == {
        "Lineage URN casing: upstream URNs not loaded",
        "Lineage URN casing: upstream catalog not loaded",
        "Lineage references not resolved to an existing entity",
    }


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
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        list(processor.process(iter([_upstream_wu(UPPER)])))  # heals to LOWER
    finally:
        patcher.stop()

    assert processor.report.num_refs_unresolved == 0
    cast(mock.MagicMock, processor.ctx.source_report).warning.assert_not_called()


def test_entity_urn_is_never_rewritten():
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(UPPER))
    assert out.get_urn() == DOWNSTREAM


def test_non_lineage_workunits_pass_through_without_resolution():
    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        status_wu = MetadataChangeProposalWrapper(
            entityUrn=DOWNSTREAM, aspect=StatusClass(removed=False)
        ).as_workunit()
        [out] = list(processor.process(iter([status_wu])))
        assert out is status_wu  # passed through untouched
        assert processor.report.num_workunits_with_lineage_aspect == 0
        assert processor.report.num_workunits_modified == 0
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


def test_patch_lineage_is_skipped_and_counted():
    # A lineage aspect emitted as a PATCH (not UPSERT) can't be reconciled: for a raw MCP,
    # get_aspect_of_type routes through try_from_mcpc, which drops non-upserts. It must be
    # counted (not silently passed through) and left unchanged. dataJobInputOutput is
    # emitted as a patch by some dbt / Airflow / Spark paths.
    raw_patch = MetadataChangeProposalClass(
        entityType="dataJob",
        entityUrn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task)",
        changeType=ChangeTypeClass.PATCH,
        aspectName=DataJobInputOutputClass.ASPECT_NAME,
        aspect=GenericAspectClass(
            value=b"[]", contentType="application/json-patch+json"
        ),
    )
    wu = MetadataWorkUnit(id="patch-lineage", mcp_raw=raw_patch)
    original_aspect = raw_patch.aspect

    processor, _provide, patcher = _make_processor({LOWER: {"amount": "int"}})
    try:
        [out] = list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    assert processor.report.num_patch_lineage_skipped == 1
    # Skipped before the normalizer loop, so it doesn't count as a lineage-bearing wu.
    assert processor.report.num_workunits_with_lineage_aspect == 0
    assert isinstance(out.metadata, MetadataChangeProposalClass)
    assert out.metadata.aspect is original_aspect  # passed through untouched


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
    resolve_all_platforms: bool = False,
) -> mock.MagicMock:
    pipeline_ctx = mock.MagicMock()
    # A server that maintains `aliases`; the gate on that is exercised on its own below.
    pipeline_ctx.graph = (
        _registers_aliases(graph) if isinstance(graph, mock.MagicMock) else graph
    )
    pipeline_ctx.flags.auto_resolve_lineage_urns = AutoResolveLineageUrnsConfig(
        enabled=enabled,
        upstream_platforms=upstream_platforms
        if upstream_platforms is not None
        else [UpstreamPlatformCasing(platform="snowflake", env="PROD")],
        resolve_all_platforms=resolve_all_platforms,
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


def test_enabled_with_nothing_in_scope_is_a_config_error():
    # Nothing preloaded and scope not widened has nothing to reconcile against -> fail
    # fast at config parse rather than silently no-op.
    with pytest.raises(pydantic.ValidationError, match="upstream_platforms"):
        AutoResolveLineageUrnsConfig(enabled=True, upstream_platforms=[])


def test_widened_scope_needs_no_upstream_platforms():
    # The other half of the same rule: with every platform in scope there is something
    # to reconcile even with nothing preloaded, so this is a valid config.
    AutoResolveLineageUrnsConfig(enabled=True, resolve_all_platforms=True)
    assert (
        AutoResolveLineageUrnsProcessor.should_enable(
            _ctx(
                True,
                mock.MagicMock(),
                upstream_platforms=[],
                resolve_all_platforms=True,
            )
        )
        is True
    )


def test_enabled_when_flag_on_with_graph():
    assert (
        AutoResolveLineageUrnsProcessor.should_enable(_ctx(True, mock.MagicMock()))
        is True
    )


def test_disabled_where_the_server_does_not_maintain_aliases():
    # Resolution reads the `aliases` aspect, so an older server cannot answer at all —
    # and approximating would report healthy lineage as broken. Off, with a warning.
    ctx = _ctx(True, mock.MagicMock())
    ctx.pipeline_context.graph.server_config = RestServiceConfig(
        raw_config={"versions": {"acryldata/datahub": {"version": "v1.7.0"}}}
    )

    assert AutoResolveLineageUrnsProcessor.should_enable(ctx) is False
    assert (
        cast(mock.MagicMock, ctx.source_report).warning.call_args.kwargs["title"]
        == "Lineage URN casing resolution disabled"
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


# --- identity from a shared index, columns from our own load -----------------------


def _instanced(name: str, instance: str) -> str:
    return make_dataset_urn_with_platform_instance("snowflake", name, instance, "PROD")


def _processor_for(
    cfg: AutoResolveLineageUrnsConfig,
    resolver: SchemaResolver,
    seed: List[str],
    regions: Sequence[_Region],
    provide: Optional[mock.MagicMock] = None,
) -> Tuple[AutoResolveLineageUrnsProcessor, Any, Any]:
    """`provide` overrides the patched provide_schema_resolver, for a load that fails."""
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = cfg
    load_index = _seed_index(pipeline_ctx.graph, seed, regions)
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    provide_mock = provide or mock.MagicMock(return_value=resolver)
    patcher = _patchers(
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    )
    patcher.start()
    return AutoResolveLineageUrnsProcessor.create(ctx), pipeline_ctx.graph, patcher


# --- scope: which references get reconciled ------------------------------------------
#
# upstream_platforms says which platforms to reconcile and which catalogs to preload;
# resolve_all_platforms says whether anything else is reconciled at all.

BQ_UPPER = make_dataset_urn("bigquery", "PROJ.DS.T")
BQ_LOWER = make_dataset_urn("bigquery", "proj.ds.t")

_SNOWFLAKE_SLICE: _Region = ("snowflake", None, "PROD")


def _widened(
    upstream_platforms: Optional[List[UpstreamPlatformCasing]] = None,
) -> AutoResolveLineageUrnsConfig:
    return AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=upstream_platforms
        if upstream_platforms is not None
        else [UpstreamPlatformCasing(platform="snowflake", env="PROD")],
        resolve_all_platforms=True,
    )


def _schema_metadata(platform: str, columns: List[str]) -> SchemaMetadataClass:
    """What `graph.get_aspect` hands back for an entity DataHub holds columns for."""
    return SchemaMetadataClass(
        schemaName="t",
        platform=make_data_platform_urn(platform),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=column,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="STRING",
            )
            for column in columns
        ],
    )


def test_a_preloaded_miss_is_still_asked_about():
    # A preload covers one platform_instance / env, so its miss is not an absence.
    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    processor, graph, patcher = _processor_for(
        cfg, _resolver({}), [], [_SNOWFLAKE_SLICE]
    )
    try:
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == UPPER  # DataHub does not have it either
    assert processor.report.num_refs_unresolved == 1
    graph.get_dataset_urns_ignoring_case.assert_called_once()


def test_an_unlisted_platform_is_healed_when_scope_is_widened():
    # The long tail this flag exists for: snowflake is referenced often enough to
    # preload, bigquery only occasionally. The bigquery reference must actually be
    # rewritten — resolved and then discarded for being unlisted is the bug.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [BQ_LOWER], [_SNOWFLAKE_SLICE]
    )
    graph.get_aspect.return_value = None  # exists, but holds no schemaMetadata
    try:
        [out] = list(processor.process(iter([_upstream_wu(BQ_UPPER)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == BQ_LOWER
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.NORMALIZED
    )
    assert processor.report.num_dataset_urns_normalized == 1


def test_an_unlisted_platform_gets_column_casing_too():
    # Table-level healing alone would leave the field URN pointing at a column name
    # DataHub does not have, so the edge stays broken where it matters.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [BQ_LOWER], [_SNOWFLAKE_SLICE]
    )
    graph.get_aspect.return_value = _schema_metadata("bigquery", ["amount"])
    try:
        [out] = list(
            processor.process(
                iter([_upstream_wu(BQ_UPPER, fine_grained_field="AMOUNT")])
            )
        )
    finally:
        patcher.stop()

    assert _fine_grained(out).upstreams == [make_schema_field_urn(BQ_LOWER, "amount")]


def test_a_failed_schema_fetch_keeps_the_table_level_healing():
    # The fetch is column enrichment on top of an identity that already resolved; a
    # network failure there must not take the resolved table reference down with it.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [BQ_LOWER], [_SNOWFLAKE_SLICE]
    )
    graph.get_aspect.side_effect = Exception("boom")
    try:
        [out] = list(
            processor.process(
                iter([_upstream_wu(BQ_UPPER, fine_grained_field="AMOUNT")])
            )
        )
    finally:
        patcher.stop()

    assert _stored_upstream(out) == BQ_LOWER
    assert _fine_grained(out).upstreams == [make_schema_field_urn(BQ_LOWER, "AMOUNT")]
    assert processor.report.num_dataset_urns_normalized == 1
    assert processor.report.num_column_urns_normalized == 1
    # The parent moved, so NORMALIZED is honest — but nothing checked the column.
    assert _fine_grained(out).matchType == LineageMatchTypeClass.NORMALIZED
    # Only the field's parent asks for columns — the table-level upstream has none to
    # reconcile. A failed fetch is not negative-cached, so a transient failure can still be
    # recovered by a later reference.
    assert processor.report.num_schema_fetches_failed == 1
    assert processor.report.num_exceptions == 0


def test_a_listed_platform_is_still_answered_locally_when_scope_is_widened():
    # Widening scope must not turn a preloaded catalog back into a stream of questions.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({LOWER: {"amount": "int"}}), [LOWER], [_SNOWFLAKE_SLICE]
    )
    try:
        [out] = list(
            processor.process(iter([_upstream_wu(UPPER, fine_grained_field="AMOUNT")]))
        )
    finally:
        patcher.stop()

    assert _fine_grained(out).upstreams == [make_schema_field_urn(LOWER, "amount")]
    graph.get_dataset_urns_ignoring_case.assert_not_called()
    assert _schema_fetches(graph) == 0


def test_a_schemaless_listed_entity_is_asked_about_once():
    # A preload is a cache, not an authority, so "not in my copy" is not "has no columns".
    # The column is fetched and comes back empty: one round trip per schemaless reference.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [LOWER], [_SNOWFLAKE_SLICE]
    )
    try:
        [out] = list(
            processor.process(iter([_upstream_wu(UPPER, fine_grained_field="AMOUNT")]))
        )
    finally:
        patcher.stop()

    # Healed at table level; the column is left as the source reported it.
    assert _fine_grained(out).upstreams == [make_schema_field_urn(LOWER, "AMOUNT")]
    assert _schema_fetches(graph) == 1


def test_a_failing_preloaded_urn_catalog_still_lets_datahub_answer():
    # Identity follows the same rule as columns: a preloaded catalog that raises has not
    # said the entity is absent, and DataHub's search is exhaustive, so the reference still
    # heals rather than going unstamped.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [LOWER], [_SNOWFLAKE_SLICE]
    )
    [preloaded] = [
        resolver
        for resolvers in processor._alias_resolvers.values()
        for resolver in resolvers
    ]
    try:
        with mock.patch.object(
            preloaded, "find_match", side_effect=Exception("sqlite went away")
        ):
            [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == LOWER
    assert processor.report.num_dataset_urns_normalized == 1
    assert processor.report.num_refs_lookup_failed == 0
    assert processor.report.num_exceptions == 0


def test_a_preloaded_collision_is_not_re_asked_of_datahub():
    # A preload's row holds every casing of its key, so a collision in it is the answer,
    # not a miss. Re-asked, DataHub could come back a single match — here only the
    # uppercase entity carries the `aliases` aspect the search reads — and the reference
    # would heal to it, silently pointing lineage at the wrong table.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [WH_MIXED, WH_UPPER], [_SNOWFLAKE_SLICE]
    )
    graph.get_dataset_urns_ignoring_case.side_effect = lambda key: [WH_UPPER]
    try:
        [out] = list(processor.process(iter([_upstream_wu(WH_LOWER)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == WH_LOWER
    assert (
        _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.UNRESOLVED
    )
    graph.get_dataset_urns_ignoring_case.assert_not_called()


def test_a_failing_preloaded_resolver_still_lets_datahub_answer():
    # A raise is not an absence. A slice whose load failed has its columns fetched, and a
    # slice that breaks at read time is no different — DataHub is still asked, so the
    # columns are recovered and nothing is reported lost.
    failing = mock.MagicMock()
    failing.schema_count.return_value = 0
    failing.resolve_urn.side_effect = Exception("sqlite went away")
    processor, graph, patcher = _processor_for(
        _widened(), failing, [LOWER], [_SNOWFLAKE_SLICE]
    )
    graph.get_aspect.return_value = _schema_metadata("snowflake", ["amount"])
    try:
        [out] = list(
            processor.process(iter([_upstream_wu(UPPER, fine_grained_field="AMOUNT")]))
        )
    finally:
        patcher.stop()

    assert _fine_grained(out).upstreams == [make_schema_field_urn(LOWER, "amount")]
    assert processor.report.num_schema_fetches_failed == 0
    assert processor.report.num_exceptions == 0


def test_a_failing_preloaded_schema_resolver_keeps_the_table_level_healing():
    # Neither the preloaded catalog nor the fetch can answer. Unhandled, the catalog's
    # failure escaped into process() and discarded the whole work unit's reconciliation —
    # losing the table casing as well as the columns.
    failing = mock.MagicMock()
    failing.schema_count.return_value = 0
    failing.resolve_urn.side_effect = Exception("sqlite went away")
    processor, graph, patcher = _processor_for(
        _widened(), failing, [LOWER], [_SNOWFLAKE_SLICE]
    )
    graph.get_aspect.side_effect = Exception("boom")
    try:
        [out] = list(
            processor.process(iter([_upstream_wu(UPPER, fine_grained_field="AMOUNT")]))
        )
    finally:
        patcher.stop()

    assert _stored_upstream(out) == LOWER
    assert processor.report.num_dataset_urns_normalized == 1
    assert processor.report.num_schema_fetches_failed == 1
    assert processor.report.num_exceptions == 0


def test_table_only_lineage_fetches_no_schema():
    # Columns are fetched for the column-level path alone. A table-level reference has no
    # columns to reconcile, so paying a schema query per resolved entity would double the
    # round trips of the common BI aspect for nothing.
    processor, graph, patcher = _processor_for(_widened(), _resolver({}), [LOWER], [])
    try:
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == LOWER
    assert _schema_fetches(graph) == 0


def test_widened_scope_with_no_upstream_platforms_reads_nothing_up_front():
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.auto_resolve_lineage_urns = _widened(upstream_platforms=[])
    _registers_aliases(pipeline_ctx.graph)
    pipeline_ctx.graph.get_dataset_urns_ignoring_case.return_value = [LOWER]
    pipeline_ctx.graph.get_aspect.return_value = None
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    provide_mock = mock.MagicMock()
    load_index = mock.MagicMock()
    with (
        mock.patch(_PATCH_TARGET, provide_mock),
        mock.patch(_LOAD_INDEX_TARGET, load_index),
    ):
        processor = AutoResolveLineageUrnsProcessor.create(ctx)
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))

    # No catalog is read; every reference is resolved by asking instead.
    provide_mock.assert_not_called()
    load_index.assert_not_called()
    assert _stored_upstream(out) == LOWER


# --- a catalog load that failed is not a catalog we hold ------------------------------
#
# A slice is claimed only by a scroll that finished. Claiming one the config merely asked
# for would tell the fallbacks below that a miss there is an answer, when in truth nothing
# was ever fetched.

_MY_INSTANCE = "my_instance"
_OTHER_INSTANCE = "other_instance"


def _two_instance_config(
    resolve_all_platforms: bool = False,
) -> AutoResolveLineageUrnsConfig:
    return AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(
                platform="snowflake", platform_instance=_MY_INSTANCE, env="PROD"
            ),
            UpstreamPlatformCasing(
                platform="snowflake", platform_instance=_OTHER_INSTANCE, env="PROD"
            ),
        ],
        resolve_all_platforms=resolve_all_platforms,
    )


def _catalog_not_loaded_warnings(source_report: mock.MagicMock) -> List[Any]:
    return [
        c
        for c in source_report.warning.call_args_list
        if "upstream catalog not loaded" in c.kwargs.get("title", "").lower()
    ]


def test_a_failed_catalog_load_still_gets_column_casing_when_scope_is_widened():
    # Nothing was read, so nothing about this slice is known locally. With scope widened
    # the reference's identity is asked for and healed — and its columns have to be
    # fetched the same way. Reading the config as coverage would answer "DataHub holds no
    # columns for this" without ever having looked.
    processor, graph, patcher = _processor_for(
        _widened(),
        _resolver({}),
        [LOWER],
        [],  # nothing loaded, so every reference is asked
        provide=mock.MagicMock(side_effect=RuntimeError("boom")),
    )
    graph.get_dataset_urns_ignoring_case.return_value = [LOWER]
    graph.get_aspect.return_value = _schema_metadata("snowflake", ["amount"])
    try:
        [out] = list(
            processor.process(iter([_upstream_wu(UPPER, fine_grained_field="AMOUNT")]))
        )
    finally:
        patcher.stop()

    assert _fine_grained(out).upstreams == [make_schema_field_urn(LOWER, "amount")]
    assert (
        len(
            _catalog_not_loaded_warnings(
                cast(mock.MagicMock, processor.ctx.source_report)
            )
        )
        == 1
    )


def test_one_failed_instance_does_not_disable_the_one_that_loaded():
    # Each configured entry is its own scroll. A platform configured for two instances
    # whose second load fails must keep healing references to the first, rather than
    # writing off the platform and discarding a catalog it already paid for.
    stored = _instanced("db.schema.table", _MY_INSTANCE)
    referenced = _instanced("DB.SCHEMA.TABLE", _MY_INSTANCE)
    loaded_slice: _Region = ("snowflake", _MY_INSTANCE, "PROD")
    provide = mock.MagicMock(
        side_effect=[_resolver({stored: {"amount": "int"}}), RuntimeError("boom")]
    )

    processor, graph, patcher = _processor_for(
        _two_instance_config(), _resolver({}), [stored], [loaded_slice], provide=provide
    )
    try:
        [out] = list(
            processor.process(
                iter([_upstream_wu(referenced, fine_grained_field="AMOUNT")])
            )
        )
    finally:
        patcher.stop()

    assert _fine_grained(out).upstreams == [make_schema_field_urn(stored, "amount")]
    assert processor.report.num_column_urns_normalized == 1
    # The instance that loaded answers locally; only the failed one is reported.
    graph.get_dataset_urns_ignoring_case.assert_not_called()
    assert (
        len(
            _catalog_not_loaded_warnings(
                cast(mock.MagicMock, processor.ctx.source_report)
            )
        )
        == 1
    )


def test_a_reference_into_the_failed_instance_is_left_as_emitted():
    # This instance's own load failed, so nothing local can answer and the reference is
    # asked of DataHub, which does not have it either.
    referenced = _instanced("DB.SCHEMA.TABLE", _OTHER_INSTANCE)
    loaded_slice: _Region = ("snowflake", _MY_INSTANCE, "PROD")
    provide = mock.MagicMock(side_effect=[_resolver({}), RuntimeError("boom")])

    processor, _graph, patcher = _processor_for(
        _two_instance_config(), _resolver({}), [], [loaded_slice], provide=provide
    )
    try:
        [out] = list(processor.process(iter([_upstream_wu(referenced)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == referenced  # left exactly as emitted
    assert _upstream_aspect(out).upstreams[0].matchType == (
        LineageMatchTypeClass.UNRESOLVED
    )
    assert processor.report.num_refs_out_of_scope == 0
    assert _catalog_not_loaded_warnings(
        cast(mock.MagicMock, processor.ctx.source_report)
    )


def test_a_sibling_instance_does_not_answer_for_the_one_referenced():
    # A preload narrowed to one platform_instance holds nothing for a sibling, so a
    # reference into the sibling must be asked about rather than called broken.
    stored = _instanced("db.schema.table", _OTHER_INSTANCE)
    referenced = _instanced("DB.SCHEMA.TABLE", _OTHER_INSTANCE)
    preloaded = _instanced("db.schema.table", _MY_INSTANCE)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(
                platform="snowflake", platform_instance=_MY_INSTANCE, env="PROD"
            )
        ],
        resolve_all_platforms=True,
    )
    processor, graph, patcher = _processor_for(
        cfg, _resolver({}), [preloaded, stored], [("snowflake", _MY_INSTANCE, "PROD")]
    )
    graph.get_aspect.return_value = None
    try:
        [out] = list(processor.process(iter([_upstream_wu(referenced)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == stored
    assert _upstream_aspect(out).upstreams[0].matchType == (
        LineageMatchTypeClass.NORMALIZED
    )
    assert processor.report.num_refs_unresolved == 0
    # Asked under the reference's own key, not answered from the preloaded sibling.
    graph.get_dataset_urns_ignoring_case.assert_called_once()


def test_a_platform_wide_preload_answers_for_every_instance_locally():
    # The same reference, when the slice that was read does contain it: no instance filter
    # narrowed the scroll, so it enumerated every instance and a miss would be a fact.
    stored = _instanced("db.schema.table", _OTHER_INSTANCE)
    referenced = _instanced("DB.SCHEMA.TABLE", _OTHER_INSTANCE)

    processor, graph, patcher = _processor_for(
        _widened(), _resolver({stored: {"amount": "int"}}), [stored], [_SNOWFLAKE_SLICE]
    )
    try:
        [out] = list(processor.process(iter([_upstream_wu(referenced)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == stored
    graph.get_dataset_urns_ignoring_case.assert_not_called()


def test_columns_are_fetched_when_an_instance_filtered_schema_fetch_came_back_empty():
    # The fetch succeeds but returns nothing, its instance filter matching an aspect the
    # connector never emitted. That does not establish that the slice has no schemas, so
    # columns must be fetched per URN — otherwise the table heals and the columns do not.
    stored = _instanced("db.schema.table", _MY_INSTANCE)
    referenced = _instanced("DB.SCHEMA.TABLE", _MY_INSTANCE)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(
                platform="snowflake", platform_instance=_MY_INSTANCE, env="PROD"
            )
        ],
    )
    processor, graph, patcher = _processor_for(
        cfg,
        _resolver({}),  # loaded, but empty
        [stored],
        [("snowflake", _MY_INSTANCE, "PROD")],
    )
    graph.get_aspect.return_value = _schema_metadata("snowflake", ["amount"])
    try:
        [out] = list(
            processor.process(
                iter([_upstream_wu(referenced, fine_grained_field="AMOUNT")])
            )
        )
    finally:
        patcher.stop()

    assert _fine_grained(out).upstreams == [make_schema_field_urn(stored, "amount")]
    assert _schema_fetches(graph) == 1


def test_columns_are_fetched_for_a_slice_whose_schemas_were_never_loaded():
    # Identity and columns are recorded apart, so a slice whose alias scroll finished while
    # its schema load failed keeps its healed identity and fetches columns per URN. A
    # platform-wide "loaded" flag would answer "no columns" without looking.
    stored = _instanced("db.schema.table", _MY_INSTANCE)
    referenced = _instanced("DB.SCHEMA.TABLE", _MY_INSTANCE)

    cfg = AutoResolveLineageUrnsConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(
                platform="snowflake", platform_instance=_MY_INSTANCE, env="PROD"
            )
        ],
    )
    processor, graph, patcher = _processor_for(
        cfg,
        _resolver({}),
        [stored],
        [("snowflake", _MY_INSTANCE, "PROD")],
        provide=mock.MagicMock(side_effect=RuntimeError("boom")),
    )
    graph.get_aspect.return_value = _schema_metadata("snowflake", ["amount"])
    try:
        [out] = list(
            processor.process(
                iter([_upstream_wu(referenced, fine_grained_field="AMOUNT")])
            )
        )
    finally:
        patcher.stop()

    # Identity healed from the completed alias scroll; the column from the fetched schema.
    assert _fine_grained(out).upstreams == [make_schema_field_urn(stored, "amount")]
    graph.get_dataset_urns_ignoring_case.assert_not_called()
    assert _schema_fetches(graph) == 1


# --- what checking a column actually established -------------------------------------
#
# match_columns_to_schema echoes an unmatched column straight back, so an unchanged field
# path alone cannot tell "already correct" from "not in the schema" from "never checked".


def test_the_four_outcomes_are_counted_apart():
    # Exact, out-of-scope platform, absent and malformed each mean something different to
    # an operator, so none of them may share a bucket.
    dev_ref = make_dataset_urn("snowflake", "DB.SCHEMA.TABLE", env="DEV")
    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPPER, type="TRANSFORMED"),  # exists as emitted
            UpstreamClass(dataset=BQ_UPPER, type="TRANSFORMED"),  # platform unlisted
            UpstreamClass(dataset=dev_ref, type="TRANSFORMED"),  # checked, absent
        ],
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
    processor, _provide, patcher = _make_processor({UPPER: {"amount": "int"}})
    try:
        list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    assert processor.report.num_refs_verified_exact == 1
    assert processor.report.num_refs_out_of_scope == 1  # bigquery: platform not listed
    assert processor.report.num_refs_unresolved == 1  # snowflake DEV: asked, not there
    assert processor.report.num_refs_skipped_malformed == 1


# --- the sample behind the unresolved count --------------------------------------------


def test_the_unresolved_sample_keeps_one_entry_per_broken_table():
    # A table with many fine-grained columns used to crowd every other broken table out
    # of the sample, so the operator saw one URN and could not tell which else was broken.
    columns = [f"COL{i}" for i in range(20)]
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPPER, type="TRANSFORMED")],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[make_schema_field_urn(UPPER, column) for column in columns],
                downstreams=[make_schema_field_urn(DOWNSTREAM, "amount")],
            )
        ],
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DOWNSTREAM, aspect=aspect
    ).as_workunit()
    processor, _provide, patcher = _make_processor({})  # nothing to match against
    try:
        list(processor.process(iter([wu])))
    finally:
        patcher.stop()

    # One entry for the table, but each column is still its own broken reference.
    assert sorted(processor.report.unresolved_refs_sample) == [UPPER]
    assert processor.report.num_refs_unresolved == len(columns) + 1


# --- a lookup that failed is not a verdict ---------------------------------------------


def test_a_failed_lookup_leaves_the_reference_unstamped():
    # A failed search establishes nothing, so the reference is treated like anything else
    # that was never checked rather than stamped UNRESOLVED.
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [], [_SNOWFLAKE_SLICE]
    )
    graph.get_dataset_urns_ignoring_case.side_effect = Exception("boom")
    try:
        [out] = list(processor.process(iter([_upstream_wu(BQ_UPPER)])))
    finally:
        patcher.stop()

    assert _stored_upstream(out) == BQ_UPPER
    assert _upstream_aspect(out).upstreams[0].matchType is None
    assert processor.report.num_refs_lookup_failed == 1
    assert processor.report.num_refs_unresolved == 0
    assert not processor.report.unresolved_refs_sample


def test_a_failed_lookup_is_reported_apart_from_broken_lineage():
    # "We could not tell" must not be reported as "this lineage looks broken".
    processor, graph, patcher = _processor_for(
        _widened(), _resolver({}), [], [_SNOWFLAKE_SLICE]
    )
    graph.get_dataset_urns_ignoring_case.side_effect = Exception("boom")
    try:
        list(processor.process(iter([_upstream_wu(BQ_UPPER)])))
    finally:
        patcher.stop()

    titles = [
        call.kwargs.get("title", "")
        for call in cast(
            mock.MagicMock, processor.ctx.source_report
        ).warning.call_args_list
    ]
    assert "Lineage URN casing not checked" in titles
    assert not any("not resolved to an existing entity" in title for title in titles)
