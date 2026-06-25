from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline_config import (
    NormalizeLineageUrnCasingConfig,
    UpstreamPlatformCasing,
)
from datahub.ingestion.workunit_processors.auto_normalize_lineage_urns import (
    AutoNormalizeLineageUrnsProcessor,
)
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    DashboardInfoClass,
    DataJobInputOutputClass,
    DatasetSnapshotClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    LineageMatchTypeClass,
    MetadataChangeEventClass,
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
    all_urns: Optional[List[str]] = None,
) -> Tuple[AutoNormalizeLineageUrnsProcessor, mock.MagicMock, Any]:
    """Patch provide_schema_resolver to a single seeded resolver; configure snowflake.

    `schemas` maps existing URN -> column schema (what the resolver caches). The
    complete URN set seen by exact-match (graph.get_urns_by_filter) defaults to the
    schema keys, but `all_urns` can add schemaless entities that exist without a schema.
    """
    resolver = _resolver(schemas)
    provide_mock = mock.MagicMock(return_value=resolver)

    cfg = NormalizeLineageUrnCasingConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.return_value = list(
        schemas if all_urns is None else all_urns
    )
    pipeline_ctx.flags.normalize_lineage_urn_casing = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx

    processor = AutoNormalizeLineageUrnsProcessor.create(ctx)
    patcher = mock.patch(_PATCH_TARGET, provide_mock)
    patcher.start()
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
    all_urns: Optional[List[str]] = None,
) -> MetadataWorkUnit:
    processor, _provide, patcher = _make_processor(schemas, all_urns=all_urns)
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


def test_heals_uppercase_emit_to_existing_lowercase():
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(UPPER))
    assert _stored_upstream(out) == LOWER


def test_heals_lowercase_emit_to_existing_uppercase():
    # Doc headline: real Snowflake entity is uppercase, BI emits lowercase.
    out = _run({UPPER: {"amount": "int"}}, _upstream_wu(LOWER))
    assert _stored_upstream(out) == UPPER


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
    # Upstream is bigquery, but only snowflake is configured -> no resolution.
    bq = make_dataset_urn("bigquery", "PROJ.DS.T")
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(bq))
    assert _stored_upstream(out) == bq


# --- mixed-casing identifiers (e.g. `DataHub` vs `datahub`) ------------------------


def test_heals_lowercase_emit_to_existing_mixedcase():
    # Warehouse stores `DataHub` (mixed); BI emits `datahub` (lower) -> heal to `DataHub`.
    out = _run({WH_MIXED: {"amount": "int"}}, _upstream_wu(WH_LOWER))
    assert _stored_upstream(out) == WH_MIXED


def test_heals_mixedcase_emit_to_existing_lowercase():
    # The other way round: warehouse stores `datahub`; BI emits `DataHub` -> heal to `datahub`.
    out = _run({WH_LOWER: {"amount": "int"}}, _upstream_wu(WH_MIXED))
    assert _stored_upstream(out) == WH_LOWER


def test_heals_uppercase_emit_to_existing_mixedcase():
    # Warehouse stores `DataHub` (mixed); BI emits `DATAHUB` (upper) -> heal to `DataHub`.
    out = _run({WH_MIXED: {"amount": "int"}}, _upstream_wu(WH_UPPER))
    assert _stored_upstream(out) == WH_MIXED


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


def test_schemaless_exact_entity_is_not_rewritten():
    # The exact entity exists but has NO schema (absent from the schema cache); a
    # case-variant exists WITH a schema. Exact match must still win via the complete
    # URN set, so the reference is never redirected to the differently-cased variant.
    out = _run(
        {LOWER: {"amount": "int"}},  # only the lowercase variant has a schema
        _upstream_wu(UPPER),  # BI references the (schemaless) exact UPPER entity
        all_urns=[LOWER, UPPER],  # both exist in the warehouse; UPPER is schemaless
    )
    assert _stored_upstream(out) == UPPER
    assert _upstream_aspect(out).upstreams[0].matchType == LineageMatchTypeClass.EXACT


def test_mixedcase_ambiguous_third_casing_left_unchanged():
    # Both `DataHub` and `datahub` exist; BI emits a third casing `DATAHUB` that matches
    # neither exactly -> ambiguous (two share the lowercase form) -> leave unchanged.
    out = _run(
        {WH_MIXED: {"amount": "int"}, WH_LOWER: {"amount": "int"}},
        _upstream_wu(WH_UPPER),
    )
    assert _stored_upstream(out) == WH_UPPER
    assert _upstream_aspect(out).upstreams[0].matchType is None


# --- match type discriminator -----------------------------------------------------


def test_match_type_normalized_when_rewritten():
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(UPPER))
    upstream = _upstream_aspect(out).upstreams[0]
    assert upstream.matchType == LineageMatchTypeClass.NORMALIZED


def test_match_type_exact_when_exact_match():
    out = _run({UPPER: {"amount": "int"}}, _upstream_wu(UPPER))
    upstream = _upstream_aspect(out).upstreams[0]
    assert upstream.matchType == LineageMatchTypeClass.EXACT


def test_match_type_unset_when_no_match():
    out = _run({}, _upstream_wu(UPPER))
    assert _upstream_aspect(out).upstreams[0].matchType is None


def test_fine_grained_match_type_normalized():
    out = _run(
        {LOWER: {"amount": "int"}}, _upstream_wu(UPPER, fine_grained_field="AMOUNT")
    )
    fg = _fine_grained(out)
    assert fg.matchType == LineageMatchTypeClass.NORMALIZED


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

    cfg = NormalizeLineageUrnCasingConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="mssql", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.return_value = [mssql_table]
    pipeline_ctx.flags.normalize_lineage_urn_casing = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    processor = AutoNormalizeLineageUrnsProcessor.create(ctx)

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

    def fake_urns(entity_types, platform, platform_instance=None, env=None, **kwargs):
        return [sf_real] if platform == "snowflake" else [rs_real]

    cfg = NormalizeLineageUrnCasingConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(platform="snowflake", env="PROD"),
            UpstreamPlatformCasing(platform="redshift", env="PROD"),
        ],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.side_effect = fake_urns
    pipeline_ctx.flags.normalize_lineage_urn_casing = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    processor = AutoNormalizeLineageUrnsProcessor.create(ctx)

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
        [out] = list(processor.process(iter([wu])))

    healed = {u.dataset for u in _upstream_aspect(out).upstreams}
    assert sf_real in healed  # snowflake lower -> mixed
    assert rs_real in healed  # redshift upper -> mixed


def test_platform_urn_form_in_config_is_normalized():
    # Config may specify the platform as a full URN; it must still match the
    # normalized platform parsed from the dataset URN (else: silent no-op).
    resolver = _resolver({LOWER: {"amount": "int"}})
    provide_mock = mock.MagicMock(return_value=resolver)
    cfg = NormalizeLineageUrnCasingConfig(
        enabled=True,
        upstream_platforms=[
            UpstreamPlatformCasing(platform="urn:li:dataPlatform:snowflake", env="PROD")
        ],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.graph.get_urns_by_filter.return_value = [LOWER]
    pipeline_ctx.flags.normalize_lineage_urn_casing = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    processor = AutoNormalizeLineageUrnsProcessor.create(ctx)

    with mock.patch(_PATCH_TARGET, provide_mock):
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))
    assert _stored_upstream(out) == LOWER


def test_platform_instance_is_threaded_through_and_heals():
    # When an upstream platform is configured with a platform_instance, that instance
    # must be passed to both the resolver and the URN-membership query, and an
    # instance-qualified reference must heal against the stored instance-qualified URN.
    stored = make_dataset_urn_with_platform_instance(
        "snowflake", "DB.SCHEMA.TABLE", "my_instance", "PROD"
    )
    referenced = make_dataset_urn_with_platform_instance(
        "snowflake", "db.schema.table", "my_instance", "PROD"
    )
    resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    resolver.add_raw_schema_info(stored, {"amount": "int"})
    provide_mock = mock.MagicMock(return_value=resolver)

    cfg = NormalizeLineageUrnCasingConfig(
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
    pipeline_ctx.flags.normalize_lineage_urn_casing = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    processor = AutoNormalizeLineageUrnsProcessor.create(ctx)

    with mock.patch(_PATCH_TARGET, provide_mock):
        [out] = list(processor.process(iter([_upstream_wu(referenced)])))

    assert _stored_upstream(out) == stored
    assert provide_mock.call_args.kwargs["platform_instance"] == "my_instance"
    assert (
        pipeline_ctx.graph.get_urns_by_filter.call_args.kwargs["platform_instance"]
        == "my_instance"
    )


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


def test_non_dataset_upstream_ref_is_skipped():
    # A non-dataset upstream URN (e.g. a dataJob) is ignored, not resolved.
    datajob = "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,prod),task)"
    out = _run({LOWER: {"amount": "int"}}, _upstream_wu(datajob))
    assert _stored_upstream(out) == datajob


def test_exception_is_recorded_and_workunit_passed_through():
    # If resolution raises, the workunit is passed through unchanged and counted.
    provide_mock = mock.MagicMock(side_effect=RuntimeError("boom"))
    cfg = NormalizeLineageUrnCasingConfig(
        enabled=True,
        upstream_platforms=[UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = mock.MagicMock()
    pipeline_ctx.flags.normalize_lineage_urn_casing = cfg
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    processor = AutoNormalizeLineageUrnsProcessor.create(ctx)

    with mock.patch(_PATCH_TARGET, provide_mock):
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))

    assert _stored_upstream(out) == UPPER  # unchanged
    assert processor.report.num_exceptions == 1


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
    pipeline_ctx.flags.normalize_lineage_urn_casing = NormalizeLineageUrnCasingConfig(
        enabled=enabled,
        upstream_platforms=upstream_platforms
        if upstream_platforms is not None
        else [UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    ctx = mock.MagicMock()
    ctx.pipeline_context = pipeline_ctx
    return ctx


def test_disabled_without_graph():
    assert AutoNormalizeLineageUrnsProcessor.should_enable(_ctx(True, None)) is False


def test_disabled_when_flag_off():
    assert (
        AutoNormalizeLineageUrnsProcessor.should_enable(_ctx(False, mock.MagicMock()))
        is False
    )


def test_disabled_when_no_upstream_platforms():
    # Enabled but unconfigured = active no-op; should_enable guards against it.
    assert (
        AutoNormalizeLineageUrnsProcessor.should_enable(
            _ctx(True, mock.MagicMock(), upstream_platforms=[])
        )
        is False
    )


def test_enabled_when_flag_on_with_graph():
    assert (
        AutoNormalizeLineageUrnsProcessor.should_enable(_ctx(True, mock.MagicMock()))
        is True
    )
