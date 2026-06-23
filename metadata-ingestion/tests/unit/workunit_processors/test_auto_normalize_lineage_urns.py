from typing import Dict, Optional
from unittest import mock

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
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
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
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

_PATCH_TARGET = (
    "datahub.ingestion.workunit_processors."
    "auto_normalize_lineage_urns.provide_schema_resolver"
)


def _resolver(schemas: Dict[str, Dict[str, str]]) -> SchemaResolver:
    """A graph-less resolver pre-populated with {urn: {column: type}}."""
    resolver = SchemaResolver(platform="snowflake", env="PROD", graph=None)
    for urn, schema in schemas.items():
        resolver.add_raw_schema_info(urn, schema)
    return resolver


def _make_processor(schemas: Dict[str, Dict[str, str]]):
    """Patch provide_schema_resolver to a single seeded resolver; configure snowflake."""
    resolver = _resolver(schemas)
    provide_mock = mock.MagicMock(return_value=resolver)

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


def _run(schemas: Dict[str, Dict[str, str]], wu: MetadataWorkUnit) -> MetadataWorkUnit:
    processor, _provide, patcher = _make_processor(schemas)
    try:
        [out] = list(processor.process(iter([wu])))
        return out
    finally:
        patcher.stop()


def _stored_upstream(wu: MetadataWorkUnit) -> str:
    return wu.get_aspect_of_type(UpstreamLineageClass).upstreams[0].dataset


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


# --- column-level (fine-grained) casing -------------------------------------------


def test_fine_grained_fixes_dataset_and_column_casing():
    # Existing entity is lowercase table with lowercase column "amount";
    # BI tool emitted uppercase table + uppercase column "AMOUNT".
    out = _run(
        {LOWER: {"amount": "int"}}, _upstream_wu(UPPER, fine_grained_field="AMOUNT")
    )
    fg = out.get_aspect_of_type(UpstreamLineageClass).fineGrainedLineages[0]
    assert fg.upstreams == [make_schema_field_urn(LOWER, "amount")]
    # Downstream field belongs to the entity itself and must never be touched.
    assert fg.downstreams == [make_schema_field_urn(DOWNSTREAM, "amount")]


def test_fine_grained_fixes_column_casing_even_when_dataset_exact():
    # Dataset casing already correct, but column casing is wrong.
    out = _run(
        {UPPER: {"amount": "int"}}, _upstream_wu(UPPER, fine_grained_field="AMOUNT")
    )
    fg = out.get_aspect_of_type(UpstreamLineageClass).fineGrainedLineages[0]
    assert fg.upstreams == [make_schema_field_urn(UPPER, "amount")]


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
        assert out.get_aspect_of_type(DashboardInfoClass).datasets == [LOWER]
    finally:
        patcher.stop()


# --- safety / enablement ----------------------------------------------------------


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


def _ctx(enabled: bool, graph: object):
    pipeline_ctx = mock.MagicMock()
    pipeline_ctx.graph = graph
    pipeline_ctx.flags.normalize_lineage_urn_casing = NormalizeLineageUrnCasingConfig(
        enabled=enabled
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


def test_enabled_when_flag_on_with_graph():
    assert (
        AutoNormalizeLineageUrnsProcessor.should_enable(_ctx(True, mock.MagicMock()))
        is True
    )
