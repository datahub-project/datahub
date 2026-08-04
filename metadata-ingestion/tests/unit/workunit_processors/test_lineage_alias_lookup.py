import inspect
from typing import Dict, List, Optional, Set, Type
from unittest import mock

import pytest

from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.entity_aspect_specs import EntityAspectSpecs
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.ingestion.run.pipeline_config import (
    AutoResolveLineageUrnsConfig,
    UpstreamPlatformCasing,
)
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns import (
    AutoResolveLineageUrnsProcessor,
)
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.alias_lookup import (
    AliasLookupStrategy,
)
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.models import (
    EXACT,
    NORMALIZED,
    UNRESOLVED,
)
from datahub.metadata.schema_classes import (
    AliasesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

# Snowflake convention: uppercase. BI-tool convention: lowercase.
UPPER = make_dataset_urn("snowflake", "DB.SCHEMA.TABLE")
LOWER = make_dataset_urn("snowflake", "db.schema.table")
MIXED = make_dataset_urn("snowflake", "Db.Schema.Table")

# The fine-grained path passes whatever parent it finds, including this.
NOT_A_DATASET = "urn:li:dashboard:(looker,dashboards.1)"


def _specs(*, supports_aliases: bool) -> EntityAspectSpecs:
    aspects = {"status", "schemaMetadata"}
    if supports_aliases:
        aspects.add(AliasesClass.ASPECT_NAME)
    return EntityAspectSpecs(entity_aspects={"dataset": aspects})


def _requested_keys(extra_or_filters: object) -> Set[str]:
    """The keys a lookup asked for, read out of the raw filter the strategy built."""
    assert isinstance(extra_or_filters, list) and len(extra_or_filters) == 1
    rules = extra_or_filters[0]["and"]
    assert len(rules) == 1
    rule = rules[0]
    # Pins the field name the strategy hardcodes against the aspect schema, by name
    # rather than position so adding a field to Aliases.pdl doesn't break every test.
    assert rule["field"] in {f.name for f in AliasesClass.RECORD_SCHEMA.fields}
    assert rule["condition"] == "EQUAL"
    return set(rule["values"])


def _schema_metadata(columns: List[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=mock.MagicMock(),
        fields=[
            SchemaFieldClass(
                fieldPath=column,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
            for column in columns
        ],
    )


class FakeGraph:
    """Answers lookups from `stored` by real lowercasing, as the server's index would."""

    def __init__(
        self,
        stored: List[str],
        *,
        supports_aliases: bool = True,
        schemas: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        self._stored = stored
        self._supports_aliases = supports_aliases
        self._schemas = schemas or {}
        self.lookup_calls: List[Set[str]] = []
        self.lookup_status: List[object] = []
        self.schema_fetch_calls: List[str] = []

    def get_entity_aspect_specs(self) -> EntityAspectSpecs:
        return _specs(supports_aliases=self._supports_aliases)

    def get_urns_by_filter(
        self, *, entity_types: List[str], extra_or_filters: object, **kwargs: object
    ) -> List[str]:
        assert entity_types == ["dataset"]
        keys = _requested_keys(extra_or_filters)
        self.lookup_calls.append(keys)
        self.lookup_status.append(kwargs.get("status"))
        return [urn for urn in self._stored if lowercase_dataset_urn(urn) in keys]

    def get_aspect(
        self, entity_urn: str, aspect_type: Type[_Aspect], version: int = 0
    ) -> Optional[SchemaMetadataClass]:
        assert aspect_type is SchemaMetadataClass
        self.schema_fetch_calls.append(entity_urn)
        columns = self._schemas.get(entity_urn)
        return _schema_metadata(columns) if columns is not None else None


def _ctx_for(graph: FakeGraph) -> mock.MagicMock:
    ctx = mock.MagicMock()
    ctx.pipeline_context.graph = graph
    return ctx


def _strategy(
    graph: FakeGraph, ctx: Optional[mock.MagicMock] = None
) -> AliasLookupStrategy:
    return AliasLookupStrategy(ctx if ctx is not None else _ctx_for(graph))


def test_fake_graph_stays_compatible_with_the_real_client() -> None:
    # FakeGraph reaches the strategy through a MagicMock ctx, so nothing else would
    # notice a renamed parameter on DataHubGraph: these tests would stay green while
    # production broke on the real call.
    for name in ("get_urns_by_filter", "get_aspect"):
        fake = set(inspect.signature(getattr(FakeGraph, name)).parameters) - {"kwargs"}
        real = set(inspect.signature(getattr(DataHubGraph, name)).parameters)
        assert fake <= real, f"FakeGraph.{name} has parameters DataHubGraph lacks"


# --- capability gate ---------------------------------------------------------------


def test_raises_when_server_does_not_register_aliases() -> None:
    # Continuing would look like "nothing matched" and emit unhealed lineage as healed.
    with pytest.raises(ValueError):
        _strategy(FakeGraph([LOWER], supports_aliases=False))


def test_raises_when_registry_api_unavailable() -> None:
    graph = FakeGraph([LOWER])
    graph.get_entity_aspect_specs = mock.MagicMock(return_value=None)  # type: ignore[method-assign]
    with pytest.raises(ValueError):
        _strategy(graph)


def test_raises_when_dataset_entity_not_registered() -> None:
    graph = FakeGraph([LOWER])
    graph.get_entity_aspect_specs = mock.MagicMock(  # type: ignore[method-assign]
        return_value=EntityAspectSpecs(entity_aspects={"chart": {"status"}})
    )
    with pytest.raises(ValueError):
        _strategy(graph)


# --- verdicts ---------------------------------------------------------------------


@pytest.mark.parametrize("reference", [UPPER, MIXED, LOWER])
def test_resolves_any_casing_to_the_stored_urn(reference: str) -> None:
    # Where this beats the bulk path, which only tries the original and lowercased forms.
    assert _strategy(FakeGraph([LOWER])).resolve(reference).urn == LOWER


def test_reference_stored_verbatim_is_exact() -> None:
    resolution = _strategy(FakeGraph([UPPER])).resolve(UPPER)
    assert resolution.match_type == EXACT
    assert resolution.urn == UPPER


def test_reference_healed_to_other_casing_is_normalized() -> None:
    assert _strategy(FakeGraph([LOWER])).resolve(UPPER).match_type == NORMALIZED


def test_reference_with_no_stored_entity_is_unresolved() -> None:
    resolution = _strategy(FakeGraph([])).resolve(UPPER)
    assert resolution.match_type == UNRESOLVED
    assert resolution.urn == UPPER


def test_collision_prefers_the_lowercased_variant() -> None:
    # Two live entities differing only by casing are what flipping
    # convert_urns_to_lowercase leaves behind, so the lowercased one is the better guess.
    resolution = _strategy(FakeGraph([LOWER, UPPER])).resolve(MIXED)
    assert resolution.match_type == NORMALIZED
    assert resolution.urn == LOWER


def test_soft_deleted_duplicates_cannot_be_candidates() -> None:
    # Preferring the lowercased variant is only defensible because a duplicate someone
    # has already retired is never offered: we pass no status override, and the client's
    # default excludes soft-deleted entities. If that default ever changes, the
    # preference silently starts resolving lineage onto deleted tables.
    graph = FakeGraph([LOWER])
    _strategy(graph).resolve(UPPER)

    assert graph.lookup_status == [None]
    status = inspect.signature(DataHubGraph.get_urns_by_filter).parameters["status"]
    assert status.default is RemovedStatusFilter.NOT_SOFT_DELETED


def test_collision_without_a_lowercased_variant_is_left_alone() -> None:
    # Neither the reference nor the lowercased form exists, so there is nothing to
    # prefer; rewriting to either candidate would merge two distinct tables.
    resolution = _strategy(FakeGraph([UPPER, MIXED])).resolve(LOWER)
    assert resolution.match_type == UNRESOLVED
    assert resolution.urn == LOWER


def test_collision_containing_the_reference_is_exact() -> None:
    # The reference itself wins over the lowercased variant: it demonstrably exists, so
    # there is no casing mismatch to heal even though a duplicate sits next to it.
    resolution = _strategy(FakeGraph([LOWER, UPPER])).resolve(UPPER)
    assert resolution.match_type == EXACT
    assert resolution.urn == UPPER


def test_non_dataset_reference_is_out_of_scope() -> None:
    graph = FakeGraph([LOWER])
    resolution = _strategy(graph).resolve(NOT_A_DATASET)
    assert resolution.match_type is None
    assert resolution.urn == NOT_A_DATASET
    assert graph.lookup_calls == []


# --- schemas ----------------------------------------------------------------------


def test_schema_is_not_fetched_for_a_table_level_reference() -> None:
    graph = FakeGraph([LOWER], schemas={LOWER: ["amount"]})
    assert _strategy(graph).resolve(UPPER).schema is None
    assert graph.schema_fetch_calls == []


def test_schema_is_fetched_under_the_resolved_urn() -> None:
    graph = FakeGraph([LOWER], schemas={LOWER: ["amount"]})
    resolution = _strategy(graph).resolve(UPPER, need_schema=True)
    assert resolution.schema == {"amount": "string"}
    assert graph.schema_fetch_calls == [LOWER]


def test_schema_is_not_fetched_for_an_unresolved_reference() -> None:
    graph = FakeGraph([])
    assert _strategy(graph).resolve(UPPER, need_schema=True).schema is None
    assert graph.schema_fetch_calls == []


def test_resolved_entity_without_a_schema_still_resolves() -> None:
    # Existence is the signal here, unlike the bulk path which needs a schema to match.
    resolution = _strategy(FakeGraph([LOWER], schemas={})).resolve(
        UPPER, need_schema=True
    )
    assert resolution.match_type == NORMALIZED
    assert resolution.schema is None


# --- collision reporting ----------------------------------------------------------


def test_collision_is_reported_separately_from_a_plain_miss() -> None:
    # A miss and an undecidable collision both come out UNRESOLVED but need opposite
    # fixes: ingest the upstream, versus delete one of two duplicates. Only the
    # collision warns here.
    graph = FakeGraph([UPPER, MIXED])
    ctx = _ctx_for(graph)
    strategy = _strategy(graph, ctx)

    strategy.resolve(LOWER)  # collides, and no lowercased variant to prefer
    strategy.resolve(make_dataset_urn("snowflake", "db.schema.absent"))  # plain miss
    strategy.finish()

    warning = ctx.source_report.warning
    assert warning.call_count == 1
    assert "1 reference(s)" in warning.call_args.kwargs["context"]


def test_collision_report_counts_every_reference() -> None:
    # Two references collide on one key, and the count is references. Neither stored
    # entity is the lowercased form, so both references stay undecidable.
    stored = [
        make_dataset_urn("snowflake", "db.schema.AB"),
        make_dataset_urn("snowflake", "db.schema.Ab"),
    ]
    graph = FakeGraph(stored)
    ctx = _ctx_for(graph)
    strategy = _strategy(graph, ctx)

    strategy.resolve(make_dataset_urn("snowflake", "db.schema.aB"))
    strategy.resolve(make_dataset_urn("snowflake", "db.schema.ab"))
    strategy.finish()

    assert "2 reference(s)" in ctx.source_report.warning.call_args.kwargs["context"]


def test_nothing_reported_when_no_collisions() -> None:
    graph = FakeGraph([LOWER])
    ctx = _ctx_for(graph)
    strategy = _strategy(graph, ctx)
    strategy.resolve(UPPER)
    strategy.finish()

    ctx.source_report.warning.assert_not_called()


# --- mode switch ------------------------------------------------------------------


def _processor(
    graph: FakeGraph, mode: str, ctx: Optional[mock.MagicMock] = None
) -> AutoResolveLineageUrnsProcessor:
    ctx = ctx if ctx is not None else _ctx_for(graph)
    ctx.pipeline_context.flags.auto_resolve_lineage_urns = AutoResolveLineageUrnsConfig(
        enabled=True,
        mode=mode,
        upstream_platforms=[UpstreamPlatformCasing(platform="snowflake", env="PROD")],
    )
    return AutoResolveLineageUrnsProcessor.create(ctx)


def _upstream_wu(upstream_urn: str) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=make_dataset_urn("looker", "explore.orders"),
        aspect=UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=upstream_urn, type="TRANSFORMED")]
        ),
    ).as_workunit()


def test_alias_lookup_mode_heals_without_downloading_a_catalog() -> None:
    graph = FakeGraph([LOWER])
    with mock.patch(
        "datahub.sql_parsing.schema_resolver_provider.provide_schema_resolver"
    ) as provide:
        processor = _processor(graph, "alias_lookup")
        [out] = list(processor.process(iter([_upstream_wu(UPPER)])))

    aspect = out.get_aspect_of_type(UpstreamLineageClass)
    assert aspect is not None
    assert aspect.upstreams[0].dataset == LOWER
    provide.assert_not_called()
    assert graph.lookup_calls == [{lowercase_dataset_urn(UPPER)}]


def test_a_failed_lookup_emits_the_lineage_unchanged() -> None:
    # An unreachable GMS must not drop lineage or halt the run: the reference passes
    # through as the source produced it, counted and surfaced in the source report.
    graph = FakeGraph([LOWER])
    graph.get_urns_by_filter = mock.MagicMock(  # type: ignore[method-assign]
        side_effect=GraphError("gms unreachable")
    )
    ctx = _ctx_for(graph)
    processor = _processor(graph, "alias_lookup", ctx)
    [out] = list(processor.process(iter([_upstream_wu(UPPER)])))

    aspect = out.get_aspect_of_type(UpstreamLineageClass)
    assert aspect is not None
    assert aspect.upstreams[0].dataset == UPPER
    assert processor.report.num_exceptions == 1
    # Carries the exception, so the operator sees the cause and not just a count.
    assert any(
        "exc" in call.kwargs for call in ctx.source_report.warning.call_args_list
    )


def test_column_level_lineage_heals_parent_casing_and_column_casing() -> None:
    # The whole column path end to end: the parent is looked up, its schema fetched
    # under the resolved urn, and the column matched against it case-insensitively.
    graph = FakeGraph([LOWER], schemas={LOWER: ["amount"]})
    upstream_field = make_schema_field_urn(UPPER, "AMOUNT")
    downstream_field = make_schema_field_urn(
        make_dataset_urn("looker", "explore.orders"), "Amount"
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=make_dataset_urn("looker", "explore.orders"),
        aspect=UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=UPPER, type="TRANSFORMED")],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[upstream_field],
                    downstreams=[downstream_field],
                )
            ],
        ),
    ).as_workunit()

    processor = _processor(graph, "alias_lookup")
    [out] = list(processor.process(iter([wu])))

    aspect = out.get_aspect_of_type(UpstreamLineageClass)
    assert aspect is not None
    assert aspect.fineGrainedLineages is not None
    fine_grained = aspect.fineGrainedLineages[0]
    assert fine_grained.upstreams == [make_schema_field_urn(LOWER, "amount")]
    # The downstream belongs to the Looker entity itself and keeps its own casing.
    assert fine_grained.downstreams == [downstream_field]
    assert graph.schema_fetch_calls == [LOWER]


def test_a_platform_outside_upstream_platforms_is_still_looked_up() -> None:
    # Documents a divergence from bulk_catalog rather than endorsing it: that mode only
    # heals the configured platforms because it downloads their catalogs, while this one
    # asks the server about every dataset reference. Change the config gate, not this
    # assertion, if alias_lookup should honour upstream_platforms too.
    stored_bigquery = make_dataset_urn("bigquery", "proj.dataset.table")
    graph = FakeGraph([stored_bigquery])
    processor = _processor(graph, "alias_lookup")  # configured for snowflake only

    referenced = make_dataset_urn("bigquery", "PROJ.DATASET.TABLE")
    [out] = list(processor.process(iter([_upstream_wu(referenced)])))

    aspect = out.get_aspect_of_type(UpstreamLineageClass)
    assert aspect is not None
    assert aspect.upstreams[0].dataset == stored_bigquery
    assert len(graph.lookup_calls) == 1


def test_bulk_catalog_mode_still_downloads_a_catalog() -> None:
    graph = FakeGraph([LOWER])
    with mock.patch(
        "datahub.sql_parsing.schema_resolver_provider.provide_schema_resolver"
    ) as provide:
        _processor(graph, "bulk_catalog")

    provide.assert_called()
    assert graph.lookup_calls == []
