import pytest
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.rewrite_upstream_urns import (
    RewriteUpstreamUrns,
    RewriteUpstreamUrnsConfig,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DOWNSTREAM = make_dataset_urn("mssql", "warehouse.dbo.target_table")
UPSTREAM = make_dataset_urn("mssql", "src_lakehouse.gold.bridge_table")


def _transformer(
    platform: str = "fabric",
    rules: list | None = None,
    mode: str = "replace",
) -> RewriteUpstreamUrns:
    config = RewriteUpstreamUrnsConfig.model_validate(
        {"platform": platform, "rules": rules or [], "mode": mode}
    )
    return RewriteUpstreamUrns(config, PipelineContext(run_id="test"))


def test_platform_and_sequential_name_rules():
    transformer = _transformer(
        rules=[
            {"find": "src_lakehouse", "replace": "workspace-id.item-id"},
            {"find": ".gold.", "replace": ".curated."},
        ]
    )
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED")]
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    assert rewritten.upstreams[0].dataset == make_dataset_urn(
        "fabric", "workspace-id.item-id.curated.bridge_table"
    )
    # Later rules see the name produced by earlier rules.
    assert aspect.upstreams[0].dataset == UPSTREAM


def test_second_rule_sees_first_replacement():
    transformer = _transformer(
        rules=[
            {"find": "src_lakehouse", "replace": "staging_lakehouse"},
            {"find": "staging_lakehouse", "replace": "workspace-id.item-id"},
        ]
    )
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED")]
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    assert rewritten.upstreams[0].dataset == make_dataset_urn(
        "fabric", "workspace-id.item-id.gold.bridge_table"
    )


def test_fine_grained_upstreams_are_rewritten_and_downstreams_are_not():
    transformer = _transformer(
        rules=[{"find": "src_lakehouse", "replace": "workspace-id.item-id"}]
    )
    downstream_field = make_schema_field_urn(DOWNSTREAM, "fund_id")
    upstream_field = make_schema_field_urn(UPSTREAM, "fund_id")
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED")],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[upstream_field],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[downstream_field],
            )
        ],
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    fine_grained = rewritten.fineGrainedLineages
    assert fine_grained is not None
    assert fine_grained[0].upstreams == [
        make_schema_field_urn(
            make_dataset_urn("fabric", "workspace-id.item-id.gold.bridge_table"),
            "fund_id",
        )
    ]
    assert fine_grained[0].downstreams == [downstream_field]


def test_unparseable_reference_is_left_unchanged():
    transformer = _transformer()
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset="not-a-urn", type="TRANSFORMED")]
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    assert rewritten.upstreams[0].dataset == "not-a-urn"


def test_missing_aspect_stays_missing():
    transformer = _transformer()
    assert transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", None) is None


def test_empty_find_is_rejected():
    with pytest.raises(ValidationError):
        RewriteUpstreamUrnsConfig.model_validate(
            {"platform": "fabric", "rules": [{"find": "", "replace": "x"}]}
        )


def test_add_mode_keeps_the_original_upstream_and_appends_the_rewritten_one():
    transformer = _transformer(
        mode="add",
        rules=[{"find": "src_lakehouse", "replace": "workspace-id.item-id"}],
    )
    downstream_field = make_schema_field_urn(DOWNSTREAM, "fund_id")
    upstream_field = make_schema_field_urn(UPSTREAM, "fund_id")
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED")],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[upstream_field],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[downstream_field],
            )
        ],
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    amended = make_dataset_urn("fabric", "workspace-id.item-id.gold.bridge_table")
    assert isinstance(rewritten, UpstreamLineageClass)
    assert [upstream.dataset for upstream in rewritten.upstreams] == [UPSTREAM, amended]
    fine_grained = rewritten.fineGrainedLineages
    assert fine_grained is not None
    assert fine_grained[0].upstreams == [
        upstream_field,
        make_schema_field_urn(amended, "fund_id"),
    ]
    assert fine_grained[0].downstreams == [downstream_field]


def test_platform_urn_is_reduced_to_its_name():
    config = RewriteUpstreamUrnsConfig.model_validate(
        {"platform": "urn:li:dataPlatform:fabric"}
    )
    assert config.platform == "fabric"
