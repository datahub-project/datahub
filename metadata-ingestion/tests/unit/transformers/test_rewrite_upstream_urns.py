import json

import pytest
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp_patch_builder import _Patch
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.transformer.rewrite_upstream_urns import (
    RewriteUpstreamUrns,
    RewriteUpstreamUrnsConfig,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DOWNSTREAM = make_dataset_urn("mssql", "warehouse.dbo.target_table")
UPSTREAM = make_dataset_urn("mssql", "src_lakehouse.gold.bridge_table")
OTHER_PLATFORM = make_dataset_urn("postgres", "src_lakehouse.gold.bridge_table")


def _transformer(
    platform_rules: list | None = None, mode: str = "replace"
) -> RewriteUpstreamUrns:
    config = RewriteUpstreamUrnsConfig.model_validate(
        {"platform_rules": platform_rules or [], "mode": mode}
    )
    return RewriteUpstreamUrns(config, PipelineContext(run_id="test"))


def _mssql_rule(id_rules: list | None = None, replace: str = "fabric") -> dict:
    return {
        "find": "mssql",
        "replace": replace,
        "id_rules": id_rules or [],
    }


def test_platform_match_runs_nested_name_rules_in_order():
    transformer = _transformer(
        platform_rules=[
            _mssql_rule(
                [
                    {"find": "src_lakehouse", "replace": "workspace-id.item-id"},
                    {"find": ".gold.", "replace": ".curated."},
                ]
            )
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
    assert aspect.upstreams[0].dataset == UPSTREAM


def test_second_name_rule_sees_first_replacement():
    transformer = _transformer(
        platform_rules=[
            _mssql_rule(
                [
                    {"find": "src_lakehouse", "replace": "staging_lakehouse"},
                    {"find": "staging_lakehouse", "replace": "workspace-id.item-id"},
                ]
            )
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


def test_name_rules_do_not_run_when_the_platform_does_not_match():
    transformer = _transformer(
        platform_rules=[
            _mssql_rule([{"find": "src_lakehouse", "replace": "workspace-id.item-id"}])
        ]
    )
    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED"),
            UpstreamClass(dataset=OTHER_PLATFORM, type="TRANSFORMED"),
        ]
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    assert rewritten.upstreams[0].dataset == make_dataset_urn(
        "fabric", "workspace-id.item-id.gold.bridge_table"
    )
    assert rewritten.upstreams[1].dataset == OTHER_PLATFORM


def test_each_platform_rule_uses_its_own_name_rules():
    transformer = _transformer(
        platform_rules=[
            _mssql_rule([{"find": "src_lakehouse", "replace": "workspace-id.item-id"}]),
            {
                "find": "postgres",
                "replace": "snowflake",
                "id_rules": [{"find": "src_lakehouse", "replace": "pg-warehouse"}],
            },
        ]
    )
    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED"),
            UpstreamClass(dataset=OTHER_PLATFORM, type="TRANSFORMED"),
        ]
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    assert rewritten.upstreams[0].dataset == make_dataset_urn(
        "fabric", "workspace-id.item-id.gold.bridge_table"
    )
    assert rewritten.upstreams[1].dataset == make_dataset_urn(
        "snowflake", "pg-warehouse.gold.bridge_table"
    )


def test_fine_grained_upstreams_are_rewritten_and_downstreams_are_not():
    transformer = _transformer(
        platform_rules=[
            _mssql_rule([{"find": "src_lakehouse", "replace": "workspace-id.item-id"}])
        ]
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
    transformer = _transformer(platform_rules=[_mssql_rule()])
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
            {"platform_rules": [{"find": "", "replace": "fabric", "id_rules": []}]}
        )
    with pytest.raises(ValidationError):
        RewriteUpstreamUrnsConfig.model_validate(
            {
                "platform_rules": [
                    {
                        "find": "mssql",
                        "replace": "fabric",
                        "id_rules": [{"find": "", "replace": "x"}],
                    }
                ]
            }
        )


def test_add_mode_keeps_the_original_upstream_and_appends_the_rewritten_one():
    transformer = _transformer(
        mode="add",
        platform_rules=[
            _mssql_rule([{"find": "src_lakehouse", "replace": "workspace-id.item-id"}])
        ],
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


def test_id_rules_apply_to_every_upstream_when_platform_rules_are_empty():
    transformer = RewriteUpstreamUrns(
        RewriteUpstreamUrnsConfig.model_validate(
            {"id_rules": [{"find": "src_lakehouse", "replace": "workspace-id.item-id"}]}
        ),
        PipelineContext(run_id="test"),
    )
    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED"),
            UpstreamClass(dataset=OTHER_PLATFORM, type="TRANSFORMED"),
        ]
    )

    rewritten = transformer.transform_aspect(DOWNSTREAM, "upstreamLineage", aspect)

    assert isinstance(rewritten, UpstreamLineageClass)
    assert rewritten.upstreams[0].dataset == make_dataset_urn(
        "mssql", "workspace-id.item-id.gold.bridge_table"
    )
    assert rewritten.upstreams[1].dataset == make_dataset_urn(
        "postgres", "workspace-id.item-id.gold.bridge_table"
    )


def _patch_operations(record: MetadataChangeProposalClass) -> list:
    assert record.aspect is not None
    payload = json.loads(record.aspect.value)
    return payload["patch"] if isinstance(payload, dict) else payload


def _path_components(path: str) -> list[str]:
    return [_Patch.unquote_path_component(part) for part in path[1:].split("/")]


def test_incremental_lineage_patch_rewrites_matching_upstreams():
    downstream_field = make_schema_field_urn(DOWNSTREAM, "fund_id")
    upstream_field = make_schema_field_urn(UPSTREAM, "fund_id")
    other_field = make_schema_field_urn(OTHER_PLATFORM, "fund_id")
    lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED"),
            UpstreamClass(dataset=OTHER_PLATFORM, type="TRANSFORMED"),
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[upstream_field, other_field],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[downstream_field],
            )
        ],
    )
    proposal = convert_upstream_lineage_to_patch(DOWNSTREAM, lineage, None).metadata
    transformer = _transformer(
        platform_rules=[
            _mssql_rule([{"find": "src_lakehouse", "replace": "workspace-id.item-id"}])
        ]
    )

    rewritten = list(
        transformer.transform([RecordEnvelope(record=proposal, metadata={})])
    )

    amended = make_dataset_urn("fabric", "workspace-id.item-id.gold.bridge_table")
    amended_field = make_schema_field_urn(amended, "fund_id")
    operations = _patch_operations(rewritten[0].record)
    paths = [_path_components(operation["path"]) for operation in operations]
    amended_values = [
        operation["value"]["dataset"]
        for operation, path in zip(operations, paths, strict=True)
        if path[:2] == ["upstreams", amended]
    ]
    assert amended_values == [amended]
    assert ["upstreams", amended] in paths
    assert ["upstreams", UPSTREAM] not in paths
    assert ["upstreams", OTHER_PLATFORM] in paths
    assert any(path[-1] == amended_field for path in paths)
    assert all(path[-1] != upstream_field for path in paths)
    assert any(downstream_field in path for path in paths)


def test_incremental_lineage_patch_add_mode_keeps_the_original_upstream():
    lineage = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=UPSTREAM, type="TRANSFORMED")]
    )
    proposal = convert_upstream_lineage_to_patch(DOWNSTREAM, lineage, None).metadata
    transformer = _transformer(
        mode="add",
        platform_rules=[
            _mssql_rule([{"find": "src_lakehouse", "replace": "workspace-id.item-id"}])
        ],
    )

    rewritten = list(
        transformer.transform([RecordEnvelope(record=proposal, metadata={})])
    )

    amended = make_dataset_urn("fabric", "workspace-id.item-id.gold.bridge_table")
    datasets = [
        _path_components(operation["path"])[1]
        for operation in _patch_operations(rewritten[0].record)
    ]
    assert datasets == [UPSTREAM, amended]


def test_platform_urn_is_reduced_to_its_name():
    config = RewriteUpstreamUrnsConfig.model_validate(
        {
            "platform_rules": [
                {
                    "find": "urn:li:dataPlatform:mssql",
                    "replace": "urn:li:dataPlatform:fabric",
                }
            ]
        }
    )
    assert config.platform_rules[0].find == "mssql"
    assert config.platform_rules[0].replace == "fabric"
