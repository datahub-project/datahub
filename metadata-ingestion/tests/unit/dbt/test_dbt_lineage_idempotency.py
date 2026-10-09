from datetime import datetime, timezone

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTColumnLineageInfo,
    DBTNode,
    make_mapping_upstream_lineage,
)
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTTest,
    make_assertion_from_test,
)
from datahub.metadata.schema_classes import AssertionSourceTypeClass

# Distinct from the schema's unknown-time sentinel. These must not leak into lineage.
_MANIFEST_GENERATED_AT_MS = 1_700_000_000_000
_NODE_CREATED_AT_MS = 1_600_000_000_000
_CATALOG_GENERATED_AT_MS = 1_500_000_000_000
_RUN_EXECUTION_MS = 1_400_000_000_000
_UNKNOWN_LINEAGE_TIME_MS = 0


def _source_node() -> DBTNode:
    node = DBTNode(
        database="warehouse_prod",
        schema="source_ops",
        name="campaign",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="redshift",
        dbt_name="source.project.source_ops.campaign",
        dbt_file_path="models/sources.yml",
        dbt_package_name="project",
        node_type="source",
        max_loaded_at=datetime.fromtimestamp(_RUN_EXECUTION_MS / 1000, tz=timezone.utc),
        materialization=None,
        catalog_type="table",
        missing_from_catalog=False,
        owner=None,
        columns=[
            DBTColumn(
                name="id",
                comment="",
                description="",
                index=0,
                data_type="integer",
            )
        ],
    )
    node.meta = {
        "manifest_generated_at_ms": _MANIFEST_GENERATED_AT_MS,
        "created_at_ms": _NODE_CREATED_AT_MS,
        "catalog_generated_at_ms": _CATALOG_GENERATED_AT_MS,
    }
    return node


def _assert_unknown_lineage_time(lineage) -> None:
    assert lineage.upstreams
    for upstream in lineage.upstreams:
        assert upstream.auditStamp is not None
        assert upstream.auditStamp.time == _UNKNOWN_LINEAGE_TIME_MS
        assert upstream.auditStamp.time not in {
            _MANIFEST_GENERATED_AT_MS,
            _NODE_CREATED_AT_MS,
            _CATALOG_GENERATED_AT_MS,
            _RUN_EXECUTION_MS,
        }


def test_source_node_lineage_is_stable_across_emits() -> None:
    node = _source_node()
    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,warehouse_prod.source_ops.campaign,PROD)"
    downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,warehouse_prod.source_ops.campaign,PROD)"

    first = make_mapping_upstream_lineage(
        upstream_urn=upstream_urn,
        downstream_urn=downstream_urn,
        node=node,
        convert_column_urns_to_lowercase=False,
        skip_sources_in_lineage=False,
    )
    second = make_mapping_upstream_lineage(
        upstream_urn=upstream_urn,
        downstream_urn=downstream_urn,
        node=node,
        convert_column_urns_to_lowercase=False,
        skip_sources_in_lineage=False,
    )

    assert first.to_obj() == second.to_obj()
    assert first.fineGrainedLineages
    _assert_unknown_lineage_time(first)
    _assert_unknown_lineage_time(second)


def test_transformed_lineage_is_stable_across_emits() -> None:
    ctx = PipelineContext(run_id="dbt-idempotency")
    source = DBTCoreSource(
        DBTCoreConfig.model_validate(
            {
                "manifest_path": "unused",
                "catalog_path": "unused",
                "target_platform": "redshift",
                "include_column_lineage": True,
            }
        ),
        ctx,
    )
    upstream_name = "source.project.source_ops.campaign"
    model_name = "model.project.campaign_enriched"
    upstream = _source_node()
    upstream.dbt_name = upstream_name
    model = DBTNode(
        database="warehouse_prod",
        schema="analytics",
        name="campaign_enriched",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code="select id from {{ source('source_ops', 'campaign') }}",
        dbt_adapter="redshift",
        dbt_name=model_name,
        dbt_file_path="models/campaign_enriched.sql",
        dbt_package_name="project",
        node_type="model",
        max_loaded_at=datetime.fromtimestamp(
            _CATALOG_GENERATED_AT_MS / 1000, tz=timezone.utc
        ),
        materialization="table",
        catalog_type="table",
        missing_from_catalog=False,
        owner=None,
        upstream_nodes=[upstream_name],
        upstream_cll=[
            DBTColumnLineageInfo(
                upstream_dbt_name=upstream_name,
                upstream_col="id",
                downstream_col="id",
            )
        ],
    )
    model.meta = {"created_at_ms": _NODE_CREATED_AT_MS}
    nodes = {upstream_name: upstream, model_name: model}

    first = source._create_lineage_aspect_for_dbt_node(model, nodes)
    second = source._create_lineage_aspect_for_dbt_node(model, nodes)

    assert first is not None
    assert second is not None
    assert first.to_obj() == second.to_obj()
    assert first.fineGrainedLineages
    assert first.upstreams[0].type == "TRANSFORMED"
    _assert_unknown_lineage_time(first)
    _assert_unknown_lineage_time(second)


def test_assertion_info_is_stable_across_emits() -> None:
    node = _source_node()
    node.node_type = "test"
    node.name = "not_null_campaign_id"
    node.test_info = DBTTest(
        qualified_test_name="not_null",
        column_name="id",
        kw_args={"column_name": "id"},
    )
    upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,warehouse_prod.source_ops.campaign,PROD)"

    first = make_assertion_from_test(
        {"dbt_unique_id": node.dbt_name},
        node,
        "urn:li:assertion:campaign-not-null",
        upstream_urn,
    )
    second = make_assertion_from_test(
        {"dbt_unique_id": node.dbt_name},
        node,
        "urn:li:assertion:campaign-not-null",
        upstream_urn,
    )

    assert first.aspect is not None
    assert second.aspect is not None
    assert first.aspect.to_obj() == second.aspect.to_obj()
    assert first.aspect.source is not None
    assert first.aspect.source.type == AssertionSourceTypeClass.EXTERNAL
    assert first.aspect.source.created is None
