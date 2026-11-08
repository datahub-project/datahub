from typing import List, Optional
from unittest import mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTColumnLineageInfo, DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import ChangeTypeClass, UpstreamLineageClass
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult


def _source(
    *,
    prefer_sql_parser_lineage: bool = False,
    skip_missing_upstreams_in_lineage: bool = False,
    include_column_lineage: bool = True,
    incremental_lineage: bool = True,
    write_semantics: str = "OVERRIDE",
) -> DBTCoreSource:
    config = DBTCoreConfig(
        manifest_path="unused",
        write_semantics=write_semantics,
        incremental_lineage=incremental_lineage,
        target_platform="postgres",
        prefer_sql_parser_lineage=prefer_sql_parser_lineage,
        skip_sources_in_lineage=prefer_sql_parser_lineage,
        entities_enabled={"sources": "NO"} if prefer_sql_parser_lineage else {},
        skip_missing_upstreams_in_lineage=skip_missing_upstreams_in_lineage,
        include_column_lineage=include_column_lineage,
    )
    ctx = PipelineContext(run_id="empty-lineage")
    ctx.graph = mock.MagicMock()
    return DBTCoreSource(config, ctx)


def _node(
    name: str = "report",
    *,
    node_type: str = "model",
    upstream_nodes: Optional[List[str]] = None,
) -> DBTNode:
    return DBTNode(
        database="test",
        schema="public",
        name=name,
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code="select 1 as id",
        compiled_code="select 1 as id",
        dbt_adapter="postgres",
        dbt_name=f"{node_type}.project.{name}",
        dbt_file_path=f"models/{name}.sql",
        dbt_package_name="project",
        node_type=node_type,
        max_loaded_at=None,
        materialization="table",
        catalog_type="table",
        missing_from_catalog=False,
        owner=None,
        upstream_nodes=upstream_nodes or [],
    )


@pytest.mark.parametrize("node_type", ["model", "seed", "snapshot"])
@pytest.mark.parametrize("include_column_lineage", [False, True])
def test_declared_empty_lineage_is_emitted(
    node_type: str,
    include_column_lineage: bool,
) -> None:
    source = _source(include_column_lineage=include_column_lineage)
    node = _node(node_type=node_type)
    first = source._create_lineage_aspect_for_dbt_node(node, {node.dbt_name: node})
    second = source._create_lineage_aspect_for_dbt_node(node, {node.dbt_name: node})
    assert first is not None
    assert second is not None
    assert first.upstreams == []
    assert not first.fineGrainedLineages
    assert first.to_obj() == second.to_obj()


def test_removing_last_dependency_clears_table_and_column_lineage() -> None:
    source = _source()
    upstream = _node("upstream")
    node = _node(upstream_nodes=[upstream.dbt_name])
    node.upstream_cll = [
        DBTColumnLineageInfo(
            upstream_dbt_name=upstream.dbt_name,
            upstream_col="id",
            downstream_col="id",
        )
    ]
    nodes = {upstream.dbt_name: upstream, node.dbt_name: node}
    before = source._create_lineage_aspect_for_dbt_node(node, nodes)
    assert before is not None
    assert len(before.upstreams) == 1
    assert before.fineGrainedLineages

    node.upstream_nodes = []
    node.upstream_cll = []
    after = source._create_lineage_aspect_for_dbt_node(node, nodes)
    assert after is not None
    assert after.upstreams == []
    assert not after.fineGrainedLineages


def test_unresolved_manifest_dependencies_do_not_clear_lineage() -> None:
    source = _source()
    node = _node(upstream_nodes=["model.project.not_in_manifest"])
    assert source._create_lineage_aspect_for_dbt_node(node, {}) is None


def test_filtered_dependencies_do_not_clear_lineage() -> None:
    source = _source(skip_missing_upstreams_in_lineage=True)
    upstream = _node("upstream")
    node = _node(upstream_nodes=[upstream.dbt_name])
    with mock.patch.object(source, "_upstream_exists_in_datahub", return_value=False):
        assert (
            source._create_lineage_aspect_for_dbt_node(
                node,
                {upstream.dbt_name: upstream},
            )
            is None
        )


@pytest.mark.parametrize("has_manifest_dependency", [False, True])
def test_successful_empty_sql_parse_is_authoritative(
    has_manifest_dependency: bool,
) -> None:
    source = _source(prefer_sql_parser_lineage=True)
    upstream = _node("upstream")
    node = _node(upstream_nodes=[upstream.dbt_name] if has_manifest_dependency else [])
    node.raw_sql_parsing_result = SqlParsingResult(in_tables=[], out_tables=[])
    result = source._create_lineage_aspect_for_dbt_node(
        node,
        {upstream.dbt_name: upstream},
    )
    assert result is not None
    assert result.upstreams == []


@pytest.mark.parametrize(
    "parsed",
    [None, SqlParsingResult.make_from_error(ValueError("invalid SQL"))],
)
def test_unavailable_sql_parse_does_not_clear_lineage(
    parsed: Optional[SqlParsingResult],
) -> None:
    source = _source(prefer_sql_parser_lineage=True)
    node = _node()
    node.raw_sql_parsing_result = parsed
    assert source._create_lineage_aspect_for_dbt_node(node, {}) is None


def test_filtered_sql_dependencies_do_not_clear_lineage() -> None:
    source = _source(
        prefer_sql_parser_lineage=True,
        skip_missing_upstreams_in_lineage=True,
    )
    node = _node()
    node.raw_sql_parsing_result = SqlParsingResult(
        in_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:postgres,test.public.upstream,PROD)"
        ],
        out_tables=[],
    )
    with mock.patch.object(source, "_upstream_exists_in_datahub", return_value=False):
        assert source._create_lineage_aspect_for_dbt_node(node, {}) is None


@pytest.mark.parametrize("incremental_lineage", [False, True])
@pytest.mark.parametrize("write_semantics", ["PATCH", "OVERRIDE"])
def test_empty_lineage_is_emitted_as_a_full_dbt_mcp(
    incremental_lineage: bool, write_semantics: str
) -> None:
    source = _source(
        incremental_lineage=incremental_lineage, write_semantics=write_semantics
    )
    node = _node()
    workunits = list(source.create_dbt_platform_mcps([node], {node.dbt_name: node}))
    lineage_mcps = [
        wu.metadata
        for wu in workunits
        if wu.get_aspect_of_type(UpstreamLineageClass) is not None
    ]
    assert len(lineage_mcps) == 1
    mcp = lineage_mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert isinstance(mcp.aspect, UpstreamLineageClass)
    assert mcp.entityUrn == source._dbt_platform_urn(node)
    assert mcp.changeType == ChangeTypeClass.UPSERT
    assert mcp.aspect.upstreams == []
    assert not mcp.aspect.fineGrainedLineages
    assert not source.report.failures
