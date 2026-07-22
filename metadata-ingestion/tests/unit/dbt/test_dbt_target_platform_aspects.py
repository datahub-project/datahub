"""Tests for target-platform dataPlatformInstance / browsePathsV2 emission.

When target_platform_instance is set, the dbt source emits platform-instance
metadata for target-platform sibling entities so that entities created only
via sibling/lineage references ("stubs") do not fall back to server-generated
name-derived defaults (plain-name browse folder, instance-less
dataPlatformInstance).
"""

from typing import Dict, List, Optional
from unittest import mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
)

TARGET_INSTANCE = "warehouse_instance"
INSTANCE_URN = (
    f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:postgres,{TARGET_INSTANCE})"
)


def create_dbt_source(
    config_overrides: Optional[Dict] = None,
    graph: Optional[mock.MagicMock] = mock.DEFAULT,
) -> DBTCoreSource:
    config: Dict = {
        "manifest_path": "temp/",
        "catalog_path": "temp/",
        "sources_path": "temp/",
        "target_platform": "postgres",
        "target_platform_instance": TARGET_INSTANCE,
        "enable_meta_mapping": False,
        **(config_overrides or {}),
    }
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    if graph is mock.DEFAULT:
        graph = mock.MagicMock()
        graph.get_aspect.return_value = None
    ctx.graph = graph
    return DBTCoreSource(DBTCoreConfig(**config), ctx)


def create_dbt_node() -> DBTNode:
    return DBTNode(
        database="warehouse_db",
        schema="warehouse_schema",
        name="my_table",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name="model.jaffle_shop.my_table",
        dbt_file_path="models/my_table.sql",
        dbt_package_name="jaffle_shop",
        node_type="model",
        max_loaded_at=None,
        materialization="table",
        catalog_type="table",
        missing_from_catalog=False,
        owner=None,
    )


def target_platform_workunit_aspects(source: DBTCoreSource, node: DBTNode) -> List:
    return [
        wu.metadata.aspect
        for wu in source.create_target_platform_mces([node])
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspect is not None
    ]


def get_aspect(aspects: List, aspect_type: type):
    matches = [a for a in aspects if isinstance(a, aspect_type)]
    assert len(matches) <= 1
    return matches[0] if matches else None


def test_emits_platform_instance_and_browse_path_for_stub_entity() -> None:
    source = create_dbt_source()
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    dpi = get_aspect(aspects, DataPlatformInstanceClass)
    assert dpi is not None
    assert dpi.platform == "urn:li:dataPlatform:postgres"
    assert dpi.instance == INSTANCE_URN

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id="warehouse_db"),
        BrowsePathEntryClass(id="warehouse_schema"),
    ]


def test_preserves_container_based_browse_path() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = BrowsePathsV2Class(
        path=[
            BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
            BrowsePathEntryClass(
                id="urn:li:container:abc123", urn="urn:li:container:abc123"
            ),
        ]
    )
    source = create_dbt_source(graph=graph)
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is not None
    assert get_aspect(aspects, BrowsePathsV2Class) is None


def test_replaces_plain_name_derived_browse_path() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = BrowsePathsV2Class(
        path=[
            BrowsePathEntryClass(id=TARGET_INSTANCE),
            BrowsePathEntryClass(id="warehouse_db"),
        ]
    )
    source = create_dbt_source(graph=graph)
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path[0].urn == INSTANCE_URN


def test_no_emission_without_target_platform_instance() -> None:
    source = create_dbt_source(config_overrides={"target_platform_instance": None})
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is None
    assert get_aspect(aspects, BrowsePathsV2Class) is None


def test_no_emission_when_disabled_by_config() -> None:
    source = create_dbt_source(
        config_overrides={"emit_target_platform_instance_aspects": False}
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is None
    assert get_aspect(aspects, BrowsePathsV2Class) is None


def test_skips_browse_path_without_graph_connection() -> None:
    source = create_dbt_source(graph=None)
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is not None
    assert get_aspect(aspects, BrowsePathsV2Class) is None


def test_skips_browse_path_when_aspect_read_fails() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.side_effect = RuntimeError("connection reset")
    source = create_dbt_source(graph=graph)
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is not None
    assert get_aspect(aspects, BrowsePathsV2Class) is None


def test_two_tier_node_omits_database_segment() -> None:
    node = create_dbt_node()
    node.database = None
    source = create_dbt_source()
    aspects = target_platform_workunit_aspects(source, node)

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert [entry.id for entry in browse.path] == [
        INSTANCE_URN,
        "warehouse_schema",
    ]
