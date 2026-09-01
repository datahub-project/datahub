"""Tests for target-platform dataPlatformInstance / browsePathsV2 emission.

When target_platform_instance is set, the dbt source emits platform-instance
metadata for target-platform sibling entities so that entities created only
via sibling/lineage references ("stubs") do not fall back to server-generated
name-derived defaults (plain-name browse folder, instance-less
dataPlatformInstance).
"""

import json
from typing import Dict, List, Optional, Type, TypeVar
from unittest import mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DataPlatformInstanceClass,
    MetadataChangeProposalClass,
)

TARGET_INSTANCE = "warehouse_instance"
INSTANCE_URN = (
    f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:postgres,{TARGET_INSTANCE})"
)
NODE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,"
    f"{TARGET_INSTANCE}.warehouse_db.warehouse_schema.my_table,PROD)"
)
DB_CONTAINER_URN = "urn:li:container:db123"
SCHEMA_CONTAINER_URN = "urn:li:container:schema456"


def make_graph(
    browse_path: Optional[BrowsePathsV2Class] = None,
    containers: Optional[Dict[str, str]] = None,
) -> mock.MagicMock:
    """A graph that answers browse-path and container reads independently.

    ``containers`` maps an entity/container urn to its parent container urn.
    """
    parents = containers or {}

    def get_aspect(urn: str, aspect_type: Type) -> Optional[object]:
        if aspect_type is BrowsePathsV2Class:
            return browse_path
        if aspect_type is ContainerClass:
            parent = parents.get(urn)
            return ContainerClass(container=parent) if parent else None
        return None

    graph = mock.MagicMock()
    graph.get_aspect.side_effect = get_aspect
    return graph


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
        graph = make_graph()
    ctx.graph = graph
    return DBTCoreSource(DBTCoreConfig(**config), ctx)


def create_dbt_node(name: str = "my_table") -> DBTNode:
    return DBTNode(
        database="warehouse_db",
        schema="warehouse_schema",
        name=name,
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="postgres",
        dbt_name=f"model.jaffle_shop.{name}",
        dbt_file_path=f"models/{name}.sql",
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


def dataset_properties_patch_ops(source: DBTCoreSource, node: DBTNode) -> List[Dict]:
    """The raw JSON patch ops proposed against datasetProperties."""
    ops: List[Dict] = []
    for wu in source.create_target_platform_mces([node]):
        mcp = wu.metadata
        if not isinstance(mcp, MetadataChangeProposalClass):
            continue
        if mcp.aspectName != "datasetProperties" or mcp.aspect is None:
            continue
        ops.extend(json.loads(mcp.aspect.value))
    return ops


T = TypeVar("T")


def get_aspect(aspects: List, aspect_type: Type[T]) -> Optional[T]:
    matches = [a for a in aspects if isinstance(a, aspect_type)]
    assert len(matches) <= 1
    return matches[0] if matches else None


def test_node_urn_matches_expected_shape() -> None:
    assert create_dbt_node().get_urn("postgres", "PROD", TARGET_INSTANCE) == NODE_URN


def test_emits_instance_only_path_when_entity_has_no_container() -> None:
    # The warehouse connector has not ingested this table, so there is no real
    # folder to nest under - the entity sits directly under the instance rather
    # than in a fabricated database/schema folder.
    source = create_dbt_source()
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    dpi = get_aspect(aspects, DataPlatformInstanceClass)
    assert dpi is not None
    assert dpi.platform == "urn:li:dataPlatform:postgres"
    assert dpi.instance == INSTANCE_URN

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN)]


def test_builds_path_from_real_container_chain() -> None:
    source = create_dbt_source(
        graph=make_graph(
            containers={
                NODE_URN: SCHEMA_CONTAINER_URN,
                SCHEMA_CONTAINER_URN: DB_CONTAINER_URN,
            }
        )
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id=DB_CONTAINER_URN, urn=DB_CONTAINER_URN),
        BrowsePathEntryClass(id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN),
    ]


def test_preserves_container_based_browse_path() -> None:
    source = create_dbt_source(
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
                    BrowsePathEntryClass(
                        id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN
                    ),
                ]
            ),
            containers={NODE_URN: SCHEMA_CONTAINER_URN},
        )
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is not None
    assert get_aspect(aspects, BrowsePathsV2Class) is None


def test_replaces_plain_name_derived_browse_path() -> None:
    source = create_dbt_source(
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=TARGET_INSTANCE),
                    BrowsePathEntryClass(id="warehouse_db"),
                ]
            ),
            containers={NODE_URN: SCHEMA_CONTAINER_URN},
        )
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN),
    ]


def test_replaces_previously_guessed_plain_segments() -> None:
    # The shape this source itself wrote before it resolved real containers:
    # instance urn at the root, plain database/schema names below it.
    source = create_dbt_source(
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
                    BrowsePathEntryClass(id="warehouse_db"),
                    BrowsePathEntryClass(id="warehouse_schema"),
                ]
            ),
            containers={NODE_URN: SCHEMA_CONTAINER_URN},
        )
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN),
    ]


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
    assert len(source.report.warnings) == 1


def test_skips_browse_path_when_container_read_fails() -> None:
    def get_aspect_impl(urn: str, aspect_type: Type) -> Optional[object]:
        if aspect_type is ContainerClass:
            raise RuntimeError("connection reset")
        return None

    graph = mock.MagicMock()
    graph.get_aspect.side_effect = get_aspect_impl
    source = create_dbt_source(graph=graph)
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is not None
    assert get_aspect(aspects, BrowsePathsV2Class) is None
    assert len(source.report.warnings) == 1


def test_cyclic_container_chain_terminates() -> None:
    source = create_dbt_source(
        graph=make_graph(
            containers={
                NODE_URN: SCHEMA_CONTAINER_URN,
                SCHEMA_CONTAINER_URN: DB_CONTAINER_URN,
                DB_CONTAINER_URN: SCHEMA_CONTAINER_URN,
            }
        )
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    browse = get_aspect(aspects, BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id=DB_CONTAINER_URN, urn=DB_CONTAINER_URN),
        BrowsePathEntryClass(id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN),
    ]


def test_container_ancestors_are_read_once_across_nodes() -> None:
    node = create_dbt_node()
    other_node = create_dbt_node(name="other_table")
    other_urn = other_node.get_urn("postgres", "PROD", TARGET_INSTANCE)
    graph = make_graph(
        containers={
            NODE_URN: SCHEMA_CONTAINER_URN,
            other_urn: SCHEMA_CONTAINER_URN,
            SCHEMA_CONTAINER_URN: DB_CONTAINER_URN,
        }
    )
    source = create_dbt_source(graph=graph)
    list(source.create_target_platform_mces([node, other_node]))

    container_reads = [
        call.args[0]
        for call in graph.get_aspect.call_args_list
        if call.args[1] is ContainerClass
    ]
    # Shared ancestors are resolved once for the whole run...
    assert container_reads.count(SCHEMA_CONTAINER_URN) == 1
    assert container_reads.count(DB_CONTAINER_URN) == 1
    # ...while each dataset still gets its own lookup.
    assert sorted(u for u in container_reads if u.startswith("urn:li:dataset")) == (
        sorted([NODE_URN, other_urn])
    )


DISPLAY_NAME_ENABLED = {"emit_target_platform_display_name": True}


def test_no_display_name_by_default() -> None:
    source = create_dbt_source()
    assert dataset_properties_patch_ops(source, create_dbt_node()) == []


def test_sets_display_name_on_stub_entity_when_enabled() -> None:
    # Without this the UI falls back to the urn's name, showing the full
    # dotted path instead of the table name.
    source = create_dbt_source(config_overrides=DISPLAY_NAME_ENABLED)
    ops = dataset_properties_patch_ops(source, create_dbt_node())

    assert ops == [{"op": "add", "path": "/name", "value": "my_table"}]


def test_no_display_name_when_warehouse_owns_the_entity() -> None:
    source = create_dbt_source(
        config_overrides=DISPLAY_NAME_ENABLED,
        graph=make_graph(containers={NODE_URN: SCHEMA_CONTAINER_URN}),
    )
    assert dataset_properties_patch_ops(source, create_dbt_node()) == []


def test_no_display_name_when_browse_path_is_container_based() -> None:
    source = create_dbt_source(
        config_overrides=DISPLAY_NAME_ENABLED,
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
                    BrowsePathEntryClass(
                        id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN
                    ),
                ]
            )
        ),
    )
    assert dataset_properties_patch_ops(source, create_dbt_node()) == []
