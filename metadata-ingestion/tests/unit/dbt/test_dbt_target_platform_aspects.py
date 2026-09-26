"""Tests for target-platform dataPlatformInstance / browsePathsV2 emission.

When target_platform_instance is set, the dbt source emits platform-instance
metadata for target-platform sibling entities so that entities created only
via sibling/lineage references ("stubs") do not fall back to server-generated
name-derived defaults (plain-name browse folder, instance-less
dataPlatformInstance).
"""

import json
import warnings
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar
from unittest import mock

import pytest

from datahub.configuration.common import ConfigurationWarning
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
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
    dataset_properties: Optional[DatasetPropertiesClass] = None,
    entities_error: Optional[Exception] = None,
) -> mock.MagicMock:
    """A graph that answers both the batched per-entity prefetch (``get_entities``)
    and the per-ancestor container walk (``get_aspect``).

    ``containers`` maps an entity/container urn to its parent container urn -
    it backs an entity's own container (read via the prefetch) as well as any
    ancestor container's parent (read individually, one hop at a time).
    """
    parents = containers or {}

    def get_aspect(urn: str, aspect_type: Type) -> Optional[object]:
        if aspect_type is ContainerClass:
            parent = parents.get(urn)
            return ContainerClass(container=parent) if parent else None
        return None

    def get_entities(
        entity_name: str, urns: List[str], aspects: List[str]
    ) -> Dict[str, Dict[str, Tuple[Any, None]]]:
        if entities_error is not None:
            raise entities_error
        result: Dict[str, Dict[str, Tuple[Any, None]]] = {}
        for urn in urns:
            entity_aspects: Dict[str, Tuple[Any, None]] = {}
            if browse_path is not None:
                entity_aspects[BrowsePathsV2Class.ASPECT_NAME] = (browse_path, None)
            parent = parents.get(urn)
            if parent:
                entity_aspects[ContainerClass.ASPECT_NAME] = (
                    ContainerClass(container=parent),
                    None,
                )
            if dataset_properties is not None:
                entity_aspects[DatasetPropertiesClass.ASPECT_NAME] = (
                    dataset_properties,
                    None,
                )
            if entity_aspects:
                result[urn] = entity_aspects
        return result

    graph = mock.MagicMock()
    graph.get_aspect.side_effect = get_aspect
    graph.get_entities.side_effect = get_entities
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


def create_dbt_node(
    name: str = "my_table",
    database: str = "warehouse_db",
    schema: str = "warehouse_schema",
) -> DBTNode:
    return DBTNode(
        database=database,
        schema=schema,
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


def aspects_by_urn(source: DBTCoreSource, nodes: List[DBTNode]) -> Dict[str, List]:
    """Emitted aspects grouped by entity urn, for runs spanning several nodes."""
    grouped: Dict[str, List] = {}
    for wu in source.create_target_platform_mces(nodes):
        mcp = wu.metadata
        if not isinstance(mcp, MetadataChangeProposalWrapper) or mcp.aspect is None:
            continue
        assert mcp.entityUrn is not None
        grouped.setdefault(mcp.entityUrn, []).append(mcp.aspect)
    return grouped


def urn_for(node: DBTNode) -> str:
    return node.get_urn("postgres", "PROD", TARGET_INSTANCE)


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


def test_skips_browse_path_and_display_name_when_prefetch_fails() -> None:
    # A failed batched read is indistinguishable from an empty one (no browse
    # path, no container, no properties), which the per-entity code reads as
    # "stub the warehouse never ingested" - so a failure must skip outright
    # rather than risk overwriting a warehouse-owned entity it simply failed
    # to see.
    graph = make_graph(entities_error=RuntimeError("connection reset"))
    source = create_dbt_source(
        config_overrides=DISPLAY_NAME_ENABLED,
        graph=graph,
    )
    aspects = target_platform_workunit_aspects(source, create_dbt_node())

    assert get_aspect(aspects, DataPlatformInstanceClass) is not None
    assert get_aspect(aspects, BrowsePathsV2Class) is None
    assert dataset_properties_patch_ops(source, create_dbt_node()) == []
    assert len(source.report.warnings) == 1


def test_skips_browse_path_when_ancestor_container_read_fails() -> None:
    # The entity's own container resolves fine (via the prefetch); it's an
    # ancestor hop above that - read individually, not batched - that fails.
    graph = make_graph(containers={NODE_URN: SCHEMA_CONTAINER_URN})
    graph.get_aspect.side_effect = RuntimeError("connection reset")
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


def test_target_platform_aspects_are_prefetched_in_one_batch() -> None:
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

    # Both datasets' own browsePathsV2/container/datasetProperties come from
    # one batched call...
    assert graph.get_entities.call_count == 1
    assert sorted(graph.get_entities.call_args.kwargs["urns"]) == sorted(
        [NODE_URN, other_urn]
    )

    # ...and only shared ANCESTOR containers are read individually, one hop at
    # a time, once each for the whole run - never the datasets' own urns.
    container_reads = [
        call.args[0]
        for call in graph.get_aspect.call_args_list
        if call.args[1] is ContainerClass
    ]
    assert container_reads.count(SCHEMA_CONTAINER_URN) == 1
    assert container_reads.count(DB_CONTAINER_URN) == 1
    assert NODE_URN not in container_reads
    assert other_urn not in container_reads


# emit_target_platform_display_name defaults to True; explicitly setting it
# here (even to the same value) marks it in pydantic's model_fields_set, which
# the warning tests below rely on.
DISPLAY_NAME_ENABLED = {"emit_target_platform_display_name": True}


def test_sets_display_name_by_default() -> None:
    source = create_dbt_source()
    assert dataset_properties_patch_ops(source, create_dbt_node()) == [
        {"op": "add", "path": "/name", "value": "my_table"}
    ]


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


def test_sets_display_name_when_stub_already_has_instance_only_path() -> None:
    # The upgrade path for the default-off flag: an earlier run wrote the
    # instance-only browse path, and enabling the flag later must still name the
    # entity even though that path needs no rewrite.
    source = create_dbt_source(
        config_overrides=DISPLAY_NAME_ENABLED,
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN)]
            )
        ),
    )
    node = create_dbt_node()

    assert dataset_properties_patch_ops(source, node) == [
        {"op": "add", "path": "/name", "value": "my_table"}
    ]
    # ...and the unchanged path is not proposed again.
    assert (
        get_aspect(target_platform_workunit_aspects(source, node), BrowsePathsV2Class)
        is None
    )


def test_no_display_name_when_one_is_already_set() -> None:
    source = create_dbt_source(
        config_overrides=DISPLAY_NAME_ENABLED,
        graph=make_graph(dataset_properties=DatasetPropertiesClass(name="my_table")),
    )
    assert dataset_properties_patch_ops(source, create_dbt_node()) == []


def test_upgrades_instance_only_path_once_containers_exist() -> None:
    source = create_dbt_source(
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN)]
            ),
            containers={NODE_URN: SCHEMA_CONTAINER_URN},
        )
    )
    browse = get_aspect(
        target_platform_workunit_aspects(source, create_dbt_node()), BrowsePathsV2Class
    )

    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN),
    ]


def test_warns_when_display_name_explicitly_set_without_target_platform_instance() -> (
    None
):
    with pytest.warns(ConfigurationWarning, match="emit_target_platform_display_name"):
        create_dbt_source(
            config_overrides={
                **DISPLAY_NAME_ENABLED,
                "target_platform_instance": None,
            }
        )


def test_warns_when_display_name_explicitly_set_without_instance_aspects_enabled() -> (
    None
):
    with pytest.warns(ConfigurationWarning, match="emit_target_platform_display_name"):
        create_dbt_source(
            config_overrides={
                **DISPLAY_NAME_ENABLED,
                "emit_target_platform_instance_aspects": False,
            }
        )


def test_no_warning_when_display_name_left_at_default_without_target_platform_instance() -> (
    None
):
    # The common case a naive default flip would break: a recipe that never
    # touches emit_target_platform_display_name and has no target_platform_instance
    # should not be warned about a flag it never set.
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConfigurationWarning)
        create_dbt_source(config_overrides={"target_platform_instance": None})


def test_stub_inherits_container_from_ingested_sibling() -> None:
    # The warehouse ingested one table in this schema and not the other. The
    # ingested one proves where the schema's folder is, so its neighbour joins it
    # rather than being stranded at the instance root.
    ingested = create_dbt_node(name="ingested_table")
    stub = create_dbt_node(name="stub_table")
    source = create_dbt_source(
        graph=make_graph(
            containers={
                urn_for(ingested): SCHEMA_CONTAINER_URN,
                SCHEMA_CONTAINER_URN: DB_CONTAINER_URN,
            }
        )
    )
    grouped = aspects_by_urn(source, [ingested, stub])

    container = get_aspect(grouped[urn_for(stub)], ContainerClass)
    assert container is not None
    assert container.container == SCHEMA_CONTAINER_URN

    browse = get_aspect(grouped[urn_for(stub)], BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [
        BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN),
        BrowsePathEntryClass(id=DB_CONTAINER_URN, urn=DB_CONTAINER_URN),
        BrowsePathEntryClass(id=SCHEMA_CONTAINER_URN, urn=SCHEMA_CONTAINER_URN),
    ]
    assert source.report.num_target_containers_inherited == 1


def test_inherited_stub_still_gets_a_display_name() -> None:
    # Joining a folder does not name the entity - it still has no
    # datasetProperties of its own.
    ingested = create_dbt_node(name="ingested_table")
    stub = create_dbt_node(name="stub_table")
    source = create_dbt_source(
        config_overrides=DISPLAY_NAME_ENABLED,
        graph=make_graph(containers={urn_for(ingested): SCHEMA_CONTAINER_URN}),
    )
    ops: List[Dict] = []
    for wu in source.create_target_platform_mces([ingested, stub]):
        mcp = wu.metadata
        if not isinstance(mcp, MetadataChangeProposalClass):
            continue
        if mcp.entityUrn == urn_for(stub) and mcp.aspectName == "datasetProperties":
            assert mcp.aspect is not None
            ops.extend(json.loads(mcp.aspect.value))

    assert ops == [{"op": "add", "path": "/name", "value": "stub_table"}]


def test_stub_stays_at_root_when_only_a_sibling_schema_was_ingested() -> None:
    # A neighbouring schema in the same database is not evidence about this
    # schema. Borrowing its database container would place the entity one level
    # up, but writing a container shallower than the key it is learned under
    # corrupts the next run's mapping and freezes the entity there, so it stays
    # at the root where it remains improvable.
    ingested = create_dbt_node(name="ingested_table", schema="other_schema")
    stub = create_dbt_node(name="stub_table", schema="dbt_only_schema")
    source = create_dbt_source(
        graph=make_graph(
            containers={
                urn_for(ingested): SCHEMA_CONTAINER_URN,
                SCHEMA_CONTAINER_URN: DB_CONTAINER_URN,
            }
        )
    )
    grouped = aspects_by_urn(source, [ingested, stub])

    assert get_aspect(grouped[urn_for(stub)], ContainerClass) is None
    browse = get_aspect(grouped[urn_for(stub)], BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN)]


def test_root_stub_is_placed_once_its_schema_is_ingested() -> None:
    # The self-correcting property the database fallback would have destroyed: a
    # single-entry path is never mistaken for a warehouse-owned one, so the
    # entity is picked up on the run after the warehouse reaches its schema.
    ingested = create_dbt_node(name="ingested_table")
    stub = create_dbt_node(name="stub_table")
    source = create_dbt_source(
        graph=make_graph(
            browse_path=BrowsePathsV2Class(
                path=[BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN)]
            ),
            containers={urn_for(ingested): SCHEMA_CONTAINER_URN},
        )
    )
    grouped = aspects_by_urn(source, [ingested, stub])

    container = get_aspect(grouped[urn_for(stub)], ContainerClass)
    assert container is not None
    assert container.container == SCHEMA_CONTAINER_URN


def test_stub_in_an_uningested_database_stays_at_instance_root() -> None:
    # Nothing in this database was ingested, so there is no folder to join and
    # none is invented.
    ingested = create_dbt_node(name="ingested_table", database="known_db")
    stub = create_dbt_node(name="stub_table", database="dbt_only_db")
    source = create_dbt_source(
        graph=make_graph(containers={urn_for(ingested): SCHEMA_CONTAINER_URN})
    )
    grouped = aspects_by_urn(source, [ingested, stub])

    assert get_aspect(grouped[urn_for(stub)], ContainerClass) is None
    browse = get_aspect(grouped[urn_for(stub)], BrowsePathsV2Class)
    assert browse is not None
    assert browse.path == [BrowsePathEntryClass(id=INSTANCE_URN, urn=INSTANCE_URN)]
    assert source.report.num_target_containers_inherited == 0
