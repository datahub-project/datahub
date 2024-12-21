from itertools import zip_longest
from typing import Any, Dict, Iterable, List
from unittest.mock import patch

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import (
    make_container_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.ingestion.api.source_helpers import (
    _prepend_platform_instance,
    auto_browse_path_v2,
    auto_status_aspect,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_utils import gen_schema_container
from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class


def test_auto_browse_path_v2_gen_containers_threaded():
    database_key = DatabaseKey(platform="snowflake", database="db")
    schema_keys = [
        SchemaKey(platform="snowflake", database="db", schema=f"schema_{i}")
        for i in range(10)
    ]

    wus_per_schema = [
        gen_schema_container(
            schema=key.db_schema,
            database=key.database,
            sub_types=[],
            database_container_key=database_key,
            schema_container_key=key,
        )
        for key in schema_keys
    ]
    for wu in auto_browse_path_v2(_iterate_wus_round_robin(wus_per_schema)):
        aspect = wu.get_aspect_of_type(BrowsePathsV2Class)
        if aspect:
            assert aspect.path == [
                BrowsePathEntryClass(
                    id=database_key.as_urn(), urn=database_key.as_urn()
                )
            ]


def _iterate_wus_round_robin(
    mcps_per_schema: List[Iterable[MetadataWorkUnit]],
) -> Iterable[MetadataWorkUnit]:
    # Simulate a potential ordering of MCPs when using thread pool to generate MCPs
    for wus in zip_longest(*mcps_per_schema, fillvalue=None):
        for wu in wus:
            if wu is not None:
                yield wu


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_by_container_hierarchy(telemetry_ping_mock):
    structure = {
        "one": {
            "a": {"i": ["1", "2", "3"], "ii": ["4"]},
            "b": {"iii": ["5", "6"]},
        },
        "two": {
            "c": {"iv": [], "v": ["7", "8"]},
        },
        "three": {"d": {}},
        "four": {},
    }

    wus = list(auto_status_aspect(_create_container_aspects(structure)))
    assert (  # Sanity check
        sum(bool(wu.get_aspect_of_type(models.StatusClass)) for wu in wus) == 21
    )

    new_wus = list(auto_browse_path_v2(wus))
    assert not telemetry_ping_mock.call_count, telemetry_ping_mock.call_args_list
    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 21
    )

    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["one"] == []
    assert (
        paths["7"]
        == paths["8"]
        == _make_container_browse_path_entries(["two", "c", "v"])
    )
    assert paths["d"] == _make_container_browse_path_entries(["three"])
    assert paths["i"] == _make_container_browse_path_entries(["one", "a"])

    # Check urns emitted on demand -- not all at end
    for urn in {wu.get_urn() for wu in new_wus}:
        try:
            idx = next(
                i
                for i, wu in enumerate(new_wus)
                if wu.get_aspect_of_type(models.ContainerClass) and wu.get_urn() == urn
            )
        except StopIteration:
            idx = next(
                i
                for i, wu in enumerate(new_wus)
                if wu.get_aspect_of_type(models.StatusClass) and wu.get_urn() == urn
            )
        assert new_wus[idx + 1].get_aspect_of_type(
            models.BrowsePathsV2Class
        ) or new_wus[idx + 2].get_aspect_of_type(models.BrowsePathsV2Class)


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_ignores_urns_already_with(telemetry_ping_mock):
    structure = {"a": {"b": {"c": {"d": ["e"]}}}}

    wus = [
        *auto_status_aspect(
            _create_container_aspects(
                structure,
                other_aspects={
                    "f": [
                        models.BrowsePathsClass(paths=["/one/two"]),
                        models.BrowsePathsV2Class(
                            path=_make_browse_path_entries(["my", "path"])
                        ),
                    ],
                    "c": [
                        models.BrowsePathsV2Class(
                            path=_make_container_browse_path_entries(["custom", "path"])
                        )
                    ],
                },
            ),
        )
    ]
    new_wus = list(auto_browse_path_v2(wus))
    assert not telemetry_ping_mock.call_count, telemetry_ping_mock.call_args_list
    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 6
    )

    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["a"] == []
    assert paths["c"] == _make_container_browse_path_entries(["custom", "path"])
    assert paths["f"] == _make_browse_path_entries(["my", "path"])
    assert paths["d"] == _make_container_browse_path_entries(["custom", "path", "c"])
    assert paths["e"] == _make_container_browse_path_entries(
        ["custom", "path", "c", "d"]
    )


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_with_platform_instance_and_source_browse_path_v2(
    telemetry_ping_mock,
):
    structure = {"a": {"b": {"c": {"d": ["e"]}}}}

    platform = "platform"
    instance = "instance"

    wus = [
        *auto_status_aspect(
            _create_container_aspects(
                structure,
                other_aspects={
                    "a": [
                        models.BrowsePathsV2Class(
                            path=_make_browse_path_entries(["my", "path"]),
                        ),
                    ],
                },
            ),
        )
    ]
    new_wus = list(
        auto_browse_path_v2(wus, platform=platform, platform_instance=instance)
    )
    assert not telemetry_ping_mock.call_count, telemetry_ping_mock.call_args_list
    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 5
    )

    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["a"] == _with_platform_instance(
        _make_browse_path_entries(["my", "path"]),
    )
    assert paths["b"] == _with_platform_instance(
        [
            *_make_browse_path_entries(["my", "path"]),
            *_make_container_browse_path_entries(["a"]),
        ],
    )
    assert paths["c"] == _with_platform_instance(
        [
            *_make_browse_path_entries(["my", "path"]),
            *_make_container_browse_path_entries(["a", "b"]),
        ],
    )
    assert paths["d"] == _with_platform_instance(
        [
            *_make_browse_path_entries(["my", "path"]),
            *_make_container_browse_path_entries(["a", "b", "c"]),
        ],
    )
    assert paths["e"] == _with_platform_instance(
        [
            *_make_browse_path_entries(["my", "path"]),
            *_make_container_browse_path_entries(["a", "b", "c", "d"]),
        ],
    )


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_legacy_browse_path(telemetry_ping_mock):
    platform = "platform"
    env = "PROD"
    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(platform, "dataset-1", env),
            aspect=models.BrowsePathsClass(["/one/two"]),
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(platform, "dataset-2", env),
            aspect=models.BrowsePathsClass([f"/{platform}/{env}/something"]),
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(platform, "dataset-3", env),
            aspect=models.BrowsePathsClass([f"/{platform}/one/two"]),
        ).as_workunit(),
    ]
    new_wus = list(auto_browse_path_v2(wus, drop_dirs=["platform", "PROD", "unused"]))
    assert not telemetry_ping_mock.call_count, telemetry_ping_mock.call_args_list
    assert len(new_wus) == 6
    paths = _get_browse_paths_from_wu(new_wus)
    assert (
        paths["platform,dataset-1,PROD)"]
        == paths["platform,dataset-3,PROD)"]
        == _make_browse_path_entries(["one", "two"])
    )
    assert paths["platform,dataset-2,PROD)"] == _make_browse_path_entries(["something"])


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_container_over_legacy_browse_path(telemetry_ping_mock):
    structure = {"a": {"b": ["c"]}}
    wus = list(
        auto_status_aspect(
            _create_container_aspects(
                structure,
                other_aspects={"b": [models.BrowsePathsClass(paths=["/one/two"])]},
            ),
        )
    )
    new_wus = list(auto_browse_path_v2(wus))
    assert not telemetry_ping_mock.call_count, telemetry_ping_mock.call_args_list
    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 3
    )

    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["a"] == []
    assert paths["b"] == _make_container_browse_path_entries(["a"])
    assert paths["c"] == _make_container_browse_path_entries(["a", "b"])


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_with_platform_instance(telemetry_ping_mock):
    platform = "my_platform"
    platform_instance = "my_instance"
    platform_instance_urn = make_dataplatform_instance_urn(platform, platform_instance)
    platform_instance_entry = models.BrowsePathEntryClass(
        platform_instance_urn, platform_instance_urn
    )

    structure = {"a": {"b": ["c"]}}
    wus = list(auto_status_aspect(_create_container_aspects(structure)))

    new_wus = list(
        auto_browse_path_v2(
            wus,
            platform=platform,
            platform_instance=platform_instance,
        )
    )
    assert telemetry_ping_mock.call_count == 0

    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 3
    )
    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["a"] == [platform_instance_entry]
    assert paths["b"] == [
        platform_instance_entry,
        *_make_container_browse_path_entries(["a"]),
    ]
    assert paths["c"] == [
        platform_instance_entry,
        *_make_container_browse_path_entries(["a", "b"]),
    ]


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_invalid_batch_telemetry(telemetry_ping_mock):
    structure = {"a": {"b": ["c"]}}
    b_urn = make_container_urn("b")
    wus = [
        *_create_container_aspects(structure),
        MetadataChangeProposalWrapper(  # Browse path for b separate from its Container aspect
            entityUrn=b_urn,
            aspect=models.BrowsePathsClass(paths=["/one/two"]),
        ).as_workunit(),
    ]
    wus = list(auto_status_aspect(wus))

    assert telemetry_ping_mock.call_count == 0
    _ = list(auto_browse_path_v2(wus))
    assert telemetry_ping_mock.call_count == 1
    assert telemetry_ping_mock.call_args_list[0][0][0] == "incorrect_browse_path_v2"
    assert telemetry_ping_mock.call_args_list[0][0][1]["num_out_of_order"] == 0
    assert telemetry_ping_mock.call_args_list[0][0][1]["num_out_of_batch"] == 1


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_no_invalid_batch_telemetry_for_unrelated_aspects(
    telemetry_ping_mock,
):
    structure = {"a": {"b": ["c"]}}
    b_urn = make_container_urn("b")
    wus = [
        *_create_container_aspects(structure),
        MetadataChangeProposalWrapper(  # Browse path for b separate from its Container aspect
            entityUrn=b_urn,
            aspect=models.ContainerPropertiesClass("container name"),
        ).as_workunit(),
    ]
    wus = list(auto_status_aspect(wus))

    assert telemetry_ping_mock.call_count == 0
    _ = list(auto_browse_path_v2(wus))
    assert telemetry_ping_mock.call_count == 0


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_invalid_order_telemetry(telemetry_ping_mock):
    structure = {"a": {"b": ["c"]}}
    wus = list(reversed(list(_create_container_aspects(structure))))
    wus = list(auto_status_aspect(wus))

    assert telemetry_ping_mock.call_count == 0
    new_wus = list(auto_browse_path_v2(wus))
    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        > 0
    )
    assert telemetry_ping_mock.call_count == 1
    assert telemetry_ping_mock.call_args_list[0][0][0] == "incorrect_browse_path_v2"
    assert telemetry_ping_mock.call_args_list[0][0][1]["num_out_of_order"] == 1
    assert telemetry_ping_mock.call_args_list[0][0][1]["num_out_of_batch"] == 0


@patch("datahub.ingestion.api.source_helpers.telemetry.telemetry_instance.ping")
def test_auto_browse_path_v2_dry_run(telemetry_ping_mock):
    structure = {"a": {"b": ["c"]}}
    wus = list(reversed(list(_create_container_aspects(structure))))
    wus = list(auto_status_aspect(wus))

    assert telemetry_ping_mock.call_count == 0
    new_wus = list(auto_browse_path_v2(wus, dry_run=True))
    assert wus == new_wus
    assert (
        sum(bool(wu.get_aspect_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 0
    )
    assert telemetry_ping_mock.call_count == 1


def _with_platform_instance(
    path: List[models.BrowsePathEntryClass],
) -> List[models.BrowsePathEntryClass]:
    platform = "platform"
    instance = "instance"
    return _prepend_platform_instance(path, platform, instance)


def _get_browse_paths_from_wu(
    stream: Iterable[MetadataWorkUnit],
) -> Dict[str, List[models.BrowsePathEntryClass]]:
    paths = {}
    for wu in stream:
        browse_path_v2 = wu.get_aspect_of_type(models.BrowsePathsV2Class)
        if browse_path_v2:
            name = wu.get_urn().split(":")[-1]
            paths[name] = browse_path_v2.path
    return paths


def _create_container_aspects(
    d: Dict[str, Any],
    other_aspects: Dict[str, List[models._Aspect]] = {},
    root: bool = True,
) -> Iterable[MetadataWorkUnit]:
    for k, v in d.items():
        urn = make_container_urn(k)
        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=models.StatusClass(removed=False)
        ).as_workunit()

        for aspect in other_aspects.pop(k, []):
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=aspect
            ).as_workunit()

        for child in list(v):
            yield MetadataChangeProposalWrapper(
                entityUrn=make_container_urn(child),
                aspect=models.ContainerClass(container=urn),
            ).as_workunit()
        if isinstance(v, dict):
            yield from _create_container_aspects(
                v, other_aspects=other_aspects, root=False
            )

    if root:
        for k, v in other_aspects.items():
            for aspect in v:
                yield MetadataChangeProposalWrapper(
                    entityUrn=make_container_urn(k), aspect=aspect
                ).as_workunit()


def _make_container_browse_path_entries(
    path: List[str],
) -> List[models.BrowsePathEntryClass]:
    return [
        models.BrowsePathEntryClass(id=make_container_urn(s), urn=make_container_urn(s))
        for s in path
    ]


def _make_browse_path_entries(path: List[str]) -> List[models.BrowsePathEntryClass]:
    return [models.BrowsePathEntryClass(id=s, urn=None) for s in path]
