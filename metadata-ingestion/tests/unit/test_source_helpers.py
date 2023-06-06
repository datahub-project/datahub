from typing import Any, Dict, Iterable, List, Union

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_container_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_helpers import (
    auto_browse_path_v2,
    auto_status_aspect,
    auto_workunit,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit

_base_metadata: List[
    Union[MetadataChangeProposalWrapper, models.MetadataChangeEventClass]
] = [
    MetadataChangeProposalWrapper(
        entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
        aspect=models.ContainerPropertiesClass(
            name="test",
        ),
    ),
    MetadataChangeProposalWrapper(
        entityUrn="urn:li:container:108e111aa1d250dd52e0fd5d4b307b12",
        aspect=models.StatusClass(removed=True),
    ),
    models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)",
            aspects=[
                models.DatasetPropertiesClass(
                    customProperties={
                        "key": "value",
                    },
                ),
            ],
        ),
    ),
    models.MetadataChangeEventClass(
        proposedSnapshot=models.DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.hospital_beds,PROD)",
            aspects=[
                models.StatusClass(removed=True),
            ],
        ),
    ),
]


def test_auto_workunit():
    wu = list(auto_workunit(_base_metadata))
    assert all(isinstance(w, MetadataWorkUnit) for w in wu)

    ids = [w.id for w in wu]
    assert ids == [
        "urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a-containerProperties",
        "urn:li:container:108e111aa1d250dd52e0fd5d4b307b12-status",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)/mce",
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.hospital_beds,PROD)/mce",
    ]


def test_auto_status_aspect():
    initial_wu = list(auto_workunit(_base_metadata))

    expected = [
        *initial_wu,
        *list(
            auto_workunit(
                [
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:container:008e111aa1d250dd52e0fd5d4b307b1a",
                        aspect=models.StatusClass(removed=False),
                    ),
                    MetadataChangeProposalWrapper(
                        entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_aha.staffing,PROD)",
                        aspect=models.StatusClass(removed=False),
                    ),
                ]
            )
        ),
    ]
    assert list(auto_status_aspect(initial_wu)) == expected


def _create_container_aspects(d: Dict[str, Any]) -> Iterable[MetadataWorkUnit]:
    for k, v in d.items():
        yield MetadataChangeProposalWrapper(
            entityUrn=make_container_urn(k),
            aspect=models.StatusClass(removed=False),
        ).as_workunit()

        for child in list(v):
            yield MetadataChangeProposalWrapper(
                entityUrn=make_container_urn(child),
                aspect=models.ContainerClass(
                    container=make_container_urn(k),
                ),
            ).as_workunit()
        if isinstance(v, dict):
            yield from _create_container_aspects(v)


def _make_container_browse_path_entries(
    path: List[str],
) -> List[models.BrowsePathEntryClass]:
    return [
        models.BrowsePathEntryClass(id=make_container_urn(s), urn=make_container_urn(s))
        for s in path
    ]


def _make_browse_path_entries(path: List[str]) -> List[models.BrowsePathEntryClass]:
    return [models.BrowsePathEntryClass(id=s, urn=None) for s in path]


def _get_browse_paths_from_wu(
    stream: Iterable[MetadataWorkUnit],
) -> Dict[str, List[models.BrowsePathEntryClass]]:
    paths = {}
    for wu in stream:
        browse_path_v2 = wu.get_aspects_of_type(models.BrowsePathsV2Class)
        if browse_path_v2:
            name = wu.get_urn().split(":")[-1]
            paths[name] = browse_path_v2[0].path
    return paths


def test_auto_browse_path_v2_by_container_hierarchy():
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
        sum(len(wu.get_aspects_of_type(models.StatusClass)) for wu in wus) == 21
    )

    new_wus = list(auto_browse_path_v2([], wus))
    assert (
        sum(len(wu.get_aspects_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
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


def test_auto_browse_path_v2_ignores_urns_already_with():
    structure = {"a": {"b": {"c": {"d": ["e"]}}}}

    mcps = [
        *MetadataChangeProposalWrapper.construct_many(
            entityUrn=make_container_urn("f"),
            aspects=[
                models.BrowsePathsClass(paths=["/one/two"]),
                models.BrowsePathsV2Class(
                    path=_make_browse_path_entries(["my", "path"])
                ),
            ],
        ),
        MetadataChangeProposalWrapper(
            entityUrn=make_container_urn("c"),
            aspect=models.BrowsePathsV2Class(
                path=_make_container_browse_path_entries(["custom", "path"])
            ),
        ),
    ]
    wus = [
        *auto_status_aspect(
            [
                *_create_container_aspects(structure),
                *(mcp.as_workunit() for mcp in mcps),
            ]
        )
    ]
    new_wus = list(auto_browse_path_v2([], wus))
    assert (
        sum(len(wu.get_aspects_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 6
    )

    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["a"] == []
    assert paths["c"] == _make_container_browse_path_entries(["custom", "path"])
    assert paths["e"] == _make_container_browse_path_entries(["a", "b", "c", "d"])
    assert paths["f"] == _make_browse_path_entries(["my", "path"])


def test_auto_browse_path_v2_legacy_browse_path():
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
    new_wus = list(auto_browse_path_v2(["platform", "PROD", "unused"], wus))
    assert len(new_wus) == 6
    paths = _get_browse_paths_from_wu(new_wus)
    assert (
        paths["platform,dataset-1,PROD)"]
        == paths["platform,dataset-3,PROD)"]
        == _make_browse_path_entries(["one", "two"])
    )
    assert paths["platform,dataset-2,PROD)"] == _make_browse_path_entries(["something"])


def test_auto_browse_path_v2_container_over_legacy_browse_path():
    structure = {"a": {"b": ["c"]}}
    wus = list(
        auto_status_aspect(
            [
                *_create_container_aspects(structure),
                MetadataChangeProposalWrapper(
                    entityUrn=make_container_urn("b"),
                    aspect=models.BrowsePathsClass(paths=["/one/two"]),
                ).as_workunit(),
            ]
        )
    )
    new_wus = list(auto_browse_path_v2([], wus))
    assert (
        sum(len(wu.get_aspects_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 3
    )

    paths = _get_browse_paths_from_wu(new_wus)
    assert paths["a"] == []
    assert paths["b"] == _make_container_browse_path_entries(["a"])
    assert paths["c"] == _make_container_browse_path_entries(["a", "b"])
