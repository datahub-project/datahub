from typing import List, Union, Iterable, Dict, Any

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_container_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.utilities.source_helpers import (
    auto_status_aspect,
    auto_workunit,
    auto_browse_path_v2,
)

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


def _make_browse_path_entries(path: List[str]) -> List[models.BrowsePathEntryClass]:
    return [
        models.BrowsePathEntryClass(id=make_container_urn(s), urn=make_container_urn(s))
        for s in path
    ]


def test_auto_browse_path_v2():
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

    new_wus = list(auto_browse_path_v2(wus))
    assert (
        sum(len(wu.get_aspects_of_type(models.BrowsePathsV2Class)) for wu in new_wus)
        == 21
    )
    paths = {}
    for wu in new_wus:
        browse_path_v2 = wu.get_aspects_of_type(models.BrowsePathsV2Class)
        if browse_path_v2:
            name = wu.get_urn().split(":")[-1]
            paths[name] = browse_path_v2[0].path

    assert paths["one"] == []
    assert paths["7"] == paths["8"] == _make_browse_path_entries(["two", "c", "v"])
    assert paths["d"] == _make_browse_path_entries(["three"])
    assert paths["i"] == _make_browse_path_entries(["one", "a"])
