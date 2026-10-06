from typing import Iterator, List
from unittest.mock import patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.abs.source import ABSSource
from datahub.metadata.schema_classes import ContainerPropertiesClass


def _abs_source(include: str) -> ABSSource:
    return ABSSource.create(
        {
            "path_specs": [{"include": include, "emit_folders_only": True}],
            "azure_config": {
                "account_name": "acct",
                "container_name": "media",
                "sas_token": "dummy",
            },
        },
        PipelineContext(run_id="abs-folders-only-test"),
    )


# Fake Azure folder listing: container-relative prefix -> its immediate subfolders.
# Includes folders BELOW the glob depth (videos/2023/q1/raw, q2/raw) that a correct
# depth-limited walk must never list. Only list_folders (the Azure listing primitive)
# is mocked; the real ABSSource.resolve_templated_folders recursion runs against this.
_FOLDER_TREE = {
    "videos/": ["videos/2023"],
    "videos/2023/": ["videos/2023/q1", "videos/2023/q2"],
    "videos/2023/q1/": ["videos/2023/q1/raw"],
    "videos/2023/q2/": ["videos/2023/q2/raw"],
}


def _fake_list_folders(
    container_name: str, prefix: str, azure_config: object = None
) -> Iterator[str]:
    return iter(_FOLDER_TREE.get(prefix, []))


def test_abs_emit_folders_only_walks_to_glob_depth_containers_only() -> None:
    # Depth-2 glob (videos/*/*/). q1/q2 sit at the glob depth; 'raw' sits below it
    # and must be absent, proving the walk stops at the number of '*' levels.
    source = _abs_source("https://acct.blob.core.windows.net/media/videos/*/*/")

    with patch(
        "datahub.ingestion.source.abs.source.list_folders",
        side_effect=_fake_list_folders,
    ):
        wus = list(source.get_workunits_internal())

    urns = {wu.get_urn() for wu in wus}
    assert urns  # non-empty
    assert all(u.startswith("urn:li:container:") for u in urns)
    assert not any(u.startswith("urn:li:dataset:") for u in urns)

    names: List[str] = []
    for wu in wus:
        if getattr(wu.metadata, "aspectName", None) != "containerProperties":
            continue
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        aspect = wu.metadata.aspect
        assert isinstance(aspect, ContainerPropertiesClass)
        names.append(aspect.name)

    # bucket 'media' + videos + 2023 + q1 + q2, each once. 'raw' (below glob depth)
    # is absent; the exact-multiset check also fails on any duplicate emission.
    assert sorted(names) == ["2023", "media", "q1", "q2", "videos"]
    assert source.report.folders_scanned == 2
