from typing import List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator


def _aspect_names(wus: List[MetadataWorkUnit]) -> List[Optional[str]]:
    names = []
    for wu in wus:
        mcp = wu.metadata
        # MetadataChangeProposalWrapper exposes aspectName; snapshots do not appear here
        names.append(getattr(mcp, "aspectName", None))
    return names


def test_create_folder_containers_emits_bucket_and_folder_chain() -> None:
    creator = ContainerWUCreator(platform="s3", platform_instance=None, env="PROD")

    wus = list(creator.create_folder_containers("s3://my-bucket/media/videos/2024"))

    # No dataset aspects at all — folders only.
    aspect_names = _aspect_names(wus)
    assert "datasetProperties" not in aspect_names

    # gen_containers legitimately emits a "container" aspect on each non-root
    # container to link it to its *parent container* — that's expected. What must
    # NOT happen is add_dataset_to_container, which would target a dataset urn.
    # So every "container"-aspect workunit's entity must itself be a container.
    container_link_wus = [
        wu for wu in wus if getattr(wu.metadata, "aspectName", None) == "container"
    ]
    assert container_link_wus  # sanity: folders do link to their parent container
    assert all(
        wu.get_urn().startswith("urn:li:container:") for wu in container_link_wus
    )

    # One containerProperties per distinct container: bucket + media + videos + 2024 = 4
    container_props = [n for n in aspect_names if n == "containerProperties"]
    assert len(container_props) == 4

    # Subtypes present: exactly one bucket subtype + three FOLDER subtypes
    subtype_payloads = [
        wu.metadata.aspect
        for wu in wus
        if getattr(wu.metadata, "aspectName", None) == "subTypes"
    ]
    flat = [t for a in subtype_payloads for t in a.typeNames]
    assert flat.count("Folder") == 3
    assert flat.count("S3 bucket") == 1


def test_create_folder_containers_dedupes_shared_parents() -> None:
    creator = ContainerWUCreator(platform="s3", platform_instance=None, env="PROD")

    list(creator.create_folder_containers("s3://my-bucket/media/videos/2023"))
    second = list(creator.create_folder_containers("s3://my-bucket/media/videos/2024"))

    # 'my-bucket', 'media', 'videos' were already emitted in the first call; only the
    # new leaf '2024' chain (just the 2024 container) should be emitted the second time.
    second_props = [
        wu
        for wu in second
        if getattr(wu.metadata, "aspectName", None) == "containerProperties"
    ]
    assert len(second_props) == 1
