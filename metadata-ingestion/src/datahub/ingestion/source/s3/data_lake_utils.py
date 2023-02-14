import logging
from typing import Iterable, List, Optional

from datahub.emitter.mcp_builder import (
    FolderKey,
    KeyType,
    PlatformKey,
    S3BucketKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    is_s3_uri,
)

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class ContainerWUCreator:
    processed_containers: List[str]

    def __init__(self, platform, platform_instance, env):
        self.processed_containers = []
        self.platform = platform
        self.instance = platform_instance
        self.env = env

    def create_emit_containers(
        self,
        container_key: KeyType,
        name: str,
        sub_types: List[str],
        parent_container_key: Optional[PlatformKey] = None,
        domain_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if container_key.guid() not in self.processed_containers:
            container_wus = gen_containers(
                container_key=container_key,
                name=name,
                sub_types=sub_types,
                parent_container_key=parent_container_key,
                domain_urn=domain_urn,
            )
            self.processed_containers.append(container_key.guid())
            logger.debug(f"Creating container with key: {container_key}")
            for wu in container_wus:
                yield wu

    def gen_folder_key(self, abs_path):
        return FolderKey(
            platform=self.platform,
            instance=self.instance,
            backcompat_instance_for_guid=self.env,
            folder_abs_path=abs_path,
        )

    def gen_bucket_key(self, name):
        return S3BucketKey(
            platform="s3",
            instance=self.instance,
            backcompat_instance_for_guid=self.env,
            bucket_name=name,
        )

    def create_container_hierarchy(
        self, path: str, is_s3: bool, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"Creating containers for {dataset_urn}")
        base_full_path = path
        parent_key = None
        if is_s3_uri(path):
            bucket_name = get_bucket_name(path)
            bucket_key = self.gen_bucket_key(bucket_name)
            yield from self.create_emit_containers(
                container_key=bucket_key,
                name=bucket_name,
                sub_types=["S3 bucket"],
                parent_container_key=None,
            )
            parent_key = bucket_key
            base_full_path = get_bucket_relative_path(path)

        parent_folder_path = (
            base_full_path[: base_full_path.rfind("/")]
            if base_full_path.rfind("/") != -1
            else ""
        )

        # Dataset is in the root folder
        if not parent_folder_path and parent_key is None:
            logger.warning(
                f"Failed to associate Dataset ({dataset_urn}) with container"
            )
            return

        for folder in parent_folder_path.split("/"):
            abs_path = folder
            if parent_key:
                prefix: str = ""
                if isinstance(parent_key, S3BucketKey):
                    prefix = parent_key.bucket_name
                elif isinstance(parent_key, FolderKey):
                    prefix = parent_key.folder_abs_path
                abs_path = prefix + "/" + folder
            folder_key = self.gen_folder_key(abs_path)
            yield from self.create_emit_containers(
                container_key=folder_key,
                name=folder,
                sub_types=["Folder"],
                parent_container_key=parent_key,
            )
            parent_key = folder_key

        assert parent_key is not None
        yield from add_dataset_to_container(parent_key, dataset_urn)
