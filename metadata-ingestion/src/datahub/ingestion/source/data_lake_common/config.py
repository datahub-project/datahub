import logging
from enum import Enum
from typing import List, Optional
from urllib.parse import urlparse

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

logger: logging.Logger = logging.getLogger(__name__)


class PathSpecsConfigMixin(ConfigModel):
    path_specs: List[PathSpec] = Field(
        description="List of PathSpec. See [below](#path-spec) the details about PathSpec"
    )


class S3PathMode(str, Enum):
    """How a path is interpreted by :meth:`S3LineageProviderConfig.get_s3_path`."""

    FILE = "file"
    DIRECTORY = "directory"


class S3LineageProviderConfig(ConfigModel):
    """
    Any source that produces s3 lineage from/to Datasets should inherit this class.
    """

    path_specs: List[PathSpec] = Field(
        default=[],
        description="List of PathSpec. See below the details about PathSpec",
    )

    strip_urls: bool = Field(
        default=True,
        description="Strip filename from s3 url. It only applies if path_specs are not specified.",
    )

    ignore_non_path_spec_path: bool = Field(
        default=False,
        description="Ignore paths that are not match in path_specs. It only applies if path_specs are specified.",
    )

    def get_s3_path(
        self, path: str, mode: S3PathMode = S3PathMode.FILE
    ) -> Optional[str]:
        for path_spec in self.path_specs:
            if mode is S3PathMode.FILE and path_spec.allowed(path):
                _, table_path = path_spec.extract_table_name_and_path(path)
                return table_path
            if mode is S3PathMode.DIRECTORY:
                folded_path = path_spec.fold_dir_to_table(path)
                if folded_path is not None:
                    return folded_path

        if self.ignore_non_path_spec_path and len(self.path_specs) > 0:
            logger.debug(f"Skipping s3 path {path} as it does not match any path spec.")
            return None

        if mode is S3PathMode.FILE and self.strip_urls:
            if "/" in urlparse(path).path:
                return str(path.rsplit("/", 1)[0])

        return path

    def get_s3_urn_for_lineage(
        self, path: str, env: str, mode: S3PathMode = S3PathMode.FILE
    ) -> Optional[str]:
        s3_path = self.get_s3_path(path, mode=mode)
        if s3_path is None:
            return None
        return make_s3_urn_for_lineage(s3_path, env)


class S3DatasetLineageProviderConfigBase(ConfigModel):
    """Groups all s3 lineage config under a single ``s3_lineage_config`` property."""

    s3_lineage_config: S3LineageProviderConfig = Field(
        default=S3LineageProviderConfig(),
        description="Common config for S3 lineage generation",
    )
