import logging
from typing import List, Optional
from urllib.parse import urlparse

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.aws.s3_util import is_s3_uri, make_s3_urn_for_lineage
from datahub.ingestion.source.azure.abs_utils import is_abs_uri, make_abs_urn
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.gcs.gcs_utils import GCS_PREFIX
from datahub.utilities.str_enum import StrEnum

logger: logging.Logger = logging.getLogger(__name__)

# Snowflake-style cloud prefixes that diverge from the canonical schemes used
# elsewhere in DataHub (``https://...blob.core.windows.net`` for ABS, ``gs://`` for GCS).
_AZURE_SNOWFLAKE_PREFIX = "azure://"
_GCS_SNOWFLAKE_PREFIX = "gcs://"


class PathSpecsConfigMixin(ConfigModel):
    path_specs: List[PathSpec] = Field(
        description="List of PathSpec. See [below](#path-spec) the details about PathSpec"
    )


class PathMode(StrEnum):
    """How a path is interpreted by :meth:`DataLakeLineageProviderConfig.get_path`."""

    FILE = "file"
    DIRECTORY = "directory"


S3PathMode = PathMode


class DataLakeLineageProviderConfig(ConfigModel):
    """
    Unified data lake lineage config. Applies ``path_specs`` to fold a path up to
    its ``{table}`` boundary and dispatches URN generation by scheme
    (``s3://``, ``gcs://``, Azure HTTPS / ``azure://``).
    """

    path_specs: List[PathSpec] = Field(
        default=[],
        description="List of PathSpec. See below the details about PathSpec",
    )

    strip_urls: bool = Field(
        default=True,
        description="Strip filename from the URL. Only applies if no path_specs are configured.",
    )

    ignore_non_path_spec_path: bool = Field(
        default=False,
        description="Ignore paths that do not match any path_spec. Only applies if path_specs are configured.",
    )

    def get_path(self, path: str, mode: PathMode = PathMode.FILE) -> Optional[str]:
        for path_spec in self.path_specs:
            if mode is PathMode.FILE and path_spec.allowed(path):
                _, table_path = path_spec.extract_table_name_and_path(path)
                return table_path
            if mode is PathMode.DIRECTORY:
                folded_path = path_spec.fold_dir_to_table(path)
                if folded_path is not None:
                    return folded_path

        if self.ignore_non_path_spec_path and len(self.path_specs) > 0:
            logger.debug(f"Skipping path {path} as it does not match any path spec.")
            return None

        if mode is PathMode.FILE and self.strip_urls:
            if "/" in urlparse(path).path:
                return str(path.rsplit("/", 1)[0])

        # Match fold_dir_to_table's trailing-slash normalization so the fallback
        # URN aligns with the folded one.
        return path.rstrip("/") if mode is PathMode.DIRECTORY else path

    def get_urn_for_lineage(
        self, url: str, env: str, mode: PathMode = PathMode.FILE
    ) -> Optional[str]:
        path = self.get_path(url, mode=mode)
        if path is None:
            return None
        if is_s3_uri(path):
            return make_s3_urn_for_lineage(path, env)
        gcs_prefix = next(
            (p for p in (_GCS_SNOWFLAKE_PREFIX, GCS_PREFIX) if path.startswith(p)),
            None,
        )
        if gcs_prefix is not None:
            return make_dataset_urn_with_platform_instance(
                platform="gcs",
                name=path[len(gcs_prefix) :].rstrip("/"),
                env=env,
                platform_instance=None,
            )
        if path.startswith(_AZURE_SNOWFLAKE_PREFIX):
            return make_abs_urn(
                path.replace(_AZURE_SNOWFLAKE_PREFIX, "https://", 1), env
            )
        if is_abs_uri(path):
            return make_abs_urn(path, env)
        logger.debug(f"Unsupported URL scheme for lineage: {url}")
        return None


class S3LineageProviderConfig(DataLakeLineageProviderConfig):
    """Alias of :class:`DataLakeLineageProviderConfig` kept for back-compat with sources (e.g. Redshift) that exposed ``s3_lineage_config``."""


class S3DatasetLineageProviderConfigBase(ConfigModel):
    """Groups all s3 lineage config under a single ``s3_lineage_config`` property."""

    s3_lineage_config: S3LineageProviderConfig = Field(
        default=S3LineageProviderConfig(),
        description="Common config for S3 lineage generation",
    )


class DataLakeLineageProviderConfigBase(ConfigModel):
    """Groups all data lake lineage config under a single ``datalake_lineage_config`` property."""

    datalake_lineage_config: DataLakeLineageProviderConfig = Field(
        default=DataLakeLineageProviderConfig(),
        description="Common config for data lake lineage generation (S3, GCS, ABS).",
    )
