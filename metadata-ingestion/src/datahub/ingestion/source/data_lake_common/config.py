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
        description="When no path_spec matches, fall back to stripping the last "
        "segment from the path. Applies to file-path lineage only (e.g. Redshift "
        "COPY/UNLOAD); it has no effect when resolving directory locations such as "
        "Snowflake external stages.",
    )

    ignore_non_path_spec_path: bool = Field(
        default=False,
        description="Ignore paths that do not match any path_spec. Only applies if path_specs are configured.",
    )

    # TODO: get_path / get_urn_for_lineage make this ConfigModel a service as well
    # as a value object. Move them to free functions (e.g. data_lake_common/
    # lineage_utils.py) in a follow-up so the config stays a plain data holder.
    def get_path(self, path: str, mode: PathMode = PathMode.FILE) -> Optional[str]:
        if mode is PathMode.DIRECTORY:
            return self._get_dir_path(path)
        return self._get_file_path(path)

    def _get_file_path(self, path: str) -> Optional[str]:
        for path_spec in self.path_specs:
            if path_spec.allowed(path):
                _, table_path = path_spec.extract_table_name_and_path(path)
                return table_path

        if self._drop_unmatched(path):
            return None

        if self.strip_urls and "/" in urlparse(path).path:
            return path.rsplit("/", 1)[0]
        return path

    def _get_dir_path(self, path: str) -> Optional[str]:
        # First matching spec wins, as in file mode.
        denied = False
        for path_spec in self.path_specs:
            result = path_spec.fold_dir_to_table(path)
            if result.table_path is not None:
                return result.table_path
            denied = denied or result.denied

        if denied:
            # An `exclude` / `tables_filter_pattern` rejection must never reach the
            # raw-path fallback below, which would emit lineage for the very prefix
            # the user filtered out — and unfolded, so it would not even match the
            # lake source's URN. A *different* spec is still allowed to claim the
            # prefix above, mirroring how `allowed()` lets a later spec claim a file
            # an earlier spec excluded.
            logger.debug(
                f"Skipping path {path}: excluded or filtered out by a path spec."
            )
            return None

        if self._drop_unmatched(path):
            return None

        # Match fold_dir_to_table's trailing-slash normalization so the fallback
        # URN aligns with the folded one. `strip_urls` is a file-mode fallback and
        # is deliberately not consulted here.
        return path.rstrip("/")

    def _drop_unmatched(self, path: str) -> bool:
        if self.ignore_non_path_spec_path and self.path_specs:
            logger.debug(f"Skipping path {path} as it does not match any path spec.")
            return True
        return False

    def get_urn_for_lineage(
        self, url: str, env: str, mode: PathMode = PathMode.FILE
    ) -> Optional[str]:
        path = self.get_path(url, mode=mode)
        if path is None:
            return None

        # A scheme with no bucket/key behind it would build a URN with an empty
        # dataset name, which the URN builders either emit malformed or reject.
        if "://" not in path or not path.split("://", 1)[1].strip("/"):
            logger.debug(f"No object path to build a lineage URN from: {url}")
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
            # Only the blob-host form is buildable; make_abs_urn raises on anything
            # else (e.g. ADLS Gen2's `dfs.core.windows.net`), so check before calling.
            abs_url = path.replace(_AZURE_SNOWFLAKE_PREFIX, "https://", 1)
            if not is_abs_uri(abs_url):
                logger.debug(f"Unsupported Azure host for lineage: {url}")
                return None
            return make_abs_urn(abs_url, env)
        if is_abs_uri(path):
            return make_abs_urn(path, env)
        logger.debug(f"Unsupported URL scheme for lineage: {url}")
        return None


# TODO: migrate all sources that expose `s3_lineage_config` (e.g. Redshift)
# to use DataLakeLineageProviderConfig / DataLakeLineageProviderConfigBase
# and remove these aliases once all callers are updated.
class S3LineageProviderConfig(DataLakeLineageProviderConfig):
    """Alias of :class:`DataLakeLineageProviderConfig` kept for back-compat with sources (e.g. Redshift) that exposed ``s3_lineage_config``."""


# TODO: migrate all sources that expose `s3_lineage_config` (e.g. Redshift)
# to use DataLakeLineageProviderConfigBase and remove this alias once all
# callers are updated.
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
