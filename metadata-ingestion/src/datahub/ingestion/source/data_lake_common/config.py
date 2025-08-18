"""
Common configuration classes for data lake operations.

This module provides reusable configuration classes that can be used across
different DataHub connectors for common data lake operations like file loading
from cloud storage and HTTP endpoints.
"""

from typing import List

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec


class PathSpecsConfigMixin(ConfigModel):
    """
    Mixin for configuration classes that use path specifications.

    This provides the common path_specs field that is used by data lake sources
    to specify which paths to ingest from.
    """

    path_specs: List[PathSpec] = Field(
        description="List of PathSpec. See [below](#path-spec) the details about PathSpec"
    )


# ExternalFileConnectionConfig removed - connectors can extend DataLakeConnectionConfig directly
