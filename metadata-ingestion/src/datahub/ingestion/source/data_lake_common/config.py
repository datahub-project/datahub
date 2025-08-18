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
    path_specs: List[PathSpec] = Field(
        description="List of PathSpec. See [below](#path-spec) the details about PathSpec"
    )
