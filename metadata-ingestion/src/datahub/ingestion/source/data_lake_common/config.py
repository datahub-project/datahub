from typing import List

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec


class PathSpecsConfigMixin(ConfigModel):
    path_specs: List[PathSpec] = Field(
        description="List of PathSpec. See [below](#path-spec) the details about PathSpec"
    )
