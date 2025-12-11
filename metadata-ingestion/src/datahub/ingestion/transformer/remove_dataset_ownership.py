# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Optional, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import OwnershipTransformer
from datahub.metadata.schema_classes import OwnershipClass


class ClearDatasetOwnershipConfig(ConfigModel):
    pass


class SimpleRemoveDatasetOwnership(OwnershipTransformer):
    """Transformer that clears all owners on each dataset."""

    def __init__(self, config: ClearDatasetOwnershipConfig, ctx: PipelineContext):
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleRemoveDatasetOwnership":
        config = ClearDatasetOwnershipConfig.model_validate(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_ownership_aspect = cast(OwnershipClass, aspect)
        if in_ownership_aspect is None:
            return None

        in_ownership_aspect.owners = []
        return cast(Aspect, in_ownership_aspect)
