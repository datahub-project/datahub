import re
from typing import List, Optional, Set, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetOwnershipTransformer,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

_USER_URN_PREFIX: str = "urn:li:corpuser:"


class PatternCleanUpOwnershipConfig(ConfigModel):
    pattern_for_cleanup: List[str]


class PatternCleanUpOwnership(DatasetOwnershipTransformer):
    """Transformer that clean the ownership URN."""

    ctx: PipelineContext
    config: PatternCleanUpOwnershipConfig

    def __init__(self, config: PatternCleanUpOwnershipConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternCleanUpOwnership":
        config = PatternCleanUpOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _get_current_owner_urns(self, entity_urn: str) -> Set[str]:
        if self.ctx.graph is not None:
            current_ownership = self.ctx.graph.get_ownership(entity_urn=entity_urn)
            if current_ownership is not None:
                current_owner_urns: Set[str] = set(
                    [owner.owner for owner in current_ownership.owners]
                )
                return current_owner_urns
            else:
                return set()
        else:
            return set()

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        # get current owner URNs from the graph
        current_owner_urns = self._get_current_owner_urns(entity_urn)

        # clean all the owners based on the parameters received from config
        cleaned_owner_urns: List[str] = []
        for owner_urn in current_owner_urns:
            user_id: str = owner_urn.split(_USER_URN_PREFIX)[1]
            for value in self.config.pattern_for_cleanup:
                user_id = re.sub(value, "", user_id)

            cleaned_owner_urns.append(_USER_URN_PREFIX + user_id)

        ownership_type, ownership_type_urn = builder.validate_ownership_type(
            OwnershipTypeClass.DATAOWNER
        )
        owners = [
            OwnerClass(
                owner=owner,
                type=ownership_type,
                typeUrn=ownership_type_urn,
            )
            for owner in cleaned_owner_urns
        ]

        out_ownership_aspect: OwnershipClass = OwnershipClass(
            owners=[],
            lastModified=None,
        )

        # generate the ownership aspect for the cleaned users
        out_ownership_aspect.owners.extend(owners)
        return cast(Optional[builder.Aspect], out_ownership_aspect)
