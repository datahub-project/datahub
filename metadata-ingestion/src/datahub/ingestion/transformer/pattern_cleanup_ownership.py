import logging
import re
from typing import List, Optional, Set, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import OwnershipTransformer
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn
from datahub.utilities.urns._urn_base import Urn
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)


class PatternCleanUpOwnershipConfig(ConfigModel):
    pattern_for_cleanup: List[str]


class PatternCleanUpOwnership(OwnershipTransformer):
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
                current_owner_urns: Set[str] = {
                    owner.owner for owner in current_ownership.owners
                }
                return current_owner_urns
            else:
                return set()
        else:
            return set()

    def _process_owner(self, name: str) -> str:
        for value in self.config.pattern_for_cleanup:
            name = re.sub(value, "", name)
        return name

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        # get current owner URNs from the graph
        current_owner_urns = self._get_current_owner_urns(entity_urn)

        # clean all the owners based on the parameters received from config
        cleaned_owner_urns: List[str] = []
        for owner_urn in current_owner_urns:
            username = ""
            try:
                owner: Urn = Urn.from_string(owner_urn)
                if isinstance(owner, CorpUserUrn):
                    username = str(CorpUserUrn(self._process_owner(owner.username)))
                elif isinstance(owner, CorpGroupUrn):
                    username = str(CorpGroupUrn(self._process_owner(owner.name)))
                else:
                    logger.warning(f"{owner_urn} is not a supported owner type.")
                    username = owner_urn
            except InvalidUrnError:
                logger.warning(f"Could not parse {owner_urn} from {entity_urn}")
                username = owner_urn
            cleaned_owner_urns.append(username)

        ownership_type, ownership_type_urn = builder.validate_ownership_type(
            OwnershipTypeClass.TECHNICAL_OWNER
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
