import time
from typing import Optional, cast, Set

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetStatusTransformer
from datahub.metadata._schema_classes import OwnershipSourceClass, OwnershipSourceTypeClass, AuditStampClass
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass
)


class PatternCleanUpOwnershipConfig(ConfigModel):
    removed: bool


class PatternCleanUpOwnership(DatasetStatusTransformer):
    """Transformer that marks status of each dataset."""

    ctx: PipelineContext
    config: PatternCleanUpOwnershipConfig

    def __init__(self, config: PatternCleanUpOwnershipConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "PatternCleanUpOwnership":
        config = PatternCleanUpOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _get_current_owner_urns(self, entity_urn: str) -> Set[str]:
        current_ownership = self.ctx.graph.get_ownership(entity_urn=entity_urn)
        print(f"current_ownership = {current_ownership}")
        current_owner_urns: Set[str] = set(
            [owner.owner for owner in current_ownership.owners]
        )
        return current_owner_urns

    @staticmethod
    def _merge_with_server_ownership(
            graph: DataHubGraph, urn: str, mce_ownership: Optional[OwnershipClass]
    ) -> Optional[OwnershipClass]:
        if not mce_ownership or not mce_ownership.owners:
            print(f"returning None")
            # If there are no owners to add, we don't need to patch anything.
            return None

        # Merge the transformed ownership with existing server ownership.
        # The transformed ownership takes precedence, which may change the ownership type.

        server_ownership = graph.get_ownership(entity_urn=urn)
        print(f"server_ownership {server_ownership}")

        if server_ownership:
            owners = {owner.owner: owner for owner in server_ownership.owners}
            owners.update({owner.owner: owner for owner in mce_ownership.owners})
            mce_ownership.owners = list(owners.values())

        return mce_ownership

    def transform_aspect(
            self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        print(f"entity_urn = {entity_urn}")
        current_owner_urns = self._get_current_owner_urns(entity_urn)
        print(f"current_owner_urns = {current_owner_urns}")

        in_ownership_aspect: Optional[OwnershipClass] = cast(OwnershipClass, aspect)
        cleaned_owner_urns = ['urn:li:corpuser:mayuri.nehate@gslab1.com']

        ownership_type, ownership_type_urn = builder.validate_ownership_type(OwnershipTypeClass.DATAOWNER)

        owners = [
            OwnerClass(
                owner=owner,
                type=ownership_type,
                typeUrn=ownership_type_urn,
            )
            for owner in cleaned_owner_urns
        ]
        print(f"owners = {owners}")

        # out_ownership_aspect: OwnershipClass = OwnershipClass(
        #     owners=[],
        #     lastModified=None,
        # )

        out_ownership_aspect: OwnershipClass = OwnershipClass(
            owners=[
                OwnerClass(
                    owner='urn:li:corpuser:mayuri.nehate@gslab1.com',
                    type=OwnershipTypeClass.DATAOWNER,
                    typeUrn=ownership_type_urn,
                )
            ],
        )

        print(f"out_ownership_aspect = {out_ownership_aspect}")
        # out_ownership_aspect.owners.extend(owners)
        # print(f"out_ownership_aspect = {out_ownership_aspect}")

        # testdata = self._merge_with_server_ownership(
        #     self.ctx.graph, entity_urn, out_ownership_aspect
        # )
        # print(f"testdata = {testdata}")

        return cast(
            Optional[builder.Aspect], out_ownership_aspect
        )
