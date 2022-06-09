# see https://datahubproject.io/docs/metadata-ingestion/transformers for original tutorial
import json
from typing import List, Optional

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.add_dataset_ownership import Semantics
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class AddCustomOwnershipConfig(ConfigModel):
    owners_json: str
    semantics: Semantics = Semantics.OVERWRITE


class AddCustomOwnership(BaseTransformer, SingleAspectTransformer):
    """Transformer that adds owners to datasets according to a callback function."""

    # context param to generate run metadata such as a run ID
    ctx: PipelineContext
    # as defined in the previous block
    config: AddCustomOwnershipConfig

    def __init__(self, config: AddCustomOwnershipConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

        with open(self.config.owners_json, "r") as f:
            raw_owner_urns = json.load(f)

        self.owners = [
            OwnerClass(owner=owner, type=OwnershipTypeClass.DATAOWNER)
            for owner in raw_owner_urns
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddCustomOwnership":
        config = AddCustomOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ["dataset"]

    def aspect_name(self) -> str:
        return "ownership"

    def transform_aspect(  # type: ignore
        self, entity_urn: str, aspect_name: str, aspect: Optional[OwnershipClass]
    ) -> Optional[OwnershipClass]:

        owners_to_add = self.owners
        assert aspect is None or isinstance(aspect, OwnershipClass)

        if owners_to_add:
            ownership = aspect or OwnershipClass(
                owners=[],
            )

            ownership.owners.extend(owners_to_add)

        return ownership
