from typing import Callable, List, Optional, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    ConfigurationError,
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetOwnershipTransformer,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class AddDatasetOwnershipConfig(TransformerSemanticsConfigModel):
    get_owners_to_add: Callable[[str], List[OwnerClass]]
    default_actor: str = builder.make_user_urn("etl")

    _resolve_owner_fn = pydantic_resolve_key("get_owners_to_add")


class AddDatasetOwnership(DatasetOwnershipTransformer):
    """Transformer that adds owners to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetOwnershipConfig

    def __init__(self, config: AddDatasetOwnershipConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        if (
            self.config.semantics == TransformerSemantics.PATCH
            and self.ctx.graph is None
        ):
            raise ConfigurationError(
                "With PATCH TransformerSemantics, AddDatasetOwnership requires a datahub_api to connect to. Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe"
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetOwnership":
        config = AddDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _merge_with_server_ownership(
        graph: DataHubGraph, urn: str, mce_ownership: Optional[OwnershipClass]
    ) -> Optional[OwnershipClass]:
        if not mce_ownership or not mce_ownership.owners:
            # If there are no owners to add, we don't need to patch anything.
            return None

        # Merge the transformed ownership with existing server ownership.
        # The transformed ownership takes precedence, which may change the ownership type.

        server_ownership = graph.get_ownership(entity_urn=urn)
        if server_ownership:
            owners = {owner.owner: owner for owner in server_ownership.owners}
            owners.update({owner.owner: owner for owner in mce_ownership.owners})
            mce_ownership.owners = list(owners.values())

        return mce_ownership

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_ownership_aspect: Optional[OwnershipClass] = cast(OwnershipClass, aspect)
        out_ownership_aspect: OwnershipClass = OwnershipClass(
            owners=[],
            lastModified=(
                in_ownership_aspect.lastModified
                if in_ownership_aspect is not None
                else None
            ),
        )

        # Check if user want to keep existing ownerships
        if in_ownership_aspect is not None and self.config.replace_existing is False:
            out_ownership_aspect.owners.extend(in_ownership_aspect.owners)

        owners_to_add = self.config.get_owners_to_add(entity_urn)
        if owners_to_add is not None:
            out_ownership_aspect.owners.extend(owners_to_add)

        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            return cast(
                Optional[Aspect],
                self._merge_with_server_ownership(
                    self.ctx.graph, entity_urn, out_ownership_aspect
                ),
            )
        else:
            return cast(Aspect, out_ownership_aspect)


class DatasetOwnershipBaseConfig(TransformerSemanticsConfigModel):
    ownership_type: str = OwnershipTypeClass.DATAOWNER


class SimpleDatasetOwnershipConfig(DatasetOwnershipBaseConfig):
    owner_urns: List[str]
    default_actor: str = builder.make_user_urn("etl")


class SimpleAddDatasetOwnership(AddDatasetOwnership):
    """Transformer that adds a specified set of owners to each dataset."""

    def __init__(self, config: SimpleDatasetOwnershipConfig, ctx: PipelineContext):
        ownership_type, ownership_type_urn = builder.validate_ownership_type(
            config.ownership_type
        )
        owners = [
            OwnerClass(
                owner=owner,
                type=ownership_type,
                typeUrn=ownership_type_urn,
            )
            for owner in config.owner_urns
        ]

        generic_config = AddDatasetOwnershipConfig(
            get_owners_to_add=lambda _: owners,
            default_actor=config.default_actor,
            semantics=config.semantics,
            replace_existing=config.replace_existing,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetOwnership":
        config = SimpleDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternDatasetOwnershipConfig(DatasetOwnershipBaseConfig):
    owner_pattern: KeyValuePattern = KeyValuePattern.all()
    default_actor: str = builder.make_user_urn("etl")


class PatternAddDatasetOwnership(AddDatasetOwnership):
    """Transformer that adds a specified set of owners to each dataset."""

    def __init__(self, config: PatternDatasetOwnershipConfig, ctx: PipelineContext):
        owner_pattern = config.owner_pattern
        ownership_type, ownership_type_urn = builder.validate_ownership_type(
            config.ownership_type
        )
        generic_config = AddDatasetOwnershipConfig(
            get_owners_to_add=lambda urn: [
                OwnerClass(
                    owner=owner,
                    type=ownership_type,
                    typeUrn=ownership_type_urn,
                )
                for owner in owner_pattern.value(urn)
            ],
            default_actor=config.default_actor,
            semantics=config.semantics,
            replace_existing=config.replace_existing,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetOwnership":
        config = PatternDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)
