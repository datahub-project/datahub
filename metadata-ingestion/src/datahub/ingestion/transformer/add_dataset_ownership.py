from enum import Enum
from typing import Callable, List, Optional, Union

from pydantic import validator

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import (
    ConfigModel,
    ConfigurationError,
    KeyValuePattern,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetOwnershipTransformer,
)
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class Semantics(Enum):
    """Describes semantics for ownership changes"""

    OVERWRITE = "OVERWRITE"  # Apply changes blindly
    PATCH = "PATCH"  # Only apply differences from what exists already on the server


class AddDatasetOwnershipConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    get_owners_to_add: Union[
        Callable[[DatasetSnapshotClass], List[OwnerClass]],
        Callable[[DatasetSnapshotClass], List[OwnerClass]],
    ]
    default_actor: str = builder.make_user_urn("etl")
    semantics: Semantics = Semantics.OVERWRITE

    _resolve_owner_fn = pydantic_resolve_key("get_owners_to_add")

    @validator("semantics", pre=True)
    def ensure_semantics_is_upper_case(cls, v):
        if isinstance(v, str):
            return v.upper()
        return v


class AddDatasetOwnership(DatasetOwnershipTransformer):
    """Transformer that adds owners to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetOwnershipConfig

    def __init__(self, config: AddDatasetOwnershipConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        if self.config.semantics == Semantics.PATCH and self.ctx.graph is None:
            raise ConfigurationError(
                "With PATCH semantics, AddDatasetOwnership requires a datahub_api to connect to. Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe"
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetOwnership":
        config = AddDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def get_ownership_to_set(
        graph: DataHubGraph, urn: str, mce_ownership: Optional[OwnershipClass]
    ) -> Optional[OwnershipClass]:
        if not mce_ownership or not mce_ownership.owners:
            # nothing to add, no need to consult server
            return None
        assert mce_ownership
        server_ownership = graph.get_ownership(entity_urn=urn)
        if server_ownership:
            # compute patch
            # we only include owners who are not present in the server ownership
            # if owner ids match, but the ownership type differs, we prefer the transformers opinion
            owners_to_add: List[OwnerClass] = []
            needs_update = False
            server_owner_ids = [o.owner for o in server_ownership.owners]
            for owner in mce_ownership.owners:
                if owner.owner not in server_owner_ids:
                    owners_to_add.append(owner)
                else:
                    # we need to check if the type matches, and if it doesn't, update it
                    for server_owner in server_ownership.owners:
                        if (
                            owner.owner == server_owner.owner
                            and owner.type != server_owner.type
                        ):
                            server_owner.type = owner.type
                            needs_update = True

            if owners_to_add or needs_update:
                mce_ownership.owners = server_ownership.owners + owners_to_add
                return mce_ownership
            else:
                return None
        else:
            return mce_ownership

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        assert isinstance(mce.proposedSnapshot, DatasetSnapshotClass)
        owners_to_add = self.config.get_owners_to_add(mce.proposedSnapshot)
        if owners_to_add:
            ownership = builder.get_or_add_aspect(
                mce,
                OwnershipClass(
                    owners=[],
                ),
            )
            ownership.owners.extend(owners_to_add)

            if self.config.semantics == Semantics.PATCH:
                assert self.ctx.graph
                patch_ownership = AddDatasetOwnership.get_ownership_to_set(
                    self.ctx.graph, mce.proposedSnapshot.urn, ownership
                )
                builder.set_aspect(
                    mce, aspect=patch_ownership, aspect_type=OwnershipClass
                )
        return mce


class DatasetOwnershipBaseConfig(ConfigModel):
    ownership_type: Optional[str] = OwnershipTypeClass.DATAOWNER


class SimpleDatasetOwnershipConfig(DatasetOwnershipBaseConfig):
    owner_urns: List[str]
    default_actor: str = builder.make_user_urn("etl")
    semantics: Semantics = Semantics.OVERWRITE

    @validator("semantics", pre=True)
    def upper_case_semantics(cls, v):
        if isinstance(v, str):
            return v.upper()
        return v


class SimpleAddDatasetOwnership(AddDatasetOwnership):
    """Transformer that adds a specified set of owners to each dataset."""

    def __init__(self, config: SimpleDatasetOwnershipConfig, ctx: PipelineContext):
        ownership_type = builder.validate_ownership_type(config.ownership_type)
        owners = [
            OwnerClass(
                owner=owner,
                type=ownership_type,
            )
            for owner in config.owner_urns
        ]

        generic_config = AddDatasetOwnershipConfig(
            get_owners_to_add=lambda _: owners,
            default_actor=config.default_actor,
            semantics=config.semantics,
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

    def getOwners(
        self,
        key: str,
        owner_pattern: KeyValuePattern,
        ownership_type: Optional[str] = None,
    ) -> List[OwnerClass]:
        owners = [
            OwnerClass(
                owner=owner,
                type=builder.validate_ownership_type(ownership_type),
            )
            for owner in owner_pattern.value(key)
        ]
        return owners

    def __init__(self, config: PatternDatasetOwnershipConfig, ctx: PipelineContext):
        ownership_type = builder.validate_ownership_type(config.ownership_type)
        owner_pattern = config.owner_pattern
        generic_config = AddDatasetOwnershipConfig(
            get_owners_to_add=lambda _: [
                OwnerClass(
                    owner=owner,
                    type=ownership_type,
                )
                for owner in owner_pattern.value(_.urn)
            ],
            default_actor=config.default_actor,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetOwnership":
        config = PatternDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)
