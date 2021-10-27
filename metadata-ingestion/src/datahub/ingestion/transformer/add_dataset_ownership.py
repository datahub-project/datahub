from typing import Callable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel, KeyValuePattern
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class AddDatasetOwnershipConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    get_owners_to_add: Union[
        Callable[[DatasetSnapshotClass], List[OwnerClass]],
        Callable[[DatasetSnapshotClass], List[OwnerClass]],
    ]
    default_actor: str = builder.make_user_urn("etl")

    _resolve_owner_fn = pydantic_resolve_key("get_owners_to_add")


class AddDatasetOwnership(DatasetTransformer):
    """Transformer that adds owners to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetOwnershipConfig

    def __init__(self, config: AddDatasetOwnershipConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetOwnership":
        config = AddDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce
        owners_to_add = self.config.get_owners_to_add(mce.proposedSnapshot)
        if owners_to_add:
            ownership = builder.get_or_add_aspect(
                mce,
                OwnershipClass(
                    owners=[],
                ),
            )
            ownership.owners.extend(owners_to_add)

        return mce


class DatasetOwnershipBaseConfig(ConfigModel):
    ownership_type: Optional[str] = OwnershipTypeClass.DATAOWNER


class SimpleDatasetOwnershipConfig(DatasetOwnershipBaseConfig):
    owner_urns: List[str]
    default_actor: str = builder.make_user_urn("etl")


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
