from abc import ABC, abstractmethod
from typing import Dict, Type

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)


class AddDatasetPropertiesResolverBase(ABC):
    @abstractmethod
    def get_properties_to_add(self, current: DatasetSnapshotClass) -> Dict[str, str]:
        pass


class AddDatasetPropertiesConfig(ConfigModel):
    add_properties_resolver_class: Type[AddDatasetPropertiesResolverBase]

    class Config:
        arbitrary_types_allowed = True

    _resolve_properties_class = pydantic_resolve_key("add_properties_resolver_class")


class AddDatasetProperties(DatasetTransformer):
    """Transformer that adds properties to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetPropertiesConfig

    def __init__(self, config: AddDatasetPropertiesConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetProperties":
        config = AddDatasetPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        properties_to_add = (
            self.config.add_properties_resolver_class().get_properties_to_add(
                mce.proposedSnapshot
            )
        )
        if properties_to_add:
            properties = builder.get_or_add_aspect(
                mce, DatasetPropertiesClass(customProperties={})
            )
            properties.customProperties.update(properties_to_add)

        return mce
