from typing import Callable, Iterable, List, Union

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    BrowsePathsClass, DatasetSnapshotClass, MetadataChangeEventClass
)
import re
import logging
logger = logging.getLogger(__name__)

class BrowsePathConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    remove_prefix: str


class BrowsePathTransform(Transformer):
    """Transformer that adds tags to datasets according to a callback function."""

    ctx: PipelineContext
    config: BrowsePathConfig

    def __init__(self, config: BrowsePathConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "BrowsePathTransform":
        config = BrowsePathConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce
        dataset_name = mce.proposedSnapshot.urn
        proposed_path = self._generate_path(dataset_name, self.config.remove_prefix)
        logger.info("The new proposed browsepath is {}".format(proposed_path))
        mce.proposedSnapshot.aspects.append(BrowsePathsClass(paths=[proposed_path]))
        return mce

    def _generate_path(self, name, prefix):
        pattern = f"urn:li:dataset:\(urn:li:dataPlatform:(.*),{prefix}\)"
        platform_schema_dataset = re.match(pattern, name).group(1)
        platform, dataset_name = platform_schema_dataset.split(",",1)[0], platform_schema_dataset.split(",",1)[1]
        return f"/{platform}/{dataset_name}"

#bleh, didn't need 4 classes at all. misread the design of add_dataset_tags.py
# class BrowsePathTransformerConfig(ConfigModel):
#     remove_prefix: str


# class SimpleBrowsePathTransform(BrowsePathTransform):
#     """Transformer that adds a specified set of tags to each dataset."""

#     def __init__(self, config: BrowsePathTransformerConfig, ctx: PipelineContext):
#         remove = config.remove_prefix

#         generic_config = BrowsePathConfig(
#             remove_prefix = remove
#         )
#         super().__init__(generic_config, ctx)

#     @classmethod
#     def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleBrowsePathTransform":
#         config = BrowsePathTransformerConfig.parse_obj(config_dict)
#         return cls(config, ctx)
