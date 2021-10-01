from typing import List

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)


class AddDatasetBrowsePathConfig(ConfigModel):
    path_templates: List[str]
    replace_existing: bool = False


class AddDatasetBrowsePathTransformer(DatasetTransformer):
    """Transformer that can be used to set browse paths through template replacement"""

    ctx: PipelineContext
    config: AddDatasetBrowsePathConfig

    def __init__(self, config: AddDatasetBrowsePathConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AddDatasetBrowsePathTransformer":
        config = AddDatasetBrowsePathConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce
        platform_part, dataset_fqdn, env = (
            mce.proposedSnapshot.urn.replace("urn:li:dataset:(", "")
            .replace(")", "")
            .split(",")
        )
        platform = platform_part.replace("urn:li:dataPlatform:", "")
        dataset = dataset_fqdn.replace(".", "/")

        browse_paths = builder.get_or_add_aspect(
            mce,
            BrowsePathsClass(
                paths=[],
            ),
        )

        if self.config.replace_existing:
            browse_paths.paths = []

        for template in self.config.path_templates:
            browse_path = (
                template.replace("PLATFORM", platform)
                .replace("DATASET_PARTS", dataset)
                .replace("ENV", env.lower())
            )

            browse_paths.paths.append(browse_path)

        return mce
