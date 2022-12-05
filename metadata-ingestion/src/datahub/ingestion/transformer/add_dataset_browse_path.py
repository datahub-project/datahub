from typing import List, Optional, cast

from datahub.configuration.common import (
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetBrowsePathsTransformer,
)
from datahub.metadata.schema_classes import BrowsePathsClass


class AddDatasetBrowsePathConfig(TransformerSemanticsConfigModel):
    path_templates: List[str]


class AddDatasetBrowsePathTransformer(DatasetBrowsePathsTransformer):
    """Transformer that can be used to set browse paths through template replacement"""

    ctx: PipelineContext
    config: AddDatasetBrowsePathConfig

    def __init__(self, config: AddDatasetBrowsePathConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AddDatasetBrowsePathTransformer":
        config = AddDatasetBrowsePathConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _merge_with_server_browse_paths(
        graph: DataHubGraph, urn: str, mce_browse_paths: Optional[BrowsePathsClass]
    ) -> Optional[BrowsePathsClass]:
        if not mce_browse_paths or not mce_browse_paths.paths:
            # nothing to add, no need to consult server
            return None

        server_browse_paths = graph.get_browse_path(entity_urn=urn)
        if server_browse_paths:
            # compute patch
            # we only include domain who are not present in the server domain list
            paths_to_add: List[str] = []
            for path in mce_browse_paths.paths:
                if path not in server_browse_paths.paths:
                    paths_to_add.append(path)
            # Lets patch
            mce_browse_paths.paths = []
            mce_browse_paths.paths.extend(server_browse_paths.paths)
            mce_browse_paths.paths.extend(paths_to_add)

        return mce_browse_paths

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        platform_part, dataset_fqdn, env = (
            entity_urn.replace("urn:li:dataset:(", "").replace(")", "").split(",")
        )

        platform = platform_part.replace("urn:li:dataPlatform:", "")
        dataset = dataset_fqdn.replace(".", "/")

        browse_paths = BrowsePathsClass(paths=[])
        if aspect is not None and self.config.replace_existing is False:
            browse_paths.paths.extend(aspect.paths)  # type: ignore[attr-defined]

        for template in self.config.path_templates:
            browse_path = (
                template.replace("PLATFORM", platform)
                .replace("DATASET_PARTS", dataset)
                .replace("ENV", env.lower())
            )
            browse_paths.paths.append(browse_path)

        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            return cast(
                Optional[Aspect],
                AddDatasetBrowsePathTransformer._merge_with_server_browse_paths(
                    self.ctx.graph, entity_urn, browse_paths
                ),
            )
        else:
            return cast(Aspect, browse_paths)
