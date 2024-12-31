import copy
import re
from typing import Any, Dict, List, Optional, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetUsageStatisticsTransformer,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass

_USER_URN_PREFIX: str = "urn:li:corpuser:"


class PatternCleanupDatasetUsageUserConfig(ConfigModel):
    pattern_for_cleanup: List[str]


class PatternCleanupDatasetUsageUser(DatasetUsageStatisticsTransformer):
    """Transformer that clean the user URN for DatasetUsageStatistics aspect."""

    ctx: PipelineContext
    config: PatternCleanupDatasetUsageUserConfig

    def __init__(
        self,
        config: PatternCleanupDatasetUsageUserConfig,
        ctx: PipelineContext,
        **resolver_args: Dict[str, Any],
    ):
        super().__init__()
        self.config = config
        self.ctx = ctx
        self.resolver_args = resolver_args

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternCleanupDatasetUsageUser":
        config = PatternCleanupDatasetUsageUserConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_dataset_properties_aspect: DatasetUsageStatisticsClass = cast(
            DatasetUsageStatisticsClass, aspect
        )

        if in_dataset_properties_aspect.userCounts is not None:
            out_dataset_properties_aspect: DatasetUsageStatisticsClass = copy.deepcopy(
                in_dataset_properties_aspect
            )

            if out_dataset_properties_aspect.userCounts is not None:
                for user in out_dataset_properties_aspect.userCounts:
                    user_id: str = user.user.split(_USER_URN_PREFIX)[1]
                    for value in self.config.pattern_for_cleanup:
                        cleaned_user_id = re.sub(value, "", user_id)
                        user.user = _USER_URN_PREFIX + cleaned_user_id

                return cast(Aspect, out_dataset_properties_aspect)
            else:
                return cast(Aspect, out_dataset_properties_aspect)
        else:
            return cast(Aspect, in_dataset_properties_aspect)
