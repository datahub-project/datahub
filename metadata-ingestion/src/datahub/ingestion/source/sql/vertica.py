
from typing import  Dict, Optional, List, Tuple
from datahub.ingestion.source.vertica.common import VerticaSQLAlchemySource , VerticaConfig 


from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)



@platform_name("Vertica")
@config_class(VerticaConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default, can be disabled via configuration `include_view_lineage` and `include_projection_lineage`")
@capability(SourceCapability.DELETION_DETECTION, "Optionally enabled via `stateful_ingestion.remove_stale_metadata`", supported=True)
class VerticaSource(VerticaSQLAlchemySource):
    def __init__(self, config: VerticaConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, "vertica__2")
        self.view_lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self.Projection_lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)