from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig


class AthenaManagedByGlueSource(GlueSource):
    def __init__(self, config: GlueSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.extract_transforms = False

    def get_urn_platform(self):
        return "urn:li:dataPlatform:athena"
