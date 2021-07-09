from typing import Iterable

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sagemaker_processors.common import (
    SagemakerSourceConfig,
    SagemakerSourceReport,
)
from datahub.ingestion.source.sagemaker_processors.feature_groups import (
    FeatureGroupProcessor,
)
from datahub.ingestion.source.sagemaker_processors.jobs import JobProcessor
from datahub.ingestion.source.sagemaker_processors.models import ModelProcessor


class SagemakerSource(Source):
    source_config: SagemakerSourceConfig
    report = SagemakerSourceReport()

    def __init__(self, config: SagemakerSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = SagemakerSourceReport()
        self.sagemaker_client = config.sagemaker_client
        self.env = config.env

    @classmethod
    def create(cls, config_dict, ctx):
        config = SagemakerSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        # extract feature groups if specified
        if self.source_config.extract_feature_groups:

            feature_group_processor = FeatureGroupProcessor(
                sagemaker_client=self.sagemaker_client, env=self.env, report=self.report
            )
            yield from feature_group_processor.get_workunits()

        # extract models if specified
        if self.source_config.extract_models:

            model_processor = ModelProcessor(
                sagemaker_client=self.sagemaker_client, env=self.env, report=self.report
            )
            yield from model_processor.get_workunits()

        # extract jobs if specified
        if self.source_config.extract_jobs is not False:

            job_processor = JobProcessor(
                sagemaker_client=self.sagemaker_client,
                env=self.env,
                report=self.report,
                job_type_filter=self.source_config.extract_jobs,
            )
            yield from job_processor.get_workunits()

    def get_report(self):
        return self.report

    def close(self):
        pass
