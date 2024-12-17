import logging
from collections import defaultdict
from typing import TYPE_CHECKING, DefaultDict, Dict, Iterable, List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.sagemaker_processors.common import (
    SagemakerSourceConfig,
    SagemakerSourceReport,
)
from datahub.ingestion.source.aws.sagemaker_processors.feature_groups import (
    FeatureGroupProcessor,
)
from datahub.ingestion.source.aws.sagemaker_processors.jobs import (
    JobKey,
    JobProcessor,
    ModelJob,
)
from datahub.ingestion.source.aws.sagemaker_processors.lineage import LineageProcessor
from datahub.ingestion.source.aws.sagemaker_processors.models import ModelProcessor
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

if TYPE_CHECKING:
    from mypy_boto3_sagemaker import SageMakerClient

logger = logging.getLogger(__name__)


@platform_name("SageMaker")
@config_class(SagemakerSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class SagemakerSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Feature groups
    - Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)
    """

    platform = "sagemaker"
    source_config: SagemakerSourceConfig
    report = SagemakerSourceReport()

    def __init__(self, config: SagemakerSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = SagemakerSourceReport()
        self.sagemaker_client = config.sagemaker_client
        self.env = config.env
        self.client_factory = ClientFactory(config)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SagemakerSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info("Starting SageMaker ingestion...")
        # get common lineage graph
        lineage_processor = LineageProcessor(
            sagemaker_client=self.sagemaker_client, env=self.env, report=self.report
        )
        lineage = lineage_processor.get_lineage()

        # extract feature groups if specified
        if self.source_config.extract_feature_groups:
            logger.info("Extracting feature groups...")
            feature_group_processor = FeatureGroupProcessor(
                sagemaker_client=self.sagemaker_client, env=self.env, report=self.report
            )
            yield from feature_group_processor.get_workunits()

        model_image_to_jobs: DefaultDict[str, Dict[JobKey, ModelJob]] = defaultdict(
            dict
        )
        model_name_to_jobs: DefaultDict[str, Dict[JobKey, ModelJob]] = defaultdict(dict)

        # extract jobs if specified
        if self.source_config.extract_jobs is not False:
            logger.info("Extracting jobs...")
            job_processor = JobProcessor(
                sagemaker_client=self.client_factory.get_client,
                env=self.env,
                report=self.report,
                job_type_filter=self.source_config.extract_jobs,
                aws_region=self.sagemaker_client.meta.region_name,
            )
            yield from job_processor.get_workunits()

            model_image_to_jobs = job_processor.model_image_to_jobs
            model_name_to_jobs = job_processor.model_name_to_jobs

        # extract models if specified
        if self.source_config.extract_models:
            logger.info("Extracting models...")

            model_processor = ModelProcessor(
                sagemaker_client=self.sagemaker_client,
                env=self.env,
                report=self.report,
                model_image_to_jobs=model_image_to_jobs,
                model_name_to_jobs=model_name_to_jobs,
                lineage=lineage,
                aws_region=self.sagemaker_client.meta.region_name,
            )
            yield from model_processor.get_workunits()

    def get_report(self):
        return self.report


class ClientFactory:
    def __init__(self, config: SagemakerSourceConfig):
        self.config = config
        self._cached_client = self.config.sagemaker_client

    def get_client(self) -> "SageMakerClient":
        if self.config.allowed_cred_refresh():
            # Always fetch the client dynamically with auto-refresh logic
            return self.config.sagemaker_client
        return self._cached_client
