from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterable, List, Tuple

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_common import AwsSourceConfig
from datahub.ingestion.source.sagemaker_processors.feature_groups import (
    FeatureGroupProcessor,
)
from datahub.ingestion.source.sagemaker_processors.models import ModelProcessor


class SagemakerSourceConfig(AwsSourceConfig):
    @property
    def sagemaker_client(self):
        return self.get_client("sagemaker")


@dataclass
class SagemakerSourceReport(SourceReport):
    feature_groups_scanned = 0
    features_scanned = 0
    models_scanned = 0
    jobs_scanned = 0
    datasets_scanned = 0

    # TODO: report these
    def report_feature_group_scanned(self) -> None:
        self.feature_groups_scanned += 1

    def report_feature_scanned(self) -> None:
        self.features_scanned += 1

    def report_model_scanned(self) -> None:
        self.models_scanned += 1

    def report_job_scanned(self) -> None:
        self.jobs_scanned += 1

    def report_dataset_scanned(self) -> None:
        self.datasets_scanned += 1


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

        feature_group_processor = FeatureGroupProcessor(
            sagemaker_client=self.sagemaker_client, env=self.env, report=self.report
        )
        yield from feature_group_processor.get_workunits()

        model_processor = ModelProcessor(
            sagemaker_client=self.sagemaker_client, env=self.env, report=self.report
        )
        yield from model_processor.get_workunits()

    def get_report(self):
        return self.report

    def close(self):
        pass
