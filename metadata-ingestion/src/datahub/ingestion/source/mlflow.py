from dataclasses import dataclass, field
from typing import Iterable, List

import mlflow.sklearn

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import MLModelSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import MLModelPropertiesClass


class MlFlowConfig(ConfigModel):
    tracking_uri: str

    experiment_pattern: AllowDenyPattern = AllowDenyPattern(deny=["Default"])


@dataclass
class MlFlowSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


class MlFlowSource(Source):
    config: MlFlowConfig

    def __init__(self, config: MlFlowConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.mlflow_client = mlflow.tracking.MlflowClient(tracking_uri=self.config.tracking_uri)
        self.report = MlFlowSourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = MlFlowConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        platform = 'mlflow'
        env = 'PROD'

        experiment_names = self.get_mlflow_objects(self.mlflow_client)

        for experiment_name in experiment_names:
            if not self.config.experiment_pattern.allowed(experiment_name):
                self.report.report_dropped(experiment_name)
                continue

            mlmodel_snapshot = MLModelSnapshot(
                urn=f"urn:li:mlModel:(urn:li:dataPlatform:{platform},{experiment_name},{env})",
                aspects=[])

            mlmodel_properties = MLModelPropertiesClass(
                tags=[],
                hyperParameters={},
                mlFeatures=[]
            )
            mlmodel_snapshot.aspects.append(mlmodel_properties)

            mce = MetadataChangeEvent(proposedSnapshot=mlmodel_snapshot)

            wu = MetadataWorkUnit(id=experiment_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    @staticmethod
    def get_mlflow_objects(mlflow_client: mlflow.tracking.MlflowClient) -> List[str]:
        experiment_list = mlflow_client.list_experiments()
        experiment_name_list = [experiment.name for experiment in experiment_list]
        return experiment_name_list

    def get_report(self) -> MlFlowSourceReport:
        return self.report
