from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from pydantic.fields import Field

from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)


class SagemakerSourceConfig(
    AwsSourceConfig,
    StatefulIngestionConfigBase,
):
    extract_feature_groups: Optional[bool] = Field(
        default=True, description="Whether to extract feature groups."
    )
    extract_models: Optional[bool] = Field(
        default=True, description="Whether to extract models."
    )
    extract_jobs: Optional[Union[Dict[str, str], bool]] = Field(
        default=True, description="Whether to extract AutoML jobs."
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @property
    def sagemaker_client(self):
        return self.get_sagemaker_client()


@dataclass
class SagemakerSourceReport(StaleEntityRemovalSourceReport):
    feature_groups_scanned = 0
    features_scanned = 0
    endpoints_scanned = 0
    groups_scanned = 0
    models_scanned = 0
    jobs_scanned = 0
    jobs_processed = 0
    datasets_scanned = 0
    filtered: List[str] = field(default_factory=list)
    model_endpoint_lineage = 0
    model_group_lineage = 0

    def report_feature_group_scanned(self) -> None:
        self.feature_groups_scanned += 1

    def report_feature_scanned(self) -> None:
        self.features_scanned += 1

    def report_endpoint_scanned(self) -> None:
        self.endpoints_scanned += 1

    def report_group_scanned(self) -> None:
        self.groups_scanned += 1

    def report_model_scanned(self) -> None:
        self.models_scanned += 1

    def report_job_processed(self) -> None:
        self.jobs_processed += 1

    def report_job_scanned(self) -> None:
        self.jobs_scanned += 1

    def report_dataset_scanned(self) -> None:
        self.datasets_scanned += 1

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)
