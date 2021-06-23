from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.aws_common import AwsSourceConfig


class SagemakerSourceConfig(AwsSourceConfig):
    @property
    def sagemaker_client(self):
        return self.get_client("samgemaker")


@dataclass
class SagemakerSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_table_dropped(self, table: str) -> None:
        self.filtered.append(table)


class GlueSource(Source):
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

    def get_all_feature_groups(self) -> List[Dict[str, Any]]:
        """
        List all feature groups in SageMaker.
        """

        feature_groups = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_feature_groups
        paginator = self.sagemaker_client.get_paginator("list_feature_groups")
        for page in paginator.paginate():
            feature_groups += page["FeatureGroupSummaries"]

        return feature_groups

    def get_feature_group_details(self, feature_group_name: str) -> Dict[str, Any]:
        """
        Get details of a feature group (including list of component features).
        """

        feature_group = self.sagemaker_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )

        next_token = feature_group.get("NextToken")

        # paginate over feature group features
        while next_token is not None:
            next_features = self.sagemaker_client.describe_feature_group(
                FeatureGroupName=feature_group_name, NextToken=next_token
            )
            feature_group["FeatureDefinitions"].append(
                next_features["FeatureDefinitions"]
            )
            next_token = feature_group.get("NextToken")

        return feature_group
