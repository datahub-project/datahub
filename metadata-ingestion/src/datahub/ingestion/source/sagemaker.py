from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterable, List

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.aws_common import AwsSourceConfig
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import MLFeatureDataType
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLFeatureSnapshot,
    MLFeatureTableSnapshot,
    MLPrimaryKeySnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    MLFeaturePropertiesClass,
    MLFeatureTablePropertiesClass,
    MLPrimaryKeyPropertiesClass,
)


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

    def get_feature_group_wu(
        self, feature_group_details: Dict[str, Any]
    ) -> MetadataWorkUnit:
        """
        Generate an MLFeatureTable workunit for a SageMaker feature group.

        Parameters
        ----------
            feature_group_details:
                ingested SageMaker feature group from get_feature_group_details()
        """

        feature_group_name = feature_group_details["FeatureGroupName"]

        feature_group_snapshot = MLFeatureTableSnapshot(
            urn=builder.make_ml_feature_table_urn("sagemaker", feature_group_name),
            aspects=[],
        )

        feature_group_snapshot.aspects.append(
            MLFeatureTablePropertiesClass(
                mlFeatures=[
                    builder.make_ml_feature_urn(
                        feature_group_name,
                        feature["FeatureName"],
                    )
                    for feature in feature_group_details["FeatureDefinitions"]
                ],
                mlPrimaryKeys=[
                    builder.make_ml_primary_key_urn(
                        feature_group_name,
                        feature_group_details["RecordIdentifierFeatureName"],
                    )
                ],
            )
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=feature_group_snapshot)
        return MetadataWorkUnit(id=feature_group_name, mce=mce)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        feature_groups = self.get_all_feature_groups()

        for feature_group in feature_groups:

            feature_group_details = self.get_feature_group_details(
                feature_group["FeatureGroupName"]
            )

            yield self.get_feature_group_wu(feature_group_details)

    def get_report(self):
        return self.report

    def close(self):
        pass
