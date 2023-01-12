from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.sagemaker_processors.common import (
    SagemakerSourceReport,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import MLFeatureDataType
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLFeatureSnapshot,
    MLFeatureTableSnapshot,
    MLPrimaryKeySnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    MLFeaturePropertiesClass,
    MLFeatureTablePropertiesClass,
    MLPrimaryKeyPropertiesClass,
)

if TYPE_CHECKING:
    from mypy_boto3_sagemaker import SageMakerClient
    from mypy_boto3_sagemaker.type_defs import (
        DescribeFeatureGroupResponseTypeDef,
        FeatureDefinitionTypeDef,
        FeatureGroupSummaryTypeDef,
    )


@dataclass
class FeatureGroupProcessor:
    sagemaker_client: "SageMakerClient"
    env: str
    report: SagemakerSourceReport

    def get_all_feature_groups(self) -> List["FeatureGroupSummaryTypeDef"]:
        """
        List all feature groups in SageMaker.
        """

        feature_groups = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_feature_groups
        paginator = self.sagemaker_client.get_paginator("list_feature_groups")
        for page in paginator.paginate():
            feature_groups += page["FeatureGroupSummaries"]

        return feature_groups

    def get_feature_group_details(
        self, feature_group_name: str
    ) -> "DescribeFeatureGroupResponseTypeDef":
        """
        Get details of a feature group (including list of component features).
        """

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_feature_group
        feature_group = self.sagemaker_client.describe_feature_group(
            FeatureGroupName=feature_group_name
        )

        # use falsy fallback since AWS stubs require this to be a string in tests
        next_token = feature_group.get("NextToken", "")

        # paginate over feature group features
        while next_token:
            next_features = self.sagemaker_client.describe_feature_group(
                FeatureGroupName=feature_group_name, NextToken=next_token
            )
            feature_group["FeatureDefinitions"] += next_features["FeatureDefinitions"]
            next_token = feature_group.get("NextToken", "")

        return feature_group

    def get_feature_group_wu(
        self, feature_group_details: "DescribeFeatureGroupResponseTypeDef"
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
            aspects=[
                BrowsePathsClass(paths=["/sagemaker"]),
            ],
        )

        feature_group_snapshot.aspects.append(
            MLFeatureTablePropertiesClass(
                description=feature_group_details.get("Description"),
                # non-primary key features
                mlFeatures=[
                    builder.make_ml_feature_urn(
                        feature_group_name,
                        feature["FeatureName"],
                    )
                    for feature in feature_group_details["FeatureDefinitions"]
                    if feature["FeatureName"]
                    != feature_group_details["RecordIdentifierFeatureName"]
                ],
                mlPrimaryKeys=[
                    builder.make_ml_primary_key_urn(
                        feature_group_name,
                        feature_group_details["RecordIdentifierFeatureName"],
                    )
                ],
                # additional metadata
                customProperties={
                    "arn": feature_group_details["FeatureGroupArn"],
                    "creation_time": str(feature_group_details["CreationTime"]),
                    "status": feature_group_details["FeatureGroupStatus"],
                },
            )
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=feature_group_snapshot)
        return MetadataWorkUnit(id=feature_group_name, mce=mce)

    field_type_mappings = {
        "String": MLFeatureDataType.TEXT,
        "Integral": MLFeatureDataType.ORDINAL,
        "Fractional": MLFeatureDataType.CONTINUOUS,
    }

    def get_feature_type(self, aws_type: str, feature_name: str) -> str:
        mapped_type = self.field_type_mappings.get(aws_type)

        if mapped_type is None:
            self.report.report_warning(
                feature_name, f"unable to map type {aws_type} to metadata schema"
            )
            mapped_type = MLFeatureDataType.UNKNOWN

        return mapped_type

    def get_feature_wu(
        self,
        feature_group_details: "DescribeFeatureGroupResponseTypeDef",
        feature: "FeatureDefinitionTypeDef",
    ) -> MetadataWorkUnit:
        """
        Generate an MLFeature workunit for a SageMaker feature.

        Parameters
        ----------
            feature_group_details:
                ingested SageMaker feature group from get_feature_group_details()
            feature:
                ingested SageMaker feature
        """

        # if the feature acts as the record identifier, then we ingest it as an MLPrimaryKey
        # the RecordIdentifierFeatureName is guaranteed to exist as it's required on creation
        is_record_identifier = (
            feature_group_details["RecordIdentifierFeatureName"]
            == feature["FeatureName"]
        )

        feature_sources = []

        if "OfflineStoreConfig" in feature_group_details:
            # remove S3 prefix (s3://)
            s3_name = feature_group_details["OfflineStoreConfig"]["S3StorageConfig"][
                "S3Uri"
            ][5:]

            if s3_name.endswith("/"):
                s3_name = s3_name[:-1]

            feature_sources.append(
                builder.make_dataset_urn(
                    "s3",
                    s3_name,
                    self.env,
                )
            )

            if "DataCatalogConfig" in feature_group_details["OfflineStoreConfig"]:
                # if Glue catalog associated with offline store
                glue_database = feature_group_details["OfflineStoreConfig"][
                    "DataCatalogConfig"
                ]["Database"]
                glue_table = feature_group_details["OfflineStoreConfig"][
                    "DataCatalogConfig"
                ]["TableName"]

                full_table_name = f"{glue_database}.{glue_table}"

                self.report.report_warning(
                    full_table_name,
                    f"""Note: table {full_table_name} is an AWS Glue object.
                        To view full table metadata, run Glue ingestion
                        (see https://datahubproject.io/docs/metadata-ingestion/#aws-glue-glue)""",
                )

                feature_sources.append(
                    f"urn:li:dataset:(urn:li:dataPlatform:glue,{full_table_name},{self.env})"
                )

        # note that there's also an OnlineStoreConfig field, but this
        # lacks enough metadata to create a dataset
        # (only specifies the security config and whether it's enabled at all)

        # append feature name and type
        if is_record_identifier:
            primary_key_snapshot: MLPrimaryKeySnapshot = MLPrimaryKeySnapshot(
                urn=builder.make_ml_primary_key_urn(
                    feature_group_details["FeatureGroupName"],
                    feature["FeatureName"],
                ),
                aspects=[
                    MLPrimaryKeyPropertiesClass(
                        dataType=self.get_feature_type(
                            feature["FeatureType"], feature["FeatureName"]
                        ),
                        sources=feature_sources,
                    ),
                ],
            )

            # make the MCE and workunit
            mce = MetadataChangeEvent(proposedSnapshot=primary_key_snapshot)
        else:
            # create snapshot instance for the feature
            feature_snapshot: MLFeatureSnapshot = MLFeatureSnapshot(
                urn=builder.make_ml_feature_urn(
                    feature_group_details["FeatureGroupName"],
                    feature["FeatureName"],
                ),
                aspects=[
                    MLFeaturePropertiesClass(
                        dataType=self.get_feature_type(
                            feature["FeatureType"], feature["FeatureName"]
                        ),
                        sources=feature_sources,
                    )
                ],
            )

            # make the MCE and workunit
            mce = MetadataChangeEvent(proposedSnapshot=feature_snapshot)

        return MetadataWorkUnit(
            id=f'{feature_group_details["FeatureGroupName"]}-{feature["FeatureName"]}',
            mce=mce,
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        feature_groups = self.get_all_feature_groups()

        for feature_group in feature_groups:
            feature_group_details = self.get_feature_group_details(
                feature_group["FeatureGroupName"]
            )

            for feature in feature_group_details["FeatureDefinitions"]:
                self.report.report_feature_scanned()
                wu = self.get_feature_wu(feature_group_details, feature)
                self.report.report_workunit(wu)
                yield wu
            self.report.report_feature_group_scanned()
            wu = self.get_feature_group_wu(feature_group_details)
            self.report.report_workunit(wu)
            yield wu
