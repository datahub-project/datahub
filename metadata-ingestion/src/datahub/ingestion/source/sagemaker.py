from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Dict, Iterable, List

import datahub.emitter.mce_builder as builder
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

SAGEMAKER_JOBS = {
    "auto_ml": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_auto_ml_jobs
        "list_command": "list_auto_ml_jobs",
        "list_key": "AutoMLJobSummaries",
        "list_name_key": "LabelingJobName",
        "list_arn_key": "LabelingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_auto_ml_job
        "describe_command": "describe_auto_ml_job",
        "describe_name_key": "AutoMLJobName",
    },
    "compilation": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_compilation_jobs
        "list_command": "list_compilation_jobs",
        "list_key": "CompilationJobSummaries",
        "list_name_key": "CompilationJobName",
        "list_arn_key": "CompilationJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_compilation_job
        "describe_command": "describe_compilation_job",
        "describe_name_key": "CompilationJobName",
    },
    "edge_packaging": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_edge_packaging_jobs
        "list_command": "list_edge_packaging_jobs",
        "list_key": "EdgePackagingJobSummaries",
        "list_name_key": "EdgePackagingJobArn",
        "list_arn_key": "EdgePackagingJobName",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_edge_packaging_job
        "describe_command": "describe_edge_packaging_job",
        "describe_name_key": "EdgePackagingJobName",
    },
    "hyper_parameter_tuning": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_hyper_parameter_tuning_jobs
        "list_command": "list_hyper_parameter_tuning_jobs",
        "list_key": "HyperParameterTuningJobSummaries",
        "list_name_key": "HyperParameterTuningJobName",
        "list_arn_key": "HyperParameterTuningJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_hyper_parameter_tuning_job
        "describe_command": "describe_hyper_parameter_tuning_job",
        "describe_name_key": "HyperParameterTuningJobName",
    },
    "labeling": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_labeling_jobs
        "list_command": "list_labeling_jobs",
        "list_key": "LabelingJobSummaryList",
        "list_name_key": "LabelingJobName",
        "list_arn_key": "LabelingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_labeling_job
        "describe_command": "describe_labeling_job",
        "describe_name_key": "LabelingJobName",
    },
    "processing": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_processing_jobs
        "list_command": "list_processing_jobs",
        "list_key": "ProcessingJobSummaries",
        "list_name_key": "ProcessingJobName",
        "list_arn_key": "ProcessingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_processing_job
        "describe_command": "describe_processing_job",
        "describe_name_key": "ProcessingJobName",
    },
    "training": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs
        "list_command": "list_training_jobs",
        "list_key": "TrainingJobSummaries",
        "list_name_key": "TrainingJobName",
        "list_arn_key": "TrainingJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job
        "describe_command": "describe_training_job",
        "describe_name_key": "TrainingJobName",
    },
    "transform": {
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_transform_jobs
        "list_command": "list_transform_jobs",
        "list_key": "TransformJobSummaries",
        "list_name_key": "TransformJobName",
        "list_arn_key": "TransformJobArn",
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job
        "describe_command": "describe_transform_job",
        "describe_name_key": "TransformJobName",
    },
}


class SagemakerSourceConfig(AwsSourceConfig):
    @property
    def sagemaker_client(self):
        return self.get_client("sagemaker")


@dataclass
class SagemakerSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_table_dropped(self, table: str) -> None:
        self.filtered.append(table)


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

    def get_all_models(self) -> List[Dict[str, Any]]:
        """
        List all models in SageMaker.
        """

        models = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_models
        paginator = self.sagemaker_client.get_paginator("list_models")
        for page in paginator.paginate():
            models += page["Models"]

        return models

    def get_all_jobs(
        self,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, str], Dict[str, str]]:
        """
        List all jobs in SageMaker.
        """

        jobs = []

        # dictionaries for translating between type-specific job names and ARNs
        arn_to_name = {}
        name_to_arn = {}

        for job_type, job_spec in SAGEMAKER_JOBS.items():

            paginator = self.sagemaker_client.get_paginator(job_spec["list_command"])
            for page in paginator.paginate():
                page_jobs = page[job_spec["list_key"]]

                for job in page_jobs:
                    job_name = f'{job_type}:{job[job_spec["list_name_key"]]}'
                    job_arn = job[job_spec["list_name_arn"]]

                    arn_to_name[job_arn] = job_name
                    name_to_arn[job_name] = job_arn

                page_jobs = [{**job, "type": job_type} for job in page_jobs]

                jobs += page_jobs

        return jobs, arn_to_name, name_to_arn

    def get_job_details(
        self, job_name: str, describe_command: str, describe_name_key: str
    ) -> Dict[str, Any]:

        return getattr(self.sagemaker_client, describe_command)(
            **{describe_name_key: job_name}
        )

    def get_feature_group_details(self, feature_group_name: str) -> Dict[str, Any]:
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
            feature_group["FeatureDefinitions"].append(
                next_features["FeatureDefinitions"]
            )
            next_token = feature_group.get("NextToken", "")

        return feature_group

    def get_model_details(self, model_name: str) -> Dict[str, Any]:
        """
        Get details of a model.
        """

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_model
        return self.sagemaker_client.describe_model(ModelName=model_name)

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
        self, feature_group_details: Dict[str, Any], feature: Dict[str, Any]
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
                    self.source_config.env,
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
                    f"urn:li:dataset:(urn:li:dataPlatform:glue,{full_table_name},{self.source_config.env})"
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
                wu = self.get_feature_wu(feature_group_details, feature)
                self.report.report_workunit(wu)
                yield wu

            wu = self.get_feature_group_wu(feature_group_details)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
