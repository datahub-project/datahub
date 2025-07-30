import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
)

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.sagemaker_processors.common import (
    SagemakerSourceReport,
)
from datahub.ingestion.source.aws.sagemaker_processors.jobs import (
    JobDirection,
    JobKey,
    ModelJob,
)
from datahub.ingestion.source.aws.sagemaker_processors.lineage import LineageInfo
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLModelDeploymentSnapshot,
    MLModelGroupSnapshot,
    MLModelSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DeploymentStatusClass,
    MLHyperParamClass,
    MLMetricClass,
    MLModelDeploymentPropertiesClass,
    MLModelGroupPropertiesClass,
    MLModelPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

if TYPE_CHECKING:
    from mypy_boto3_sagemaker import SageMakerClient
    from mypy_boto3_sagemaker.type_defs import (
        DescribeEndpointOutputTypeDef,
        DescribeModelOutputTypeDef,
        DescribeModelPackageGroupOutputTypeDef,
        EndpointSummaryTypeDef,
        ModelPackageGroupSummaryTypeDef,
        ModelSummaryTypeDef,
    )

ENDPOINT_STATUS_MAP: Dict[str, str] = {
    "OutOfService": DeploymentStatusClass.OUT_OF_SERVICE,
    "Creating": DeploymentStatusClass.CREATING,
    "Updating": DeploymentStatusClass.UPDATING,
    "SystemUpdating": DeploymentStatusClass.UPDATING,
    "RollingBack": DeploymentStatusClass.ROLLING_BACK,
    "InService": DeploymentStatusClass.IN_SERVICE,
    "Deleting": DeploymentStatusClass.DELETING,
    "Failed": DeploymentStatusClass.FAILED,
    "Unknown": DeploymentStatusClass.UNKNOWN,
}

logger = logging.getLogger(__name__)


@dataclass
class ModelProcessor:
    sagemaker_client: "SageMakerClient"
    env: str
    report: SagemakerSourceReport
    lineage: LineageInfo
    aws_region: str

    # map from model image file path to jobs referencing the model
    model_image_to_jobs: DefaultDict[str, Dict[JobKey, ModelJob]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    # map from model name to jobs referencing the model
    model_name_to_jobs: DefaultDict[str, Dict[JobKey, ModelJob]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    # map from model uri to model name
    model_uri_to_name: Dict[str, str] = field(default_factory=dict)
    # map from model image path to model name
    model_image_to_name: Dict[str, str] = field(default_factory=dict)

    group_arn_to_name: Dict[str, str] = field(default_factory=dict)

    def get_all_models(self) -> List["ModelSummaryTypeDef"]:
        """
        List all models in SageMaker.
        """

        models = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_models
        paginator = self.sagemaker_client.get_paginator("list_models")
        for page in paginator.paginate():
            models += page["Models"]

        return models

    def get_model_details(self, model_name: str) -> "DescribeModelOutputTypeDef":
        """
        Get details of a model.
        """

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_model
        return self.sagemaker_client.describe_model(ModelName=model_name)

    def get_all_groups(self) -> List["ModelPackageGroupSummaryTypeDef"]:
        """
        List all model groups in SageMaker.
        """
        groups = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_model_package_groups
        paginator = self.sagemaker_client.get_paginator("list_model_package_groups")
        for page in paginator.paginate():
            groups += page["ModelPackageGroupSummaryList"]

        return groups

    def get_group_details(
        self, group_name: str
    ) -> "DescribeModelPackageGroupOutputTypeDef":
        """
        Get details of a model group.
        """

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_model_package_group
        return self.sagemaker_client.describe_model_package_group(
            ModelPackageGroupName=group_name
        )

    def get_all_endpoints(self) -> List["EndpointSummaryTypeDef"]:
        endpoints = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_endpoints
        paginator = self.sagemaker_client.get_paginator("list_endpoints")

        for page in paginator.paginate():
            endpoints += page["Endpoints"]

        return endpoints

    def get_endpoint_details(
        self, endpoint_name: str
    ) -> "DescribeEndpointOutputTypeDef":
        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_endpoint
        return self.sagemaker_client.describe_endpoint(EndpointName=endpoint_name)

    def get_endpoint_status(
        self, endpoint_name: str, endpoint_arn: str, sagemaker_status: str
    ) -> str:
        endpoint_status = ENDPOINT_STATUS_MAP.get(sagemaker_status)

        if endpoint_status is None:
            self.report.report_warning(
                endpoint_arn,
                f"Unknown status for {endpoint_name} ({endpoint_arn}): {sagemaker_status}",
            )

            endpoint_status = DeploymentStatusClass.UNKNOWN

        return endpoint_status

    def get_endpoint_wu(
        self, endpoint_details: "DescribeEndpointOutputTypeDef"
    ) -> MetadataWorkUnit:
        """a
        Get a workunit for an endpoint.
        """

        # params to remove since we extract them
        redundant_fields = {"EndpointName", "CreationTime"}

        endpoint_snapshot = MLModelDeploymentSnapshot(
            urn=builder.make_ml_model_deployment_urn(
                "sagemaker", endpoint_details["EndpointName"], self.env
            ),
            aspects=[
                MLModelDeploymentPropertiesClass(
                    createdAt=int(
                        endpoint_details.get("CreationTime", datetime.now()).timestamp()
                        * 1000
                    ),
                    status=self.get_endpoint_status(
                        endpoint_details["EndpointArn"],
                        endpoint_details["EndpointName"],
                        endpoint_details.get("EndpointStatus", "Unknown"),
                    ),
                    externalUrl=f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/endpoints/{endpoint_details['EndpointName']}",
                    customProperties={
                        key: str(value)
                        for key, value in endpoint_details.items()
                        if key not in redundant_fields
                    },
                )
            ],
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=endpoint_snapshot)

        return MetadataWorkUnit(
            id=f"{endpoint_details['EndpointName']}",
            mce=mce,
        )

    def get_model_endpoints(
        self,
        model_details: "DescribeModelOutputTypeDef",
        endpoint_arn_to_name: Dict[str, str],
        model_image: Optional[str],
        model_uri: Optional[str],
    ) -> List[str]:
        """
        Get all endpoints for a model.
        """

        model_endpoints = set()

        # get endpoints and groups by model image
        if model_image is not None:
            model_endpoints |= self.lineage.model_image_endpoints[model_image]
            self.model_image_to_name[model_image] = model_details["ModelName"]

        # get endpoints and groups by model uri
        if model_uri is not None:
            model_endpoints |= self.lineage.model_uri_endpoints[model_uri]
            self.model_uri_to_name[model_uri] = model_details["ModelName"]

        # sort endpoints and groups for consistency
        model_endpoints_sorted = sorted(
            [x for x in model_endpoints if x in endpoint_arn_to_name]
        )

        return model_endpoints_sorted

    def get_group_wu(
        self, group_details: "DescribeModelPackageGroupOutputTypeDef"
    ) -> MetadataWorkUnit:
        """
        Get a workunit for a model group.
        """

        # params to remove since we extract them
        redundant_fields = {"ModelPackageGroupName", "CreationTime"}

        group_arn = group_details["ModelPackageGroupArn"]
        group_name = group_details["ModelPackageGroupName"]

        self.group_arn_to_name[group_arn] = group_name

        owners = []

        if group_details.get("CreatedBy", {}).get("UserProfileName") is not None:
            owners.append(
                OwnerClass(
                    owner=f"urn:li:corpuser:{group_details['CreatedBy']['UserProfileName']}",
                    type=OwnershipTypeClass.DATAOWNER,
                )
            )

        group_snapshot = MLModelGroupSnapshot(
            urn=builder.make_ml_model_group_urn("sagemaker", group_name, self.env),
            aspects=[
                MLModelGroupPropertiesClass(
                    createdAt=int(
                        group_details.get("CreationTime", datetime.now()).timestamp()
                        * 1000
                    ),
                    description=group_details.get("ModelPackageGroupDescription"),
                    customProperties={
                        key: str(value)
                        for key, value in group_details.items()
                        if key not in redundant_fields
                    },
                ),
                OwnershipClass(owners),
                BrowsePathsClass(paths=["/sagemaker"]),
            ],
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=group_snapshot)

        return MetadataWorkUnit(id=group_name, mce=mce)

    def match_model_jobs(
        self, model_details: "DescribeModelOutputTypeDef"
    ) -> Tuple[Set[str], Set[str], List[MLHyperParamClass], List[MLMetricClass]]:
        model_training_jobs: Set[str] = set()
        model_downstream_jobs: Set[str] = set()

        # extract model data URLs for matching with jobs
        model_data_urls = []

        for model_container in model_details.get("Containers", []):
            model_data_url = model_container.get("ModelDataUrl")

            if model_data_url is not None:
                model_data_urls.append(model_data_url)
        model_data_url = model_details.get("PrimaryContainer", {}).get("ModelDataUrl")
        if model_data_url is not None:
            model_data_urls.append(model_data_url)

        model_hyperparams_raw = {}
        model_metrics_raw = {}

        for model_data_url in model_data_urls:
            data_url_matched_jobs = self.model_image_to_jobs.get(model_data_url, dict())
            # extend set of training jobs
            model_training_jobs = model_training_jobs.union(
                {
                    job_urn
                    for job_urn, job_direction in data_url_matched_jobs
                    if job_direction == JobDirection.TRAINING
                }
            )
            # extend set of downstream jobs
            model_downstream_jobs = model_downstream_jobs.union(
                {
                    job_urn
                    for job_urn, job_direction in data_url_matched_jobs
                    if job_direction == JobDirection.DOWNSTREAM
                }
            )

            for job_key, job_info in data_url_matched_jobs.items():
                if job_key.job_direction == JobDirection.TRAINING:
                    model_hyperparams_raw.update(job_info.hyperparameters)
                    model_metrics_raw.update(job_info.metrics)

        def strip_quotes(string: str) -> str:
            if string.startswith('"') or string.startswith("'"):
                string = string[1:]
            if string.endswith('"') or string.startswith("'"):
                string = string[:-1]
            return string

        model_hyperparams = [
            # all SageMaker hyperparams are strings, but stringify just in case
            MLHyperParamClass(name=key, value=strip_quotes(str(value)))
            for key, value in model_hyperparams_raw.items()
        ]
        model_hyperparams = sorted(model_hyperparams, key=lambda x: x.name)

        model_metrics = [
            # all SageMaker metrics are strings, but stringify just in case
            MLMetricClass(name=key, value=strip_quotes(str(value)))
            for key, value in model_metrics_raw.items()
        ]
        model_metrics = sorted(model_metrics, key=lambda x: x.name)

        # get jobs referencing the model by name
        name_matched_jobs = self.model_name_to_jobs.get(model_details["ModelName"], {})
        # extend set of training jobs
        model_training_jobs = model_training_jobs.union(
            {
                job_urn
                for job_urn, job_direction in name_matched_jobs
                if job_direction == JobDirection.TRAINING
            }
        )
        # extend set of downstream jobs
        model_downstream_jobs = model_downstream_jobs.union(
            {
                job_urn
                for job_urn, job_direction in name_matched_jobs
                if job_direction == JobDirection.DOWNSTREAM
            }
        )

        return (
            model_training_jobs,
            model_downstream_jobs,
            model_hyperparams,
            model_metrics,
        )

    @staticmethod
    def get_group_name_from_arn(arn: str) -> str:
        """
        Extract model package group name from a SageMaker ARN.

        Args:
            arn (str): Full ARN of the model package group

        Returns:
            str: Name of the model package group

        Example:
            >>> ModelProcessor.get_group_name_from_arn("arn:aws:sagemaker:eu-west-1:123456789:model-package-group/my-model-group")
            'my-model-group'
        """
        logger.debug(
            f"Extracting group name from ARN: {arn} because group was not seen before"
        )
        return arn.split("/")[-1]

    def get_model_wu(
        self,
        model_details: "DescribeModelOutputTypeDef",
        endpoint_arn_to_name: Dict[str, str],
    ) -> MetadataWorkUnit:
        """
        Get a workunit for a model.
        """

        # params to remove since we extract them
        redundant_fields = {"ModelName", "CreationTime"}

        model_image = model_details.get("PrimaryContainer", {}).get("Image")
        model_uri = model_details.get("PrimaryContainer", {}).get("ModelDataUrl")

        model_endpoints_sorted = self.get_model_endpoints(
            model_details, endpoint_arn_to_name, model_image, model_uri
        )

        (
            model_training_jobs,
            model_downstream_jobs,
            model_hyperparams,
            model_metrics,
        ) = self.match_model_jobs(model_details)

        # resolve groups that the model is a part of
        model_uri_groups: Set[str] = set()
        if model_uri is not None:
            model_uri_groups = self.lineage.model_uri_to_groups.get(model_uri, set())

        model_image_groups: Set[str] = set()
        if model_image is not None:
            model_image_groups = self.lineage.model_image_to_groups.get(
                model_image, set()
            )

        model_group_arns = model_uri_groups | model_image_groups

        model_group_names = sorted(
            [
                self.group_arn_to_name[x]
                if x in self.group_arn_to_name
                else self.get_group_name_from_arn(x)
                for x in model_group_arns
            ]
        )

        model_group_urns = [
            builder.make_ml_model_group_urn("sagemaker", x, self.env)
            for x in model_group_names
        ]

        model_browsepaths = [f"/sagemaker/{x}" for x in model_group_names]

        # if model is not in any groups, set a single browsepath with the model as the first entity
        if not model_browsepaths:
            model_browsepaths.append("/sagemaker")

        model_snapshot = MLModelSnapshot(
            urn=builder.make_ml_model_urn(
                "sagemaker", model_details["ModelName"], self.env
            ),
            aspects=[
                MLModelPropertiesClass(
                    date=int(
                        model_details.get("CreationTime", datetime.now()).timestamp()
                        * 1000
                    ),
                    deployments=[
                        builder.make_ml_model_deployment_urn(
                            "sagemaker", endpoint_name, self.env
                        )
                        for endpoint_name in model_endpoints_sorted
                    ],
                    customProperties={
                        key: str(value)
                        for key, value in model_details.items()
                        if key not in redundant_fields
                    },
                    trainingJobs=sorted(list(model_training_jobs)),
                    downstreamJobs=sorted(list(model_downstream_jobs)),
                    externalUrl=f"https://{self.aws_region}.console.aws.amazon.com/sagemaker/home?region={self.aws_region}#/models/{model_details['ModelName']}",
                    hyperParams=model_hyperparams,
                    trainingMetrics=model_metrics,
                    groups=model_group_urns,
                ),
                BrowsePathsClass(paths=model_browsepaths),
            ],
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=model_snapshot)

        return MetadataWorkUnit(
            id=f"{model_details['ModelName']}",
            mce=mce,
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        endpoints = self.get_all_endpoints()
        # sort endpoints for consistency
        endpoints = sorted(endpoints, key=lambda x: x["EndpointArn"])

        endpoint_arn_to_name = {}

        # ingest endpoints first since we need to know the endpoint ARN -> name mapping
        for endpoint in endpoints:
            endpoint_details = self.get_endpoint_details(endpoint["EndpointName"])

            endpoint_arn_to_name[endpoint["EndpointArn"]] = endpoint_details[
                "EndpointName"
            ]

            self.report.report_endpoint_scanned()
            yield self.get_endpoint_wu(endpoint_details)

        groups = self.get_all_groups()
        # sort groups for consistency
        groups = sorted(groups, key=lambda x: x["ModelPackageGroupName"])

        # ingest endpoints first since we need to know the endpoint ARN -> name mapping
        for group in groups:
            group_details = self.get_group_details(group["ModelPackageGroupName"])

            self.report.report_group_scanned()
            yield self.get_group_wu(group_details)

        models = self.get_all_models()
        # sort models for consistency
        models = sorted(models, key=lambda x: x["ModelArn"])

        for model in models:
            model_details = self.get_model_details(model["ModelName"])

            self.report.report_model_scanned()
            yield self.get_model_wu(model_details, endpoint_arn_to_name)
