from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sagemaker_processors.common import SagemakerSourceReport
from datahub.ingestion.source.sagemaker_processors.lineage import LineageInfo
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    MLModelEndpointSnapshot,
    MLModelSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    EndpointStatusClass,
    MLModelEndpointPropertiesClass,
    MLModelPropertiesClass,
)

ENDPOINT_STATUS_MAP: Dict[str, EndpointStatusClass] = {
    "OutOfService": EndpointStatusClass.OUT_OF_SERVICE,
    "Creating": EndpointStatusClass.CREATING,
    "Updating": EndpointStatusClass.UPDATING,
    "SystemUpdating": EndpointStatusClass.UPDATING,
    "RollingBack": EndpointStatusClass.ROLLING_BACK,
    "InService": EndpointStatusClass.IN_SERVICE,
    "Deleting": EndpointStatusClass.DELETING,
    "Failed": EndpointStatusClass.FAILED,
}


@dataclass
class ModelProcessor:
    sagemaker_client: Any
    env: str
    report: SagemakerSourceReport
    lineage: LineageInfo

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

    def get_model_details(self, model_name: str) -> Dict[str, Any]:
        """
        Get details of a model.
        """

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_model
        return self.sagemaker_client.describe_model(ModelName=model_name)

    def get_all_endpoints(self) -> List[Dict[str, Any]]:

        endpoints = []

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_endpoints
        paginator = self.sagemaker_client.get_paginator("list_endpoints")

        for page in paginator.paginate():
            endpoints += page["Endpoints"]

        return endpoints

    def get_endpoint_details(self, endpoint_name: str) -> Dict[str, Any]:

        # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_endpoint
        return self.sagemaker_client.describe_endpoint(EndpointName=endpoint_name)

    def get_endpoint_status(
        self, endpoint_name: str, endpoint_arn: str, sagemaker_status: str
    ) -> EndpointStatusClass:
        endpoint_status = ENDPOINT_STATUS_MAP.get(sagemaker_status)

        if endpoint_status is None:

            self.report.report_warning(
                endpoint_arn,
                f"Unknown status for {endpoint_name} ({endpoint_arn}): {sagemaker_status}",
            )

            endpoint_status = EndpointStatusClass.UNKNOWN

        return endpoint_status

    def get_endpoint_wu(self, endpoint_details: Dict[str, Any]) -> MetadataWorkUnit:
        """
        Get a workunit for an endpoint.
        """

        # params to remove since we extract them
        redundant_fields = {"EndpointName", "CreationTime"}

        endpoint_snapshot = MLModelEndpointSnapshot(
            urn=builder.make_ml_model_endpoint_urn(
                "sagemaker", endpoint_details["EndpointName"], self.env
            ),
            aspects=[
                MLModelEndpointPropertiesClass(
                    date=int(
                        endpoint_details.get("CreationTime", datetime.now()).timestamp()
                        * 1000
                    ),
                    status=self.get_endpoint_status(
                        endpoint_details["EndpointArn"],
                        endpoint_details["EndpointName"],
                        endpoint_details.get("EndpointStatus"),
                    ),
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
            id=f'{endpoint_details["EndpointName"]}',
            mce=mce,
        )

    def get_model_wu(
        self, model_details: Dict[str, Any], endpoint_arn_to_name: Dict[str, str]
    ) -> MetadataWorkUnit:
        """
        Get a workunit for a model.
        """

        # params to remove since we extract them
        redundant_fields = {"ModelName", "CreationTime"}

        model_image = model_details.get("PrimaryContainer", {}).get("Image")
        model_uri = model_details.get("PrimaryContainer", {}).get("ModelDataUrl")

        model_endpoints = set()

        if model_image is not None:
            model_endpoints |= self.lineage.model_image_endpoints[model_image]

        if model_uri is not None:
            model_endpoints |= self.lineage.model_uri_endpoints[model_image]

        model_endpoints = sorted(
            [x for x in model_endpoints if x in endpoint_arn_to_name]
        )

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
                    endpoints=[
                        builder.make_ml_model_endpoint_urn(
                            "sagemaker", endpoint_name, self.env
                        )
                        for endpoint_name in model_endpoints
                    ],
                    customProperties={
                        key: str(value)
                        for key, value in model_details.items()
                        if key not in redundant_fields
                    },
                )
            ],
        )

        # make the MCE and workunit
        mce = MetadataChangeEvent(proposedSnapshot=model_snapshot)

        return MetadataWorkUnit(
            id=f'{model_details["ModelName"]}',
            mce=mce,
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:

        endpoints = self.get_all_endpoints()
        # sort endpoints for consistency
        endpoints = sorted(endpoints, key=lambda x: x["EndpointArn"])

        endpoint_arn_to_name = {}

        for endpoint in endpoints:

            endpoint_details = self.get_endpoint_details(endpoint["EndpointName"])

            endpoint_arn_to_name[endpoint["EndpointArn"]] = endpoint_details[
                "EndpointName"
            ]

            self.report.report_endpoint_scanned()
            wu = self.get_endpoint_wu(endpoint_details)
            self.report.report_workunit(wu)
            yield wu

        models = self.get_all_models()
        # sort models for consistency
        models = sorted(models, key=lambda x: x["ModelArn"])

        for model in models:

            model_details = self.get_model_details(model["ModelName"])

            self.report.report_model_scanned()
            wu = self.get_model_wu(model_details, endpoint_arn_to_name)
            self.report.report_workunit(wu)
            yield wu
