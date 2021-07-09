from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sagemaker_processors.common import SagemakerSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import MLModelSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import MLModelPropertiesClass


@dataclass
class ModelProcessor:
    sagemaker_client: Any
    env: str
    report: SagemakerSourceReport

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

    def get_model_wu(self, model_details: Dict[str, Any]) -> MetadataWorkUnit:

        # params to remove since we extract them
        redundant_fields = {"ModelName", "CreationTime"}

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

        models = self.get_all_models()
        # sort models for consistency
        models = sorted(models, key=lambda x: x["ModelArn"])

        for model in models:

            model_details = self.get_model_details(model["ModelName"])

            self.report.report_model_scanned()
            wu = self.get_model_wu(model_details)
            self.report.report_workunit(wu)
            yield wu
