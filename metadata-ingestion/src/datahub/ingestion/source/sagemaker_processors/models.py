from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, DefaultDict, Dict, Iterable, List, Set

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sagemaker_processors.common import SagemakerSourceReport
from datahub.ingestion.source.sagemaker_processors.jobs import JobDirection, ModelJob
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import MLModelSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import MLModelPropertiesClass


@dataclass
class ModelProcessor:
    sagemaker_client: Any
    env: str
    report: SagemakerSourceReport

    # map from model image file path to jobs referencing the model
    model_data_to_jobs: DefaultDict[str, Set[ModelJob]] = field(
        default_factory=lambda: defaultdict(set)
    )

    # map from model name to jobs referencing the model
    model_name_to_jobs: DefaultDict[str, Set[ModelJob]] = field(
        default_factory=lambda: defaultdict(set)
    )

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

        for model_data_url in model_data_urls:

            data_url_matched_jobs = self.model_data_to_jobs.get(model_data_url, set())
            # extend set of training jobs
            model_training_jobs = model_training_jobs.union(
                {
                    job.job_urn
                    for job in data_url_matched_jobs
                    if job.job_direction == JobDirection.TRAINING
                }
            )
            # extend set of downstream jobs
            model_downstream_jobs = model_downstream_jobs.union(
                {
                    job.job_urn
                    for job in data_url_matched_jobs
                    if job.job_direction == JobDirection.DOWNSTREAM
                }
            )

        # get jobs referencing the model by name
        name_matched_jobs = self.model_name_to_jobs.get(
            model_details["ModelName"], set()
        )
        # extend set of training jobs
        model_training_jobs = model_training_jobs.union(
            {
                job.job_urn
                for job in name_matched_jobs
                if job.job_direction == JobDirection.TRAINING
            }
        )
        # extend set of downstream jobs
        model_downstream_jobs = model_downstream_jobs.union(
            {
                job.job_urn
                for job in name_matched_jobs
                if job.job_direction == JobDirection.DOWNSTREAM
            }
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
                    customProperties={
                        key: str(value)
                        for key, value in model_details.items()
                        if key not in redundant_fields
                    },
                    trainingJobs=sorted(list(model_training_jobs)),
                    downstreamJobs=sorted(list(model_downstream_jobs)),
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
