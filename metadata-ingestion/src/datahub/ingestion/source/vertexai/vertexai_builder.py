import logging
from typing import Any, List, Optional

from google.cloud.aiplatform import Experiment, ExperimentRun, Model
from google.cloud.aiplatform.models import VersionInfo

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.vertexai.vertexai_constants import (
    ExternalURLs,
    URIPatterns,
)
from datahub.ingestion.source.vertexai.vertexai_models import ArtifactURNs

logger = logging.getLogger(__name__)


class VertexAIUrnBuilder:
    """Builder for DataHub URNs from Vertex AI resources."""

    def __init__(self, platform: str, env: str, project_id: str):
        self.platform = platform
        self.env = env
        self.project_id = project_id
        self._name_formatter = VertexAINameFormatter(project_id)

    def make_ml_model_urn(self, model_version: VersionInfo, model_name: str) -> str:
        return builder.make_ml_model_urn(
            platform=self.platform,
            model_name=f"{model_name}_{model_version.version_id}",
            env=self.env,
        )

    def make_training_job_urn(self, job_name: str) -> str:
        job_id = self._name_formatter.format_job_name(job_name)
        return builder.make_data_process_instance_urn(dataProcessInstanceId=job_id)

    def make_experiment_run_urn(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        return builder.make_data_process_instance_urn(
            self._name_formatter.format_experiment_run_name(
                f"{experiment.name}-{run.name}"
            )
        )

    def make_ml_model_group_urn(self, model: Model) -> str:
        return builder.make_ml_model_group_urn(
            platform=self.platform,
            group_name=self._name_formatter.format_model_group_name(model.name),
            env=self.env,
        )

    def make_dataset_urn(self, dataset_name: str) -> str:
        return builder.make_dataset_urn(
            platform=self.platform,
            name=self._name_formatter.format_dataset_name(dataset_name),
            env=self.env,
        )


class VertexAINameFormatter:
    """Formatter for Vertex AI entity names used in DataHub."""

    def __init__(self, project_id: str):
        self.project_id = project_id

    def format_model_group_name(self, entity_id: str) -> str:
        return f"{self.project_id}.model_group.{entity_id}"

    def format_endpoint_name(self, entity_id: str) -> str:
        return f"{self.project_id}.endpoint.{entity_id}"

    def format_model_name(self, entity_id: str) -> str:
        return f"{self.project_id}.model.{entity_id}"

    def format_dataset_name(self, entity_id: str) -> str:
        return f"{self.project_id}.dataset.{entity_id}"

    def format_job_name(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.job.{entity_id}"

    def format_experiment_id(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.experiment.{entity_id}"

    def format_experiment_run_name(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.experiment_run.{entity_id}"

    def format_run_execution_name(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.execution.{entity_id}"

    def format_pipeline_id(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.pipeline.{entity_id}"

    def format_pipeline_task_id(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.pipeline_task.{entity_id}"

    def format_pipeline_task_run_id(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.pipeline_task_run.{entity_id}"

    def format_evaluation_name(self, entity_id: Optional[str]) -> str:
        return f"{self.project_id}.model_evaluation.{entity_id}"


class VertexAIExternalURLBuilder:
    """Builder for external URLs to Vertex AI console."""

    def __init__(self, base_url: str, project_id: str, region: str):
        self.base_url = base_url
        self.project_id = project_id
        self.region = region

    def make_artifact_url(self, experiment: Experiment, run: ExperimentRun) -> str:
        return self.base_url + ExternalURLs.EXPERIMENT_ARTIFACTS.format(
            region=self.region,
            experiment_name=experiment.name,
            run_name=f"{experiment.name}-{run.name}",
            project_id=self.project_id,
        )

    def make_job_url(self, job_name: str) -> str:
        return self.base_url + ExternalURLs.TRAINING_JOB.format(
            job_name=job_name,
            project_id=self.project_id,
        )

    def make_model_url(self, model_name: str) -> str:
        return self.base_url + ExternalURLs.MODEL.format(
            region=self.region,
            model_name=model_name,
            project_id=self.project_id,
        )

    def make_model_version_url(self, model_name: str, version_id: str) -> str:
        return self.base_url + ExternalURLs.MODEL_VERSION.format(
            region=self.region,
            model_name=model_name,
            version_id=version_id,
            project_id=self.project_id,
        )

    def make_experiment_url(self, experiment_name: str) -> str:
        return self.base_url + ExternalURLs.EXPERIMENT.format(
            region=self.region,
            experiment_name=experiment_name,
            project_id=self.project_id,
        )

    def make_experiment_run_url(
        self, experiment: Experiment, run: ExperimentRun
    ) -> str:
        return self.base_url + ExternalURLs.EXPERIMENT_RUN.format(
            region=self.region,
            experiment_name=experiment.name,
            run_name=f"{experiment.name}-{run.name}",
            project_id=self.project_id,
        )

    def make_pipeline_url(self, pipeline_name: str) -> str:
        return self.base_url + ExternalURLs.PIPELINE.format(
            region=self.region,
            pipeline_name=pipeline_name,
            project_id=self.project_id,
        )


class VertexAIURIParser:
    """Parser for Vertex AI URIs and converter to DataHub dataset URNs."""

    def __init__(self, env: str):
        self.env = env

    def dataset_urns_from_artifact_uri(self, uri: Optional[str]) -> List[str]:
        urns: List[str] = []
        if not uri:
            return urns

        try:
            if uri.startswith("gs://"):
                urns.append(
                    builder.make_dataset_urn(platform="gcs", name=uri, env=self.env)
                )
            elif uri.startswith("bq://"):
                name = uri.replace("bq://", "")
                urns.append(
                    builder.make_dataset_urn(
                        platform="bigquery", name=name, env=self.env
                    )
                )
            elif "projects/" in uri and "datasets/" in uri and "tables/" in uri:
                parts = uri.split("/")
                project = parts[parts.index("projects") + 1]
                dataset = parts[parts.index("datasets") + 1]
                table = parts[parts.index("tables") + 1]
                name = f"{project}.{dataset}.{table}"
                urns.append(
                    builder.make_dataset_urn(
                        platform="bigquery", name=name, env=self.env
                    )
                )
        except Exception:
            logger.debug(
                f"Could not parse artifact uri for lineage: {uri}", exc_info=True
            )
        return urns

    def extract_external_uris_from_job(self, job: Any) -> ArtifactURNs:
        input_uris: List[str] = []
        output_uris: List[str] = []

        def walk(obj: object, key_path: List[str]) -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    walk(v, key_path + [str(k)])
            elif isinstance(obj, list):
                for v in obj:
                    walk(v, key_path)
            elif isinstance(obj, str) and URIPatterns.looks_like_uri(obj):
                kp = ".".join([k.lower() for k in key_path])
                if URIPatterns.is_input_like(kp):
                    input_uris.append(obj)
                elif URIPatterns.is_output_like(kp):
                    output_uris.append(obj)
                else:
                    output_uris.append(obj)

        try:
            job_conf = job.to_dict()
            walk(job_conf, [])
        except Exception:
            logger.debug(
                "Failed to parse training job config for external URIs", exc_info=True
            )

        input_urns: List[str] = []
        for uri in input_uris:
            input_urns.extend(self.dataset_urns_from_artifact_uri(uri))
        output_urns: List[str] = []
        for uri in output_uris:
            output_urns.extend(self.dataset_urns_from_artifact_uri(uri))

        return ArtifactURNs(input_urns=input_urns, output_urns=output_urns)
