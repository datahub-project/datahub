import logging
import re
from typing import Dict, List, Optional, Pattern

from google.cloud.aiplatform import Experiment, ExperimentRun, Model
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import VersionInfo

from datahub.emitter import mce_builder as builder
from datahub.ingestion.source.aws.s3_util import S3_PREFIXES, strip_s3_prefix
from datahub.ingestion.source.azure.abs_utils import strip_abs_prefix
from datahub.ingestion.source.gcs.gcs_utils import GCS_PREFIX, strip_gcs_prefix
from datahub.ingestion.source.vertexai.vertexai_config import PlatformDetail
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PLATFORM,
    ExternalPlatforms,
    ExternalURLs,
    URIPatterns,
)
from datahub.ingestion.source.vertexai.vertexai_models import ArtifactURNs

logger = logging.getLogger(__name__)


class VertexAIUrnBuilder:
    def __init__(
        self,
        platform: str,
        env: str,
        project_id: str,
        platform_instance: Optional[str] = None,
    ):
        self.platform = platform
        self.env = env
        self.project_id = project_id
        self.platform_instance = platform_instance
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
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=self._name_formatter.format_dataset_name(dataset_name),
            platform_instance=self.platform_instance,
            env=self.env,
        )


class VertexAINameFormatter:
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
        if entity_id is None:
            raise ValueError("entity_id cannot be None for job name formatting")
        return f"{self.project_id}.job.{entity_id}"

    def format_experiment_id(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError("entity_id cannot be None for experiment ID formatting")
        return f"{self.project_id}.experiment.{entity_id}"

    def format_experiment_run_name(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError(
                "entity_id cannot be None for experiment run name formatting"
            )
        return f"{self.project_id}.experiment_run.{entity_id}"

    def format_run_execution_name(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError(
                "entity_id cannot be None for run execution name formatting"
            )
        return f"{self.project_id}.execution.{entity_id}"

    def format_pipeline_id(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError("entity_id cannot be None for pipeline ID formatting")
        return f"{self.project_id}.pipeline.{entity_id}"

    def format_pipeline_run_id(self, entity_id: Optional[str]) -> str:
        """Format ID for a pipeline run (execution instance)."""
        if entity_id is None:
            raise ValueError("entity_id cannot be None for pipeline run ID formatting")
        return f"{self.project_id}.pipeline_run.{entity_id}"

    def format_pipeline_task_id(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError("entity_id cannot be None for pipeline task ID formatting")
        return f"{self.project_id}.pipeline_task.{entity_id}"

    def format_pipeline_task_run_id(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError(
                "entity_id cannot be None for pipeline task run ID formatting"
            )
        return f"{self.project_id}.pipeline_task_run.{entity_id}"

    def format_evaluation_name(self, entity_id: Optional[str]) -> str:
        if entity_id is None:
            raise ValueError("entity_id cannot be None for evaluation name formatting")
        return f"{self.project_id}.model_evaluation.{entity_id}"


class VertexAIExternalURLBuilder:
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
    def __init__(
        self,
        env: str,
        platform: str = PLATFORM,
        platform_instance: Optional[str] = None,
        platform_to_instance_map: Optional[Dict[str, PlatformDetail]] = None,
        normalize_paths: bool = False,
        partition_patterns: Optional[List[str]] = None,
    ):
        self.env = env
        self.platform = platform
        self.platform_instance = platform_instance
        self.platform_to_instance_map = platform_to_instance_map or {}
        self.normalize_paths = normalize_paths

        # Pre-compile regex patterns for efficiency
        self.compiled_partition_patterns: List[Pattern[str]] = []
        for pattern_str in partition_patterns or []:
            try:
                self.compiled_partition_patterns.append(re.compile(pattern_str))
            except re.error as e:
                logger.warning(
                    f"Invalid partition pattern '{pattern_str}': {e}. Skipping pattern."
                )

    def _get_platform_details(self, platform: str) -> Optional[PlatformDetail]:
        """Get platform details for the given platform from the map."""
        return self.platform_to_instance_map.get(platform)

    def _strip_partition_segments(self, path: str) -> str:
        """
        Strip partition segments from a path to create stable dataset identifiers.

        Applies pre-compiled regex patterns to identify and remove partition-specific
        path segments, enabling lineage aggregation across partitions.

        Args:
            path: Original path (e.g., gs://bucket/data/year=2024/month=01/file.parquet)

        Returns:
            Normalized path (e.g., gs://bucket/data/)

        Examples:
            gs://bucket/data/year=2024/month=01/ → gs://bucket/data/
            s3://bucket/data/dt=2024-01-15/ → s3://bucket/data/
            gs://bucket/logs/2024/01/15/ → gs://bucket/logs/
        """
        if not self.normalize_paths or not self.compiled_partition_patterns:
            return path

        # Split path into scheme+bucket and path components
        if "://" in path:
            scheme_part, path_part = path.split("://", 1)
            if "/" in path_part:
                bucket, rest = path_part.split("/", 1)
                base = f"{scheme_part}://{bucket}/"
                path_to_normalize = rest
            else:
                # Just bucket, no path
                return path
        else:
            base = ""
            path_to_normalize = path

        # Apply pre-compiled patterns to remove partition segments
        normalized_path = path_to_normalize
        for compiled_pattern in self.compiled_partition_patterns:
            normalized_path = compiled_pattern.sub("", normalized_path)

        # Clean up path - replace multiple consecutive slashes with single slash
        normalized_path = re.sub(r"/+", "/", normalized_path)

        # Remove trailing slashes
        normalized_path = normalized_path.rstrip("/")

        # Strip filename if present (datasets are typically directories, not files)
        if normalized_path and "/" in normalized_path:
            last_segment = normalized_path.rsplit("/", 1)[-1]
            if "." in last_segment:
                normalized_path = normalized_path.rsplit("/", 1)[0]

        # Reconstruct full path
        result = base + normalized_path + "/" if normalized_path else base

        if result != path:
            logger.debug(f"Normalized path: {path} → {result}")

        return result

    def _convert_azure_uri_to_https(self, uri: str) -> str:
        """
        Convert Hadoop-style Azure URIs to HTTPS format for ABS connector compatibility.

        Args:
            uri: Azure URI in Hadoop format (wasbs:// or abfss://)

        Returns:
            HTTPS formatted URI that matches ABS connector format

        Examples:
            wasbs://container@account.blob.core.windows.net/path
              → https://account.blob.core.windows.net/container/path
            abfss://container@account.dfs.core.windows.net/path
              → https://account.blob.core.windows.net/container/path
        """
        # Extract prefix if it's an Azure URI
        prefix = None
        if uri.startswith("wasbs://"):
            prefix = "wasbs://"
        elif uri.startswith("abfss://"):
            prefix = "abfss://"

        if prefix:
            uri_without_prefix = uri[len(prefix) :]
            container_and_rest = uri_without_prefix.split("@", 1)
            if len(container_and_rest) == 2:
                container = container_and_rest[0]
                rest = container_and_rest[1]
                account_and_path = rest.split("/", 1)
                account_domain = account_and_path[0]
                path = account_and_path[1] if len(account_and_path) > 1 else ""
                account_name = account_domain.split(".")[0]
                return (
                    f"https://{account_name}.blob.core.windows.net/{container}/{path}"
                )

        return uri

    def _make_external_dataset_urn(
        self, platform: str, name: str, platform_detail: Optional[PlatformDetail]
    ) -> str:
        """
        Create a dataset URN for an external platform using configured details.

        Args:
            platform: Platform identifier (e.g., 'gcs', 'bigquery')
            name: Dataset name/path
            platform_detail: Optional platform configuration

        Returns:
            Dataset URN matching what the native connector would create
        """
        if platform_detail:
            if platform_detail.convert_urns_to_lowercase:
                name = name.lower()
            env = platform_detail.env
            platform_instance = platform_detail.platform_instance
        else:
            env = self.env
            platform_instance = None

        return builder.make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
            env=env,
        )

    def dataset_urns_from_artifact_uri(self, uri: Optional[str]) -> List[str]:
        urns: List[str] = []
        if not uri:
            return urns

        # Strip partition segments if normalization is enabled
        normalized_uri = self._strip_partition_segments(uri)

        try:
            if uri.startswith(GCS_PREFIX):
                name = strip_gcs_prefix(normalized_uri)
                platform_detail = self._get_platform_details(ExternalPlatforms.GCS)
                urns.append(
                    self._make_external_dataset_urn(
                        platform=ExternalPlatforms.GCS,
                        name=name,
                        platform_detail=platform_detail,
                    )
                )
            elif uri.startswith("bq://"):
                name = uri.replace("bq://", "")
                platform_detail = self._get_platform_details(ExternalPlatforms.BIGQUERY)
                urns.append(
                    self._make_external_dataset_urn(
                        platform=ExternalPlatforms.BIGQUERY,
                        name=name,
                        platform_detail=platform_detail,
                    )
                )
            elif any(uri.startswith(prefix) for prefix in S3_PREFIXES):
                name = strip_s3_prefix(normalized_uri)
                platform_detail = self._get_platform_details(ExternalPlatforms.S3)
                urns.append(
                    self._make_external_dataset_urn(
                        platform=ExternalPlatforms.S3,
                        name=name,
                        platform_detail=platform_detail,
                    )
                )
            elif uri.startswith("wasbs://") or uri.startswith("abfss://"):
                https_uri = self._convert_azure_uri_to_https(normalized_uri)
                name = strip_abs_prefix(https_uri)
                platform_detail = self._get_platform_details(
                    ExternalPlatforms.AZURE_BLOB_STORAGE
                )
                urns.append(
                    self._make_external_dataset_urn(
                        platform=ExternalPlatforms.AZURE_BLOB_STORAGE,
                        name=name,
                        platform_detail=platform_detail,
                    )
                )
            elif uri.startswith("snowflake://"):
                name = uri.replace("snowflake://", "")
                platform_detail = self._get_platform_details(
                    ExternalPlatforms.SNOWFLAKE
                )
                urns.append(
                    self._make_external_dataset_urn(
                        platform=ExternalPlatforms.SNOWFLAKE,
                        name=name,
                        platform_detail=platform_detail,
                    )
                )
            elif "projects/" in uri and "datasets/" in uri and "tables/" in uri:
                parts = uri.split("/")
                project = parts[parts.index("projects") + 1]
                dataset = parts[parts.index("datasets") + 1]
                table = parts[parts.index("tables") + 1]
                name = f"{project}.{dataset}.{table}"
                platform_detail = self._get_platform_details(ExternalPlatforms.BIGQUERY)
                urns.append(
                    self._make_external_dataset_urn(
                        platform=ExternalPlatforms.BIGQUERY,
                        name=name,
                        platform_detail=platform_detail,
                    )
                )
        except (ValueError, IndexError, AttributeError, KeyError):
            logger.debug(
                f"Could not parse artifact uri for lineage: {uri}", exc_info=True
            )
        return urns

    def model_urn_from_artifact_uri(self, uri: Optional[str]) -> Optional[str]:
        """
        Extract model URN from ML Metadata artifact URI if it references a Vertex AI model.
        Returns None if the URI doesn't reference a model.
        """
        if not uri:
            return None

        try:
            # Model artifacts in ML Metadata can have URIs like:
            # - projects/{project}/locations/{location}/models/{model_id}
            # - aiplatform.Model-{project}-{location}-{model_id}
            if "projects/" in uri and "/models/" in uri:
                parts = uri.split("/")
                if "models" in parts:
                    model_idx = parts.index("models")
                    if model_idx + 1 < len(parts):
                        model_id = parts[model_idx + 1]
                        # Note: This won't have version info from the URI alone
                        return builder.make_ml_model_urn(
                            platform=self.platform,
                            model_name=model_id,
                            env=self.env,
                        )
        except (ValueError, IndexError):
            logger.debug(
                f"Could not parse model uri from artifact: {uri}", exc_info=True
            )
        return None

    def _classify_uri_by_key_path(self, uri: str, key_path: List[str]) -> str:
        """
        Classify a URI as 'input' or 'output' based on its key path in the job config.

        Args:
            uri: The URI string to classify
            key_path: List of dictionary keys leading to this URI

        Returns:
            Either 'input' or 'output' (defaults to 'output' if ambiguous)
        """
        key_path_string = ".".join(k.lower() for k in key_path)

        if URIPatterns.is_input_like(key_path_string):
            return "input"
        elif URIPatterns.is_output_like(key_path_string):
            return "output"
        else:
            return "output"

    def extract_external_uris_from_job(self, job: VertexAiResourceNoun) -> ArtifactURNs:
        input_uris: List[str] = []
        output_uris: List[str] = []

        def walk(obj: object, key_path: List[str]) -> None:
            """
            Recursively walk through job configuration to find external data URIs.

            Args:
                obj: Current object (dict, list, or primitive value)
                key_path: List of keys from root to current position
            """
            if isinstance(obj, dict):
                for key, value in obj.items():
                    walk(value, key_path + [str(key)])
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, key_path)
            elif isinstance(obj, str) and URIPatterns.looks_like_uri(obj):
                classification = self._classify_uri_by_key_path(obj, key_path)
                if classification == "input":
                    input_uris.append(obj)
                else:
                    output_uris.append(obj)

        try:
            job_conf = job.to_dict()
            walk(job_conf, [])
        except (AttributeError, TypeError, ValueError):
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
