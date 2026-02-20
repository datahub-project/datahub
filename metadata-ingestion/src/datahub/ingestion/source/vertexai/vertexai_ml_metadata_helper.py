import itertools
import logging
from typing import List, Optional, Sequence

from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
    Unauthenticated,
)
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform_v1 import MetadataServiceClient
from google.cloud.aiplatform_v1.types import (
    Execution,
    ListExecutionsRequest,
    QueryExecutionInputsAndOutputsRequest,
)

from datahub.ingestion.source.vertexai.protobuf_utils import (
    extract_numeric_value,
    extract_protobuf_value,
)
from datahub.ingestion.source.vertexai.vertexai_builder import VertexAIURIParser
from datahub.ingestion.source.vertexai.vertexai_constants import (
    HyperparameterPatterns,
    MetricPatterns,
    MLMetadataDefaults,
    MLMetadataEventTypes,
    MLMetadataSchemas,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ArtifactURNs,
    ExecutionMetadata,
    LineageMetadata,
    MLMetadataConfig,
)
from datahub.ingestion.source.vertexai.vertexai_utils import format_api_error_message
from datahub.metadata.schema_classes import MLHyperParamClass, MLMetricClass

logger = logging.getLogger(__name__)


class MLMetadataHelper:
    """Encapsulates ML Metadata API operations for lineage and metrics extraction."""

    def __init__(
        self,
        metadata_client: MetadataServiceClient,
        config: MLMetadataConfig,
        uri_parser: VertexAIURIParser,
    ):
        self.client = metadata_client
        self.config = config
        self.uri_parser = uri_parser

    def get_job_lineage_metadata(
        self, job: VertexAiResourceNoun
    ) -> Optional[LineageMetadata]:
        try:
            executions = self._find_executions_for_job(job)

            if not executions:
                logger.debug(
                    f"No executions found in ML Metadata for job {job.display_name}"
                )
                return None

            lineage = LineageMetadata()

            for execution in executions:
                exec_metadata = self._extract_execution_metadata(execution)
                lineage.input_urns.extend(exec_metadata.input_artifact_urns)
                lineage.output_urns.extend(exec_metadata.output_artifact_urns)
                lineage.hyperparams.extend(exec_metadata.hyperparams)
                lineage.metrics.extend(exec_metadata.metrics)

            lineage.deduplicate()

            logger.info(
                f"Extracted lineage for job {job.display_name}: "
                f"{len(lineage.input_urns)} inputs, {len(lineage.output_urns)} outputs, "
                f"{len(lineage.hyperparams)} hyperparams, {len(lineage.metrics)} metrics"
            )

            return lineage

        except (
            PermissionDenied,
            Unauthenticated,
            ResourceExhausted,
            DeadlineExceeded,
            ServiceUnavailable,
        ) as e:
            logger.warning(
                format_api_error_message(
                    e,
                    f"extracting lineage metadata for job {job.display_name}",
                    "training_job",
                    job.name,
                ),
                exc_info=True,
            )
            return None
        except NotFound as e:
            logger.debug(
                format_api_error_message(
                    e,
                    f"ML Metadata for job {job.display_name}",
                    "training_job",
                    job.name,
                )
            )
            return None
        except AttributeError as e:
            logger.debug(
                f"Failed to extract lineage metadata for job {job.display_name} due to missing attribute | resource_type=training_job | resource_name={job.name} | cause={type(e).__name__}: {e}",
                exc_info=True,
            )
            return None
        except GoogleAPICallError as e:
            logger.warning(
                f"Failed to extract lineage metadata for job {job.display_name} | resource_type=training_job | resource_name={job.name} | cause={type(e).__name__}: {e}",
                exc_info=True,
            )
            return None

    def _find_executions_for_job(self, job: VertexAiResourceNoun) -> List[Execution]:
        parent = self.config.get_parent_path()

        filter_str = f'display_name="{job.display_name}"'
        request = ListExecutionsRequest(parent=parent, filter=filter_str)
        executions: List[Execution] = list(self.client.list_executions(request=request))

        if not executions:
            matching = self._find_executions_by_schema_and_name(
                parent, job.name, job.display_name
            )
            executions = list(matching)

        if self.config.max_executions_per_job:
            executions = list(
                itertools.islice(executions, self.config.max_executions_per_job)
            )

        return executions

    def _find_executions_by_schema_and_name(
        self, parent: str, job_name: str, job_display_name: str
    ) -> Sequence[Execution]:
        schema_filters = [
            MLMetadataSchemas.CONTAINER_EXECUTION,
            MLMetadataSchemas.RUN,
            MLMetadataSchemas.CUSTOM_JOB,
        ]

        filter_str = " OR ".join(
            [f'schema_title="{schema}"' for schema in schema_filters]
        )

        # Use configured search limit (default 500) to prevent excessive API calls and timeouts
        max_to_retrieve = self.config.max_execution_search_limit

        request = ListExecutionsRequest(
            parent=parent,
            filter=filter_str,
            order_by="LAST_UPDATE_TIME desc",
            page_size=MLMetadataDefaults.MAX_PAGE_SIZE,  # API max is 100 per page
        )

        paged_response = self.client.list_executions(request=request)

        all_executions: List[Execution] = list(
            itertools.islice(paged_response, max_to_retrieve)
        )

        if len(all_executions) >= max_to_retrieve:
            logger.warning(
                f"Retrieved maximum number of executions ({max_to_retrieve}) "
                f"while searching for job '{job_display_name}'. Results may be incomplete. "
                f"Consider using more specific display names or reducing concurrent job volume."
            )

        matching_executions: List[Execution] = []
        for execution in all_executions:
            if self._execution_matches_job(execution, job_name, job_display_name):
                matching_executions.append(execution)

        return matching_executions

    def _execution_matches_job(
        self, execution: Execution, job_name: str, job_display_name: str
    ) -> bool:
        if not getattr(execution, "metadata", None):
            return False

        metadata_str = str(execution.metadata).lower()
        return (
            job_name.lower() in metadata_str or job_display_name.lower() in metadata_str
        )

    def _extract_execution_metadata(self, execution: Execution) -> ExecutionMetadata:
        exec_metadata = ExecutionMetadata(execution_name=execution.name)

        if self.config.enable_metrics_extraction:
            exec_metadata.hyperparams = self._extract_hyperparams(execution)
            exec_metadata.metrics = self._extract_metrics(execution)

        if self.config.enable_lineage_extraction:
            artifact_urns = self._extract_artifact_lineage(execution.name)
            exec_metadata.input_artifact_urns = artifact_urns.input_urns
            exec_metadata.output_artifact_urns = artifact_urns.output_urns

        return exec_metadata

    def _extract_hyperparams(self, execution: Execution) -> List[MLHyperParamClass]:
        hyperparams: List[MLHyperParamClass] = []
        if not getattr(execution, "metadata", None):
            return hyperparams

        for key in execution.metadata:
            if not HyperparameterPatterns.is_hyperparam(key):
                continue

            value = execution.metadata[key]
            param_value = extract_protobuf_value(value)
            if param_value:
                hyperparams.append(MLHyperParamClass(name=key, value=param_value))

        return hyperparams

    def _extract_metrics(self, execution: Execution) -> List[MLMetricClass]:
        metrics: List[MLMetricClass] = []
        if not getattr(execution, "metadata", None):
            return metrics

        for key in execution.metadata:
            if not MetricPatterns.is_metric(key):
                continue

            value = execution.metadata[key]
            metric_value = extract_numeric_value(value)
            if metric_value is not None:
                metrics.append(MLMetricClass(name=key, value=metric_value))

        return metrics

    def _extract_artifact_lineage(self, execution_name: str) -> ArtifactURNs:
        try:
            request = QueryExecutionInputsAndOutputsRequest(execution=execution_name)
            response = self.client.query_execution_inputs_and_outputs(request=request)

            input_urns: List[str] = []
            output_urns: List[str] = []

            artifact_events = {}
            for event in response.events:
                artifact_events[event.artifact] = event.type_.name

            for artifact in response.artifacts:
                if not artifact.uri:
                    continue

                event_type = artifact_events.get(artifact.name, "")
                dataset_urns = self.uri_parser.dataset_urns_from_artifact_uri(
                    artifact.uri
                )

                if event_type == MLMetadataEventTypes.INPUT:
                    input_urns.extend(dataset_urns)
                elif event_type == MLMetadataEventTypes.OUTPUT:
                    output_urns.extend(dataset_urns)

            return ArtifactURNs(input_urns=input_urns, output_urns=output_urns)

        except (
            PermissionDenied,
            Unauthenticated,
            ResourceExhausted,
            DeadlineExceeded,
            ServiceUnavailable,
        ) as e:
            logger.warning(
                format_api_error_message(
                    e,
                    f"extracting artifact lineage for execution {execution_name}",
                    "execution",
                    execution_name,
                )
            )
            return ArtifactURNs()
        except NotFound as e:
            logger.debug(
                format_api_error_message(
                    e,
                    f"Artifact lineage for execution {execution_name}",
                    "execution",
                    execution_name,
                )
            )
            return ArtifactURNs()
        except AttributeError as e:
            logger.debug(
                f"Failed to extract artifact lineage for execution {execution_name} due to missing attribute | resource_type=execution | resource_name={execution_name} | cause={type(e).__name__}: {e}"
            )
            return ArtifactURNs()
        except GoogleAPICallError as e:
            logger.warning(
                f"Failed to extract artifact lineage for execution {execution_name} | resource_type=execution | resource_name={execution_name} | cause={type(e).__name__}: {e}"
            )
            return ArtifactURNs()
