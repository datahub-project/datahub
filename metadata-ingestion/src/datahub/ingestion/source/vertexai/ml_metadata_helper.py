import logging
from typing import Callable, List, Optional, Sequence, Tuple

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
from datahub.ingestion.source.vertexai.vertexai_constants import (
    HyperparameterPatterns,
    MetricPatterns,
    MLMetadataDefaults,
    MLMetadataSchemas,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    ExecutionMetadata,
    LineageMetadata,
    MLMetadataConfig,
)
from datahub.metadata.schema_classes import MLHyperParamClass, MLMetricClass

logger = logging.getLogger(__name__)


class MLMetadataHelper:
    """Encapsulates ML Metadata API operations for lineage and metrics extraction."""

    def __init__(
        self,
        metadata_client: MetadataServiceClient,
        config: MLMetadataConfig,
        dataset_urn_converter: Callable[[str], List[str]],
    ):
        self.client = metadata_client
        self.config = config
        self._dataset_urn_converter = dataset_urn_converter

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

        except Exception as e:
            logger.warning(
                f"Failed to extract lineage metadata for job {job.display_name}: {e}",
                exc_info=True,
            )
            return None

    def _find_executions_for_job(self, job: VertexAiResourceNoun) -> List[Execution]:
        parent = self.config.get_parent_path()

        # Try display name first (fastest lookup)
        filter_str = f'display_name="{job.display_name}"'
        request = ListExecutionsRequest(parent=parent, filter=filter_str)
        executions = list(self.client.list_executions(request=request))

        # Fallback to broader search if display name lookup fails
        if not executions:
            matching = self._find_executions_by_schema_and_name(
                parent, job.name, job.display_name
            )
            executions = list(matching)

        if (
            self.config.max_executions_per_job
            and len(executions) > self.config.max_executions_per_job
        ):
            executions = executions[: self.config.max_executions_per_job]

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

        request = ListExecutionsRequest(
            parent=parent,
            filter=filter_str,
            page_size=MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS,
        )

        all_executions: List[Execution] = list(
            self.client.list_executions(request=request)
        )

        if len(all_executions) >= MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS:
            logger.warning(
                f"Retrieved maximum number of executions ({MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS}) "
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
            input_urns, output_urns = self._extract_artifact_lineage(execution.name)
            exec_metadata.input_artifact_urns = input_urns
            exec_metadata.output_artifact_urns = output_urns

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

    def _extract_artifact_lineage(
        self, execution_name: str
    ) -> Tuple[List[str], List[str]]:
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
                dataset_urns = self._dataset_urn_converter(artifact.uri)

                if event_type == "INPUT":
                    input_urns.extend(dataset_urns)
                elif event_type == "OUTPUT":
                    output_urns.extend(dataset_urns)

            return input_urns, output_urns

        except Exception as e:
            logger.warning(
                f"Failed to extract artifact lineage for execution {execution_name}: {e}"
            )
            return [], []
