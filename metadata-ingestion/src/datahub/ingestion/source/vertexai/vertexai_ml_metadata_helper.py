import functools
import itertools
import logging
from contextlib import AbstractContextManager, nullcontext
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Union

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
    DetailCalls,
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
from datahub.ingestion.source.vertexai.vertexai_report import VertexAISourceReport
from datahub.ingestion.source.vertexai.vertexai_scope import (
    DetailReader,
    DetailResult,
    ErrorKind,
    Scope,
    StoppedScopes,
    scope_context,
)
from datahub.ingestion.source.vertexai.vertexai_utils import (
    create_vertex_retry_without_429,
    rate_limited_paged_call,
)
from datahub.metadata.schema_classes import MLHyperParamClass, MLMetricClass
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

_METADATA_RETRY = create_vertex_retry_without_429()


class MLMetadataHelper:
    def __init__(
        self,
        metadata_client: MetadataServiceClient,
        config: MLMetadataConfig,
        uri_parser: VertexAIURIParser,
        report: VertexAISourceReport,
        stopped_scopes: StoppedScopes,
        scope: Callable[[], Scope],
        rate_limiter: Union[RateLimiter, AbstractContextManager[None]] = nullcontext(),
    ):
        self.client = metadata_client
        self.config = config
        self.uri_parser = uri_parser
        self.report = report
        self._stopped = stopped_scopes
        self._scope = scope
        self._read_detail = DetailReader(report, stopped_scopes, scope)
        self.rate_limiter = rate_limiter
        self._execution_cache: Optional[List[Execution]] = None

    def _list_executions(self, request: ListExecutionsRequest) -> Iterable[Execution]:
        return rate_limited_paged_call(
            functools.partial(self.client.list_executions, retry=_METADATA_RETRY),
            request,
            self.rate_limiter,
        )

    def get_job_lineage_metadata(
        self, job: VertexAiResourceNoun
    ) -> DetailResult[Optional[LineageMetadata]]:
        executions = self._read_detail(
            DetailCalls.ML_METADATA,
            lambda: self._read_job_executions(job),
            item=job.name,
        )
        if executions.keep_previous or not executions.value:
            return DetailResult(value=None, keep_previous=executions.keep_previous)

        lineage = LineageMetadata()

        for exec_metadata in executions.value:
            lineage.hyperparams.extend(exec_metadata.hyperparams)
            lineage.metrics.extend(exec_metadata.metrics)
            if not self.config.enable_lineage_extraction:
                continue
            # A separate call, so losing it costs only the links.
            artifacts = self._read_detail(
                DetailCalls.ML_METADATA_ARTIFACTS,
                functools.partial(
                    self._extract_artifact_lineage, exec_metadata.execution_name
                ),
                item=job.name,
            )
            if artifacts.keep_previous:
                return DetailResult(value=None, keep_previous=True)
            if artifacts.value:
                lineage.input_urns.extend(artifacts.value.input_urns)
                lineage.output_urns.extend(artifacts.value.output_urns)

        lineage.deduplicate()

        logger.info(
            f"Extracted lineage for job {job.display_name}: "
            f"{len(lineage.input_urns)} inputs, {len(lineage.output_urns)} outputs, "
            f"{len(lineage.hyperparams)} hyperparams, {len(lineage.metrics)} metrics"
        )

        return DetailResult(value=lineage, keep_previous=False)

    def _read_job_executions(
        self, job: VertexAiResourceNoun
    ) -> List[ExecutionMetadata]:
        executions = self._find_executions_for_job(job)
        if not executions:
            logger.debug(
                f"No executions found in ML Metadata for job {job.display_name}"
            )
        return [self._extract_execution_metadata(e) for e in executions]

    def _find_executions_for_job(self, job: VertexAiResourceNoun) -> List[Execution]:
        parent = self.config.get_parent_path()

        escaped_name = job.display_name.replace("\\", "\\\\").replace('"', '\\"')
        filter_str = f'display_name="{escaped_name}"'
        request = ListExecutionsRequest(parent=parent, filter=filter_str)

        executions: List[Execution] = list(self._list_executions(request))

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

    def _load_execution_cache(self) -> List[Execution]:
        """Fetch all relevant executions from the API once per helper instance.

        This is called at most once per region/project to avoid making repeated
        API calls for each training job, which would quickly exhaust the 600 RPM
        quota for regional resource management requests.
        """
        if self._execution_cache is not None:
            return self._execution_cache

        parent = self.config.get_parent_path()
        schema_filters = [
            MLMetadataSchemas.CONTAINER_EXECUTION,
            MLMetadataSchemas.RUN,
            MLMetadataSchemas.CUSTOM_JOB,
        ]
        filter_str = " OR ".join(
            [f'schema_title="{schema}"' for schema in schema_filters]
        )
        max_to_retrieve = self.config.max_execution_search_limit

        request = ListExecutionsRequest(
            parent=parent,
            filter=filter_str,
            order_by="LAST_UPDATE_TIME desc",
            page_size=MLMetadataDefaults.MAX_PAGE_SIZE,
        )

        executions = list(
            itertools.islice(self._list_executions(request), max_to_retrieve)
        )
        logger.info(
            f"Loaded {len(executions)} executions into cache for ML Metadata matching"
        )

        if len(executions) >= max_to_retrieve:
            self.report.warning(
                title="Execution cache limit reached",
                message="The ML Metadata execution cache is full, so training jobs beyond it may be written without lineage or metrics. Raise ml_metadata_max_execution_search_limit.",
                context=f"{scope_context(self._scope())}, limit={max_to_retrieve}",
            )
            scope = self._scope()
            self._stopped.lost(
                Scope(
                    scope.project,
                    scope.region,
                    kind=DetailCalls.ML_METADATA_EXECUTION_CACHE,
                ),
                ErrorKind.ITEM_LASTING,
                None,
            )

        self._execution_cache = executions
        return self._execution_cache

    def _find_executions_by_schema_and_name(
        self, parent: str, job_name: str, job_display_name: str
    ) -> Sequence[Execution]:
        # Uses a cached bulk fetch instead of re-fetching from the API for each job.
        # Without caching, N training jobs × (limit/page_size) pages = O(N) API calls,
        # which quickly exhausts the 600 RPM quota for regional resource management.
        all_executions = self._load_execution_cache()

        matching_executions: List[Execution] = [
            execution
            for execution in all_executions
            if self._execution_matches_job(execution, job_name, job_display_name)
        ]

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
        request = QueryExecutionInputsAndOutputsRequest(execution=execution_name)

        with self.rate_limiter:
            response = self.client.query_execution_inputs_and_outputs(
                request=request, retry=_METADATA_RETRY
            )

        input_urns: List[str] = []
        output_urns: List[str] = []

        artifact_events: Dict[str, str] = {}
        for event in response.events:
            artifact_events[event.artifact] = event.type_.name

        for artifact in response.artifacts:
            event_type = artifact_events.get(artifact.name, "")
            dataset_urns = self.uri_parser.dataset_urns_from_artifact_uri(artifact.uri)

            if event_type == MLMetadataEventTypes.INPUT:
                input_urns.extend(dataset_urns)
            elif event_type == MLMetadataEventTypes.OUTPUT:
                output_urns.extend(dataset_urns)

        return ArtifactURNs(input_urns=input_urns, output_urns=output_urns)
