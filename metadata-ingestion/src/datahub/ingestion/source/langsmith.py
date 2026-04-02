import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

import langsmith
from langsmith.schemas import (
    Dataset as LangSmithDataset,
    Run as LangSmithRun,
    TracerSession as LangSmithProject,
)
from pydantic import SecretStr
from pydantic.fields import Field

from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mce_builder import datahub_guid, make_assertion_urn, make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ExperimentKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionSourceClass,
    AssertionSourceTypeClass,
    AssertionTypeClass,
    AuditStampClass,
    ContainerClass,
    CustomAssertionInfoClass,
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DataProcessInstanceInputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    EdgeClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
    PlatformTypeClass,
    SubTypesClass,
    _Aspect,
)
from datahub.metadata.urns import DataPlatformUrn
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.mlmodel import MLModel


class LangSmithConfig(StatefulIngestionConfigBase, EnvConfigMixin):
    api_key: SecretStr = Field(
        description="LangSmith API key. Create a Personal Access Token at https://smith.langchain.com under Settings > API Keys.",
    )
    api_url: str = Field(
        default="https://api.smith.langchain.com",
        description="LangSmith API base URL. Use https://eu.api.smith.langchain.com for EU-region workspaces.",
    )
    project_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter projects (LangSmith sessions) by name.",
    )
    max_traces_per_project: Optional[int] = Field(
        default=1000,
        description="Maximum number of traces (root runs) to ingest per project. Set to None for unlimited.",
    )
    trace_start_time: Optional[datetime] = Field(
        default=None,
        description="Only ingest traces that started at or after this timestamp (UTC). Useful for incremental loads.",
    )
    include_datasets: bool = Field(
        default=True,
        description="Whether to ingest LangSmith datasets as DataHub Dataset entities.",
    )
    include_feedback_on_traces: bool = Field(
        default=True,
        description="Whether to attach aggregated feedback scores as custom properties on traces.",
    )
    include_child_spans: bool = Field(
        default=False,
        description="Ingest child spans (LLM calls, tool calls, retrievers) within each trace "
        "as nested DataProcessInstances with parent-child lineage edges. Increases entity "
        "volume significantly.",
    )
    include_assertions: bool = Field(
        default=False,
        description="Emit DataHub Assertion entities from LangSmith feedback scores on traces. "
        "Each unique (feedback key, run name) pair becomes one Assertion entity targeting the "
        "eval dataset, so each named scenario has its own current pass/fail status in the "
        "DataHub Quality tab. Each trace emits an AssertionRunEvent. Requires "
        "include_datasets=True or assertion_dataset_urn to be set.",
    )
    assertion_score_threshold: float = Field(
        default=0.7,
        description="Minimum avg feedback score (0.0-1.0) for an assertion to pass. "
        "Scores >= threshold yield SUCCESS; scores below yield FAILURE.",
    )
    assertion_dataset_urn: Optional[str] = Field(
        default=None,
        description="Explicit dataset URN to use as the assertion target (assertee). "
        "Auto-detected from ingested LangSmith datasets when not set.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class LangSmithSourceReport(StaleEntityRemovalSourceReport):
    projects_scanned: int = 0
    traces_ingested: int = 0
    spans_ingested: int = 0
    retriever_datasets_emitted: int = 0
    models_emitted: int = 0
    datasets_ingested: int = 0
    projects_filtered: int = 0
    traces_skipped_limit: int = 0
    assertions_emitted: int = 0
    assertion_run_events_emitted: int = 0
    assertions_skipped_no_dataset: int = 0


@platform_name("LangSmith")
@config_class(LangSmithConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extracts descriptions for LangSmith projects and datasets.",
)
@capability(
    SourceCapability.CONTAINERS,
    "Extracts LangSmith projects as containers.",
    subtype_modifier=[MLAssetSubTypes.LANGSMITH_PROJECT],
)
@capability(
    SourceCapability.TAGS,
    "Extracts run tags as DataHub tags on traces.",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Soft-deletes projects and traces removed from LangSmith since the last run.",
)
class LangSmithSource(StatefulIngestionSourceBase):
    platform = "langsmith"

    def __init__(self, ctx: PipelineContext, config: LangSmithConfig) -> None:
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report = LangSmithSourceReport()
        self.client = langsmith.Client(
            api_url=self.config.api_url,
            api_key=self.config.api_key.get_secret_value(),
        )
        self._dataset_urns: Dict[str, str] = {}
        self._assertion_urns_emitted: set = set()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "LangSmithSource":
        config = LangSmithConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=make_data_platform_urn(self.platform),
            aspect=DataPlatformInfoClass(
                name=self.platform,
                displayName="LangSmith",
                type=PlatformTypeClass.OTHERS,
                datasetNameDelimiter=".",
                logoUrl="https://unpkg.com/@lobehub/icons-static-svg@1.84.0/icons/langsmith-color.svg",
            ),
        ).as_workunit()
        # Datasets first so _dataset_urns is populated before trace assertion emission
        if self.config.include_datasets:
            yield from self._get_dataset_workunits()
        yield from self._get_project_workunits()

    # -------------------------------------------------------------------------
    # Project (Container) extraction
    # -------------------------------------------------------------------------

    def _get_project_workunits(self) -> Iterable[MetadataWorkUnit]:
        for project in self.client.list_projects():
            self.report.projects_scanned += 1
            if not self.config.project_pattern.allowed(project.name):
                self.report.projects_filtered += 1
                continue
            yield from self._emit_project_container(project)
            yield from self._get_trace_workunits(project)

    def _emit_project_container(
        self, project: LangSmithProject
    ) -> Iterable[MetadataWorkUnit]:
        custom_props: Dict[str, str] = {}
        if project.description:
            custom_props["description"] = project.description
        if project.extra:
            for k, v in project.extra.items():
                custom_props[f"extra.{k}"] = str(v)

        container = Container(
            container_key=ExperimentKey(
                platform=str(DataPlatformUrn(platform_name=self.platform)),
                id=project.name,
            ),
            subtype=MLAssetSubTypes.LANGSMITH_PROJECT,
            display_name=project.name,
            description=project.description,
            extra_properties=custom_props or None,
        )
        yield from container.as_workunits()

    # -------------------------------------------------------------------------
    # Trace (DataProcessInstance) extraction
    # -------------------------------------------------------------------------

    def _get_trace_workunits(
        self, project: LangSmithProject
    ) -> Iterable[MetadataWorkUnit]:
        count = 0
        limit = self.config.max_traces_per_project

        list_kwargs = {
            "project_name": project.name,
            "is_root": True,
        }
        if self.config.trace_start_time:
            list_kwargs["start_time"] = self.config.trace_start_time

        for run in self.client.list_runs(**list_kwargs):
            if limit is not None and count >= limit:
                self.report.traces_skipped_limit += 1
                continue
            yield from self._emit_run(project, run)
            # Assertions from feedback scores
            if not run.feedback_stats:
                logger.debug("run %s (%s) has no feedback_stats", run.name, run.id)
            if self.config.include_assertions and run.feedback_stats:
                assertion_dataset_urn = self._resolve_assertion_dataset_urn()
                if assertion_dataset_urn:
                    yield from self._emit_assertion_workunits(run, assertion_dataset_urn)
                else:
                    self.report.assertions_skipped_no_dataset += 1
            if self.config.include_child_spans:
                # Child span mode: flow graph handles all lineage including root inputEdges
                yield from self._get_child_span_workunits(project, run)
            else:
                # No child spans: collect asset edges directly on root trace
                root_dpi_urn = self._make_dpi_urn(run.id)
                input_edge_urns: Dict[str, EdgeClass] = {}
                yield from self._collect_run_edges(project, run, input_edge_urns)
                if input_edge_urns:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=root_dpi_urn,
                        aspect=DataProcessInstanceInputClass(
                            inputs=[],
                            inputEdges=list(input_edge_urns.values()),
                        ),
                    ).as_workunit()
            count += 1
            self.report.traces_ingested += 1

    def _make_dpi_urn(self, run_id: uuid.UUID) -> str:
        return str(DataProcessInstance(id=str(run_id), orchestrator=self.platform).urn)

    def _get_child_span_workunits(
        self,
        project: LangSmithProject,
        root_run: LangSmithRun,
    ) -> Iterable[MetadataWorkUnit]:
        root_dpi_urn = self._make_dpi_urn(root_run.id)

        # Collect all children first -- needed to build the flow graph
        all_children = list(self.client.list_runs(trace_id=root_run.id, is_root=False))

        # Phase 1: Emit DPI aspects for each child; collect per-child asset edges
        child_asset_edges: Dict[uuid.UUID, List[EdgeClass]] = {}
        for child_run in all_children:
            parent_dpi_urn = self._make_dpi_urn(child_run.parent_run_id)
            yield from self._emit_run(
                project,
                child_run,
                subtype=MLAssetSubTypes.LANGSMITH_SPAN,
                parent_dpi_urn=parent_dpi_urn,
                root_dpi_urn=root_dpi_urn,
            )
            self.report.spans_ingested += 1
            asset_edges: Dict[str, EdgeClass] = {}
            yield from self._collect_run_edges(project, child_run, asset_edges)
            if asset_edges:
                child_asset_edges[child_run.id] = list(asset_edges.values())

        # Phase 2: Compute execution flow graph from span timestamps
        flow_upstreams = self._compute_flow_graph(root_run, all_children)

        # Phase 3: Emit DataProcessInstanceInput for each child that has edges
        for child_run in all_children:
            edges: List[EdgeClass] = []
            for upstream_id in flow_upstreams.get(child_run.id, []):
                edges.append(EdgeClass(destinationUrn=self._make_dpi_urn(upstream_id)))
            edges.extend(child_asset_edges.get(child_run.id, []))
            if edges:
                yield MetadataChangeProposalWrapper(
                    entityUrn=self._make_dpi_urn(child_run.id),
                    aspect=DataProcessInstanceInputClass(inputs=[], inputEdges=edges),
                ).as_workunit()

        # Phase 4: Emit DataProcessInstanceInput on root (terminal children + own asset edges)
        root_upstream_ids = flow_upstreams.get(root_run.id, [])
        root_edges: List[EdgeClass] = [
            EdgeClass(destinationUrn=self._make_dpi_urn(uid)) for uid in root_upstream_ids
        ]
        root_own_assets: Dict[str, EdgeClass] = {}
        yield from self._collect_run_edges(project, root_run, root_own_assets)
        root_edges.extend(root_own_assets.values())
        if root_edges:
            yield MetadataChangeProposalWrapper(
                entityUrn=root_dpi_urn,
                aspect=DataProcessInstanceInputClass(inputs=[], inputEdges=root_edges),
            ).as_workunit()

    def _compute_flow_graph(
        self,
        root_run: LangSmithRun,
        child_runs: List[LangSmithRun],
    ) -> Dict[uuid.UUID, List[uuid.UUID]]:
        """Compute execution flow edges for a trace's span tree.

        Returns a mapping of run_id -> list of upstream run_ids.
        The root_run.id entry lists the terminal children of the trace (last step).
        Sibling order is inferred from start_time; overlapping siblings are
        treated as a parallel group (fan-in to the next sequential step).
        """
        children_by_parent: Dict[uuid.UUID, List[LangSmithRun]] = defaultdict(list)
        for child in child_runs:
            children_by_parent[child.parent_run_id].append(child)

        flow_upstreams: Dict[uuid.UUID, List[uuid.UUID]] = defaultdict(list)

        def _process_node(parent_id: uuid.UUID) -> None:
            siblings = children_by_parent.get(parent_id, [])
            if not siblings:
                return

            siblings.sort(key=lambda r: r.start_time)

            # Group into sequential steps; overlapping siblings share a step
            steps: List[List[LangSmithRun]] = []
            current_group: List[LangSmithRun] = [siblings[0]]
            current_end = siblings[0].end_time or siblings[0].start_time

            for sib in siblings[1:]:
                if sib.start_time < current_end:
                    # Overlapping -- same parallel group
                    current_group.append(sib)
                    if sib.end_time and sib.end_time > current_end:
                        current_end = sib.end_time
                else:
                    steps.append(current_group)
                    current_group = [sib]
                    current_end = sib.end_time or sib.start_time
            steps.append(current_group)

            # Sequential flow: every member of step[i] is upstream of every member of step[i+1]
            for i in range(1, len(steps)):
                for consumer in steps[i]:
                    for producer in steps[i - 1]:
                        flow_upstreams[consumer.id].append(producer.id)

            # Terminal step feeds the parent
            for terminal in steps[-1]:
                flow_upstreams[parent_id].append(terminal.id)

            # Recurse into each child's own subtree
            for sib in siblings:
                _process_node(sib.id)

        _process_node(root_run.id)
        return dict(flow_upstreams)

    def _emit_run(
        self,
        project: LangSmithProject,
        run: LangSmithRun,
        subtype: str = MLAssetSubTypes.LANGSMITH_TRACE,
        parent_dpi_urn: Optional[str] = None,
        root_dpi_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dpi = DataProcessInstance(
            id=str(run.id),
            orchestrator=self.platform,
        )
        dpi_urn = str(dpi.urn)

        experiment_key = ExperimentKey(
            platform=str(DataPlatformUrn(platform_name=self.platform)),
            id=project.name,
        )

        # Build custom properties from run metadata
        custom_props: Dict[str, str] = {
            "run_type": run.run_type or "",
            "status": run.status or "",
        }
        if run.trace_id and run.trace_id != run.id:
            custom_props["trace_id"] = str(run.trace_id)
        if run.start_time and run.end_time:
            latency_ms = int((run.end_time - run.start_time).total_seconds() * 1000)
            custom_props["latency_ms"] = str(latency_ms)
        if run.extra:
            metadata = run.extra.get("metadata") or {}
            for k, v in metadata.items():
                custom_props[f"metadata.{k}"] = str(v)
            runtime_env = run.extra.get("runtime") or {}
            if runtime_env.get("library"):
                custom_props["library"] = str(runtime_env["library"])

        # Tags as custom properties (globalTags aspect is not supported on dataProcessInstance)
        if run.tags:
            custom_props["tags"] = ",".join(run.tags)

        # Feedback as custom properties
        if self.config.include_feedback_on_traces and run.feedback_stats:
            for key, stats in run.feedback_stats.items():
                if isinstance(stats, dict) and "avg" in stats:
                    custom_props[f"feedback.{key}.avg"] = str(stats["avg"])
                    if "n" in stats:
                        custom_props[f"feedback.{key}.count"] = str(stats["n"])

        created_time = (
            int(run.start_time.timestamp() * 1000)
            if run.start_time
            else int(time.time() * 1000)
        )

        # DataProcessInstanceProperties
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=run.name or str(run.id),
                created=AuditStampClass(
                    time=created_time,
                    actor="urn:li:corpuser:datahub",
                ),
                externalUrl=self._make_trace_url(project, run),
                customProperties=custom_props,
            ),
        ).as_workunit()

        # Container link (trace -> project)
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=ContainerClass(container=experiment_key.as_urn()),
        ).as_workunit()

        # SubType
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=SubTypesClass(typeNames=[subtype]),
        ).as_workunit()

        # Parent-child relationship (child spans only)
        if parent_dpi_urn is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceRelationshipsClass(
                    parentInstance=parent_dpi_urn,
                    upstreamInstances=[parent_dpi_urn],
                ),
            ).as_workunit()

        # Platform instance
        yield MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            aspect=DataPlatformInstanceClass(
                platform=str(DataPlatformUrn(platform_name=self.platform))
            ),
        ).as_workunit()

        # Token usage as ML metrics
        metrics = self._get_token_metrics(run)
        if metrics:
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=MLTrainingRunPropertiesClass(
                    id=str(run.id),
                    trainingMetrics=metrics,
                ),
            ).as_workunit()

        # Run event (status + duration)
        if run.end_time and run.start_time:
            duration_ms = int((run.end_time - run.start_time).total_seconds() * 1000)
            yield MetadataChangeProposalWrapper(
                entityUrn=dpi_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=int(run.end_time.timestamp() * 1000),
                    result=DataProcessInstanceRunResultClass(
                        type=self._convert_run_status(run.status),
                        nativeResultType=self.platform,
                    ),
                    durationMillis=duration_ms,
                ),
            ).as_workunit()

    def _build_retriever_stub(
        self,
        project: LangSmithProject,
        run: LangSmithRun,
    ) -> Tuple[str, Iterable[MetadataWorkUnit]]:
        """Build a stub Dataset entity for a retriever span. Returns (urn, workunits)."""
        run_metadata = (run.extra or {}).get("metadata") or {}
        retriever_name = run_metadata.get(
            "ls_retriever_name"
        ) or run.name.lower().replace(" ", "-")
        provider = run_metadata.get("ls_vector_store_provider")
        platform = provider.lower() if provider else self.platform
        source_dataset = Dataset(
            platform=platform,
            name=f"{project.name}/{retriever_name}",
        )
        return (str(source_dataset.urn), source_dataset.as_workunits())

    @staticmethod
    def _infer_provider(model_name: str) -> str:
        """Infer DataHub platform from a model name when ls_provider is absent."""
        m = model_name.lower()
        if m.startswith("claude"):
            return "anthropic"
        if m.startswith(("gpt-", "o1-", "o3-", "o4-")):
            return "openai"
        if m.startswith("gemini"):
            return "google"
        if m.startswith(("mistral", "mixtral")):
            return "mistral"
        if m.startswith("command"):
            return "cohere"
        return "langsmith"

    def _build_model_stub(
        self,
        run: LangSmithRun,
    ) -> Optional[Tuple[str, Iterable[MetadataWorkUnit]]]:
        """Build a stub MLModel entity from LLM span metadata. Returns (urn, workunits) or None."""
        run_metadata = (run.extra or {}).get("metadata") or {}
        invocation_params = (run.extra or {}).get("invocation_params") or {}
        # Priority: ls_model_name (LangChain standard) > invocation_params.model > metadata.model
        model_name = (
            run_metadata.get("ls_model_name")
            or invocation_params.get("model")
            or run_metadata.get("model")
        )
        if not model_name:
            return None
        provider = run_metadata.get("ls_provider")
        platform = provider.lower() if provider else self._infer_provider(model_name)
        custom_props: Dict[str, str] = {}
        model_type = run_metadata.get("ls_model_type")
        if model_type:
            custom_props["model_type"] = str(model_type)
        model = MLModel(
            id=model_name,
            platform=platform,
            env=self.config.env,
            name=model_name,
            custom_properties=custom_props or None,
        )
        return (str(model.urn), model.as_workunits())

    def _collect_run_edges(
        self,
        project: LangSmithProject,
        run: LangSmithRun,
        input_edge_urns: Dict[str, EdgeClass],
    ) -> Iterable[MetadataWorkUnit]:
        """Collect upstream input edges for a run into input_edge_urns. Yields stub workunits."""
        if run.run_type == "retriever":
            ds_urn, stub_wus = self._build_retriever_stub(project, run)
            yield from stub_wus
            if ds_urn not in input_edge_urns:
                input_edge_urns[ds_urn] = EdgeClass(destinationUrn=ds_urn)
            self.report.retriever_datasets_emitted += 1
        if run.run_type == "llm":
            result = self._build_model_stub(run)
            if result is not None:
                model_urn, stub_wus = result
                yield from stub_wus
                if model_urn not in input_edge_urns:
                    input_edge_urns[model_urn] = EdgeClass(destinationUrn=model_urn)
                self.report.models_emitted += 1

    def _get_token_metrics(self, run: LangSmithRun) -> List[MLMetricClass]:
        metrics = []
        prompt_tokens = getattr(run, "prompt_tokens", None)
        completion_tokens = getattr(run, "completion_tokens", None)
        total_tokens = getattr(run, "total_tokens", None)
        if prompt_tokens is not None:
            metrics.append(
                MLMetricClass(name="prompt_tokens", value=str(prompt_tokens))
            )
        if completion_tokens is not None:
            metrics.append(
                MLMetricClass(name="completion_tokens", value=str(completion_tokens))
            )
        if total_tokens is not None:
            metrics.append(MLMetricClass(name="total_tokens", value=str(total_tokens)))
        return metrics

    def _convert_run_status(self, status: Optional[str]) -> str:
        if status == "success":
            return "SUCCESS"
        elif status == "error":
            return "FAILURE"
        else:
            return "SKIPPED"

    def _make_trace_url(
        self, project: LangSmithProject, run: LangSmithRun
    ) -> Optional[str]:
        return self._run_url(run)

    def _run_url(self, run: LangSmithRun) -> Optional[str]:
        # Use app_path from the run object if available (e.g. "/o/<org>/projects/p/<id>/r/<run_id>")
        if run.app_path:
            base = self.config.api_url.replace("api.", "").replace("/api", "")
            return f"{base.rstrip('/')}{run.app_path}"
        return None

    # -------------------------------------------------------------------------
    # Dataset extraction
    # -------------------------------------------------------------------------

    def _get_dataset_workunits(self) -> Iterable[MetadataWorkUnit]:
        for ls_dataset in self.client.list_datasets():
            yield from self._emit_dataset(ls_dataset)
            self.report.datasets_ingested += 1

    def _emit_dataset(self, ls_dataset: LangSmithDataset) -> Iterable[MetadataWorkUnit]:
        custom_props: Dict[str, str] = {
            "langsmith_dataset_id": str(ls_dataset.id),
        }
        if ls_dataset.data_type:
            custom_props["data_type"] = str(ls_dataset.data_type)
        if ls_dataset.created_at:
            custom_props["created_at"] = ls_dataset.created_at.isoformat()

        dataset = Dataset(
            platform=self.platform,
            name=ls_dataset.name,
            description=ls_dataset.description,
            custom_properties=custom_props,
        )
        self._dataset_urns[ls_dataset.name] = str(dataset.urn)
        yield from dataset.as_workunits()

    # -------------------------------------------------------------------------
    # Assertion extraction
    # -------------------------------------------------------------------------

    def _resolve_assertion_dataset_urn(self) -> Optional[str]:
        """Return the dataset URN to use as the assertion assertee."""
        if self.config.assertion_dataset_urn:
            return self.config.assertion_dataset_urn
        if self._dataset_urns:
            return next(iter(self._dataset_urns.values()))
        return None

    def _emit_assertion_workunits(
        self, run: LangSmithRun, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit AssertionInfo + AssertionRunEvent for each feedback key on a run."""
        for key, stats in (run.feedback_stats or {}).items():
            if not isinstance(stats, dict) or "avg" not in stats:
                continue
            avg_score = float(stats["avg"])
            count = int(stats.get("n", 1))

            run_name = run.name or str(run.id)
            assertion_id = datahub_guid(
                {
                    "platform": self.platform,
                    "feedback_key": key,
                    "dataset": dataset_urn,
                    "run_name": run_name,
                }
            )
            assertion_urn = make_assertion_urn(assertion_id)

            # AssertionInfo: emit once per unique assertion URN
            if assertion_urn not in self._assertion_urns_emitted:
                self._assertion_urns_emitted.add(assertion_urn)
                yield MetadataChangeProposalWrapper(
                    entityUrn=assertion_urn,
                    aspect=AssertionInfoClass(
                        type=AssertionTypeClass.CUSTOM,
                        customAssertion=CustomAssertionInfoClass(
                            type=key.replace("_", " ").title(),
                            entity=dataset_urn,
                        ),
                        source=AssertionSourceClass(
                            type=AssertionSourceTypeClass.EXTERNAL,
                        ),
                        description=f"{run_name}: {key.replace('_', ' ')} score >= {self.config.assertion_score_threshold}",
                        customProperties={
                            "feedback_key": key,
                            "threshold": str(self.config.assertion_score_threshold),
                            "langsmith_platform": self.platform,
                            "run_name": run_name,
                        },
                    ),
                ).as_workunit()
                yield MetadataChangeProposalWrapper(
                    entityUrn=assertion_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=str(DataPlatformUrn(platform_name=self.platform))
                    ),
                ).as_workunit()
                self.report.assertions_emitted += 1

            # AssertionRunEvent: one per (assertion, trace)
            result_type = (
                AssertionResultTypeClass.SUCCESS
                if avg_score >= self.config.assertion_score_threshold
                else AssertionResultTypeClass.FAILURE
            )
            timestamp_ms = (
                int(run.end_time.timestamp() * 1000)
                if run.end_time
                else int(time.time() * 1000)
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionRunEventClass(
                    timestampMillis=timestamp_ms,
                    runId=str(run.id),
                    asserteeUrn=dataset_urn,
                    assertionUrn=assertion_urn,
                    status=AssertionRunStatusClass.COMPLETE,
                    result=AssertionResultClass(
                        type=result_type,
                        actualAggValue=avg_score,
                        nativeResults={
                            "feedback_key": key,
                            "avg_score": str(avg_score),
                            "count": str(count),
                            "threshold": str(self.config.assertion_score_threshold),
                            "run_name": run.name or str(run.id),
                        },
                        externalUrl=self._run_url(run),
                    ),
                ),
            ).as_workunit()
            self.report.assertion_run_events_emitted += 1

    # -------------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------------

    def _create_workunit(self, urn: str, aspect: _Aspect) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        ).as_workunit()
