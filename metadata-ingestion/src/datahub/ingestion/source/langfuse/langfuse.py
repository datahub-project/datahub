import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataprocess.dataprocess_instance import DataProcessInstance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes, MLAssetSubTypes
from datahub.ingestion.source.langfuse.langfuse_client import (
    ATTACHABLE_SCORE_SUBJECT_KINDS,
    LangfuseAuthenticationError,
    LangfuseClient,
    LangfuseObservation,
    LangfusePromptVersion,
)
from datahub.ingestion.source.langfuse.langfuse_config import LangfuseSourceConfig
from datahub.ingestion.source.langfuse.langfuse_report import LangfuseSourceReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    DataPlatformInstanceClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
    SubTypesClass,
    VersionPropertiesClass,
    VersionTagClass,
)
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn, VersionSetUrn
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset

logger = logging.getLogger(__name__)


class LangfuseProjectKey(ContainerKey):
    project_id: str


def _iso_to_millis(iso_timestamp: Optional[str]) -> Optional[int]:
    if not iso_timestamp:
        return None
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


@platform_name("Langfuse")
@config_class(LangfuseSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.CONTAINERS, "Enabled by default (Project container)")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Not implemented in this version",
    supported=False,
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled for Prompts only (fully enumerated each run). NOT enabled for "
    "Traces/Generations, which are retrieved through a rolling time window "
    "and are excluded from stale-entity tracking to avoid incorrectly "
    "soft-deleting entities that simply aged out of the window.",
)
class LangfuseSource(StatefulIngestionSourceBase, TestableSource):
    """DataHub source for Langfuse.

    Extracts, within a configurable rolling time window:
    - Traces, as DataProcessInstance entities under a Project container
    - Generation-type Observations, as child DataProcessInstance entities
    - Scores, attached to the corresponding Trace/Generation as MLMetric entries

    Extracts, fully enumerated on every run:
    - Prompts (all versions), as versioned Dataset entities

    Out of scope for this version: Datasets, Dataset Items, Dataset Runs/
    Experiments, Dataset<->Trace lineage, and the Langfuse Model resource.
    """

    platform = "langfuse"

    def __init__(self, ctx: PipelineContext, config: LangfuseSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report: LangfuseSourceReport = LangfuseSourceReport()
        self.client = LangfuseClient(
            connection=config.connection,
            page_size=config.page_size,
        )
        # Set once get_workunits_internal() resolves the project; see
        # _get_project_container_urn() for the guarded accessor.
        self._project_container_urn: Optional[str] = None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "LangfuseSource":
        config = LangfuseSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_report(self) -> LangfuseSourceReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

    def _get_project_container_urn(self) -> str:
        assert self._project_container_urn is not None, (
            "Project container URN accessed before get_workunits_internal() "
            "resolved it."
        )
        return self._project_container_urn

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            project = self.client.get_project()
        except LangfuseAuthenticationError as e:
            self.report.failure(
                title="Failed to authenticate with Langfuse",
                message="Verify connection.public_key and connection.secret_key.",
                exc=e,
            )
            return
        except requests.RequestException as e:
            self.report.failure(
                title="Failed to reach Langfuse",
                message="Could not fetch the project for the configured API key. "
                "Verify connection.host is correct and reachable.",
                exc=e,
            )
            return

        project_key = LangfuseProjectKey(
            platform=str(DataPlatformUrn(platform_name=self.platform)),
            instance=self.config.platform_instance,
            project_id=project["id"],
        )
        self._project_container_urn = str(project_key.as_urn())
        yield from self._emit_project_container(project_key, project)

        if self.config.include_prompts:
            yield from self._get_prompt_workunits()

        if self.config.include_traces:
            yield from self._get_trace_workunits()

    # ------------------------------------------------------------------
    # Project container
    # ------------------------------------------------------------------

    def _emit_project_container(
        self, project_key: LangfuseProjectKey, project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        organization = project.get("organization") or {}
        container = Container(
            container_key=project_key,
            subtype=MLAssetSubTypes.LANGFUSE_PROJECT,
            display_name=project.get("name") or project["id"],
            extra_properties={
                "project_id": project["id"],
                "organization_name": organization.get("name", ""),
            },
        )
        yield from container.as_workunits()

    # ------------------------------------------------------------------
    # Prompts
    # ------------------------------------------------------------------

    def _get_prompt_workunits(self) -> Iterable[MetadataWorkUnit]:
        for prompt_meta in self.client.iter_prompt_names():
            name = prompt_meta["name"]
            if not self.config.prompt_pattern.allowed(name):
                self.report.prompts_filtered += 1
                continue

            self.report.prompts_scanned += 1
            version_set_urn = self._get_prompt_version_set_urn(name)
            for version in prompt_meta.get("versions", []):
                try:
                    prompt_version = self.client.get_prompt_version(name, version)
                except requests.HTTPError as e:
                    self.report.warning(
                        title="Failed to fetch prompt version",
                        message="This prompt version will be skipped.",
                        context=f"name={name}, version={version}",
                        exc=e,
                    )
                    continue
                yield from self._emit_prompt_version(prompt_version, version_set_urn)
                self.report.prompt_versions_scanned += 1

    def _get_prompt_version_set_urn(self, prompt_name: str) -> VersionSetUrn:
        guid_dict = {"platform": self.platform, "name": prompt_name}
        return VersionSetUrn(
            id=builder.datahub_guid(guid_dict),
            entity_type=DatasetUrn.ENTITY_TYPE,
        )

    def _make_prompt_dataset_name(self, prompt: LangfusePromptVersion) -> str:
        return f"{prompt.name}.v{prompt.version}"

    def _emit_prompt_version(
        self,
        prompt: LangfusePromptVersion,
        version_set_urn: VersionSetUrn,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = self._make_prompt_dataset_name(prompt)
        dataset = Dataset(
            platform=self.platform,
            name=dataset_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=f"{prompt.name} (v{prompt.version})",
            custom_properties={
                "prompt_name": prompt.name,
                "prompt_type": prompt.prompt_type,
                "content": self._render_prompt_content(prompt),
                "tags": ",".join(prompt.tags),
                "commit_message": prompt.commit_message or "",
            },
            subtype=DatasetSubTypes.LANGFUSE_PROMPT,
            parent_container=[self._get_project_container_urn()],
        )
        yield from dataset.as_workunits()

        yield MetadataChangeProposalWrapper(
            entityUrn=str(dataset.urn),
            aspect=VersionPropertiesClass(
                versionSet=str(version_set_urn),
                version=VersionTagClass(versionTag=str(prompt.version)),
                sortId=str(prompt.version).zfill(10),
                aliases=[VersionTagClass(versionTag=label) for label in prompt.labels],
            ),
        ).as_workunit()

    @staticmethod
    def _render_prompt_content(prompt: LangfusePromptVersion) -> str:
        if prompt.prompt_type == "chat":
            return json.dumps(prompt.prompt)
        return str(prompt.prompt)

    # ------------------------------------------------------------------
    # Traces / Observations / Scores
    # ------------------------------------------------------------------

    def _get_trace_workunits(self) -> Iterable[MetadataWorkUnit]:
        window = self.config.window
        metrics_by_urn: Dict[str, List[MLMetricClass]] = (
            self._build_score_metrics_map(window.start_time, window.end_time)
            if self.config.include_scores
            else {}
        )

        observations_by_trace: Dict[str, List[LangfuseObservation]] = defaultdict(list)
        try:
            for obs in self.client.iter_observations(
                window.start_time, window.end_time
            ):
                observations_by_trace[obs.trace_id].append(obs)
        except (LangfuseAuthenticationError, requests.RequestException) as e:
            self.report.failure(
                title="Failed to fetch Observations",
                message="Could not retrieve Observations for the configured time "
                "window. Traces from this run will be incomplete or missing.",
                exc=e,
            )
            return

        for trace_id, observations in observations_by_trace.items():
            root = next((o for o in observations if o.is_root_observation), None)
            is_partial_trace = root is None
            if root is None:
                # The trace's root observation started outside the configured
                # window while at least one of its children (a generation)
                # started inside it. Rather than dropping the generation's
                # data, synthesize a minimal trace record from the earliest
                # observation we did retrieve. This is a documented, accepted
                # limitation rather than a silent data drop.
                root = min(observations, key=lambda o: o.start_time or "")

            trace_name = root.trace_name or root.name or trace_id
            if not self.config.trace_name_pattern.allowed(trace_name):
                self.report.traces_filtered += 1
                continue

            generations = [o for o in observations if o.type == "GENERATION"]
            non_generation_count = len(observations) - len(generations)
            self.report.non_generation_observations_skipped += non_generation_count

            yield from self._emit_trace(
                trace_id=trace_id,
                trace_name=trace_name,
                root=root,
                generations=generations,
                non_generation_count=non_generation_count,
                is_partial_trace=is_partial_trace,
                metrics_by_urn=metrics_by_urn,
            )
            self.report.traces_scanned += 1

    def _build_score_metrics_map(
        self, from_timestamp: datetime, to_timestamp: datetime
    ) -> Dict[str, List[MLMetricClass]]:
        metrics_by_urn: Dict[str, List[MLMetricClass]] = defaultdict(list)
        try:
            scores = list(self.client.iter_scores(from_timestamp, to_timestamp))
        except (LangfuseAuthenticationError, requests.RequestException) as e:
            self.report.failure(
                title="Failed to fetch Scores",
                message="Could not retrieve Scores for the configured time window. "
                "Traces/Generations from this run will have no attached metrics.",
                exc=e,
            )
            return metrics_by_urn

        for score in scores:
            if score.subject_kind not in ATTACHABLE_SCORE_SUBJECT_KINDS:
                self.report.report_score_dropped(
                    score.id, score.subject_kind or "unknown"
                )
                continue
            if not score.subject_id:
                self.report.report_score_dropped(score.id, "missing_subject_id")
                continue

            target_urn = str(
                DataProcessInstance(id=score.subject_id, orchestrator=self.platform).urn
            )
            metrics_by_urn[target_urn].append(
                MLMetricClass(
                    name=score.name,
                    description=f"dataType={score.data_type}",
                    value=str(score.value),
                    createdAt=_iso_to_millis(score.timestamp),
                )
            )
            self.report.scores_attached += 1
        return metrics_by_urn

    def _emit_trace(
        self,
        trace_id: str,
        trace_name: str,
        root: LangfuseObservation,
        generations: List[LangfuseObservation],
        non_generation_count: int,
        is_partial_trace: bool,
        metrics_by_urn: Dict[str, List[MLMetricClass]],
    ) -> Iterable[MetadataWorkUnit]:
        trace_urn = str(
            DataProcessInstance(id=trace_id, orchestrator=self.platform).urn
        )

        custom_properties = {
            "trace_id": trace_id,
            "generation_count": str(len(generations)),
            "non_generation_observation_count": str(non_generation_count),
        }
        if is_partial_trace:
            custom_properties["partial_trace"] = "true"
        if root.release:
            custom_properties["release"] = root.release
        if root.tags:
            custom_properties["tags"] = ",".join(root.tags)
        if self.config.include_sessions:
            if root.session_id:
                custom_properties["langfuse_session_id"] = root.session_id
            if root.user_id:
                custom_properties["langfuse_user_id"] = root.user_id

        yield from self._emit_dpi_common_aspects(
            entity_urn=trace_urn,
            native_id=trace_id,
            name=trace_name,
            custom_properties=custom_properties,
            obs=root,
            subtype=MLAssetSubTypes.LANGFUSE_TRACE,
            metrics=metrics_by_urn.get(trace_urn, []),
            external_url=self._make_trace_external_url(trace_id),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=trace_urn,
            aspect=ContainerClass(container=self._get_project_container_urn()),
        ).as_workunit(is_primary_source=False)

        for generation in generations:
            yield from self._emit_generation(generation, trace_urn, metrics_by_urn)
            self.report.generations_scanned += 1

    def _emit_generation(
        self,
        obs: LangfuseObservation,
        trace_urn: str,
        metrics_by_urn: Dict[str, List[MLMetricClass]],
    ) -> Iterable[MetadataWorkUnit]:
        obs_urn = str(DataProcessInstance(id=obs.id, orchestrator=self.platform).urn)

        custom_properties: Dict[str, str] = {
            "langfuse_observation_type": obs.type,
            "trace_id": obs.trace_id,
        }
        if obs.model:
            custom_properties["model"] = obs.model
        if obs.total_cost is not None:
            custom_properties["total_cost"] = str(obs.total_cost)
        if obs.latency is not None:
            custom_properties["latency_ms"] = str(obs.latency)
        if obs.time_to_first_token is not None:
            custom_properties["time_to_first_token_ms"] = str(obs.time_to_first_token)
        if obs.prompt_name:
            custom_properties["prompt_name"] = obs.prompt_name
        if obs.prompt_version is not None:
            custom_properties["prompt_version"] = str(obs.prompt_version)
        for key, value in obs.usage_details.items():
            custom_properties[f"usage_{key}"] = str(value)
        for key, value in obs.cost_details.items():
            custom_properties[f"cost_{key}"] = str(value)

        yield from self._emit_dpi_common_aspects(
            entity_urn=obs_urn,
            native_id=obs.id,
            name=obs.name or obs.id,
            custom_properties=custom_properties,
            obs=obs,
            subtype=MLAssetSubTypes.LANGFUSE_GENERATION,
            metrics=metrics_by_urn.get(obs_urn, []),
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=obs_urn,
            aspect=DataProcessInstanceRelationshipsClass(
                upstreamInstances=[],
                parentInstance=trace_urn,
            ),
        ).as_workunit(is_primary_source=False)

    def _emit_dpi_common_aspects(
        self,
        *,
        entity_urn: str,
        native_id: str,
        name: str,
        custom_properties: Dict[str, str],
        obs: LangfuseObservation,
        subtype: str,
        metrics: List[MLMetricClass],
        external_url: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit the aspect set shared by Trace and Generation DataProcessInstances.

        Every workunit here is emitted with is_primary_source=False - Trace and
        Generation entities are retrieved through a rolling time window, not
        fully enumerated, so they must never be considered by stale-entity
        removal. Otherwise entities that simply aged out of the window would
        be incorrectly soft-deleted on every subsequent run.
        """
        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=DataProcessInstancePropertiesClass(
                name=name,
                customProperties=custom_properties,
                created=self._audit_stamp(obs.start_time),
                externalUrl=external_url,
            ),
        ).as_workunit(is_primary_source=False)

        yield from self._emit_run_event(entity_urn, obs)

        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=MLTrainingRunPropertiesClass(
                id=native_id,
                trainingMetrics=metrics,
            ),
        ).as_workunit(is_primary_source=False)

        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=self._data_platform_instance(),
        ).as_workunit(is_primary_source=False)

        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=SubTypesClass(typeNames=[subtype]),
        ).as_workunit(is_primary_source=False)

    def _emit_run_event(
        self, entity_urn: str, obs: LangfuseObservation
    ) -> Iterable[MetadataWorkUnit]:
        start_millis = _iso_to_millis(obs.start_time)
        end_millis = _iso_to_millis(obs.end_time)

        if start_millis is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.STARTED,
                    timestampMillis=start_millis,
                ),
            ).as_workunit(is_primary_source=False)

        if end_millis is not None:
            result_type = "FAILURE" if obs.level == "ERROR" else "SUCCESS"
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=end_millis,
                    result=DataProcessInstanceRunResultClass(
                        type=result_type,
                        nativeResultType=self.platform,
                    ),
                    durationMillis=(
                        end_millis - start_millis if start_millis is not None else None
                    ),
                ),
            ).as_workunit(is_primary_source=False)

    def _data_platform_instance(self) -> DataPlatformInstanceClass:
        instance_urn = None
        if self.config.platform_instance:
            instance_urn = builder.make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            )
        return DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.platform),
            instance=instance_urn,
        )

    def _audit_stamp(self, iso_timestamp: Optional[str]) -> AuditStampClass:
        millis = _iso_to_millis(iso_timestamp) or int(
            datetime.now(tz=timezone.utc).timestamp() * 1000
        )
        return AuditStampClass(time=millis, actor="urn:li:corpuser:datahub")

    def _make_trace_external_url(self, trace_id: str) -> Optional[str]:
        host = self.config.connection.host
        if host.startswith("http"):
            return f"{host}/trace/{trace_id}"
        return None

    # ------------------------------------------------------------------
    # test_connection
    # ------------------------------------------------------------------

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = LangfuseSourceConfig.model_validate(config_dict)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Invalid configuration: {e}"
            )
            return test_report

        client = LangfuseClient(connection=config.connection, page_size=1)
        try:
            health = client.get_health()
            version = health.get("version", "unknown")
            test_report.basic_connectivity = CapabilityReport(capable=True)

            try:
                client.get_project()
                # Exercises the v2/prompts endpoint; confirms this deployment's
                # public API generation matches what this connector requires
                # (v2/v3 surface, not the legacy v1-only surface some
                # self-hosted deployments disable).
                next(client.iter_prompt_names(), None)
                test_report.capability_report = {
                    SourceCapability.CONTAINERS: CapabilityReport(capable=True),
                }
            except LangfuseAuthenticationError as e:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason=str(e)
                )
            except requests.RequestException as e:
                test_report.capability_report = {
                    SourceCapability.CONTAINERS: CapabilityReport(
                        capable=False,
                        failure_reason=(
                            f"Connected to Langfuse {version}, but a required v2/v3 "
                            f"API call failed: {e}. This connector requires a "
                            "self-hosted Langfuse v4+ deployment (or Langfuse Cloud) "
                            "with the v2/v3 public API endpoints reachable."
                        ),
                    )
                }
        except LangfuseAuthenticationError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        except requests.RequestException as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to Langfuse at {config.connection.host}: {e}",
            )
        finally:
            client.close()

        return test_report
