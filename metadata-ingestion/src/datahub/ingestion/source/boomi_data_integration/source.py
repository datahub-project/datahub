import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Union

import requests
from pydantic import SecretStr, ValidationError

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    DataProcessInstanceKey,
)
from datahub.emitter.mce_builder import make_data_job_urn_with_flow
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
from datahub.ingestion.source.boomi_data_integration.client import (
    BoomiDataIntegrationApiError,
    BoomiDataIntegrationClient,
)
from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
    EnvironmentToken,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    ACCESS_DENIED_STATUS_CODES,
    CONSOLE_LINKED_RIVER_TYPES,
    HTTP_BAD_REQUEST,
    LOAD_JOB_ID,
    PLATFORM,
    PROP_CALLED_RIVER_ID,
    PROP_DATASOURCE_ID,
    PROP_ENVIRONMENT_ID,
    PROP_GROUP_ID,
    PROP_IS_DEFAULT,
    PROP_RIVER_STATUS,
    PROP_RIVER_TYPE,
    PROP_RUN_TYPE,
    PROP_SCHEDULE,
    PROP_SOURCE_NAME,
    PROP_STEP_TYPE,
    PROP_TARGET_NAME,
    RIVER_CONSOLE_URL_TEMPLATE,
    RUN_CONSOLE_URL_TEMPLATE,
    RUN_JOB_ID,
    SCHEDULE_SEPARATOR,
    UNREACHABLE_ENVIRONMENTS_TEMPLATE,
    UNREACHABLE_SEPARATOR,
    LogicNodeType,
    RiverType,
)
from datahub.ingestion.source.boomi_data_integration.lineage import (
    TargetLineageResolver,
)
from datahub.ingestion.source.boomi_data_integration.models import (
    Environment,
    River,
    RiverSummary,
    SourceToTargetProperties,
)
from datahub.ingestion.source.boomi_data_integration.owners import (
    OwnerResolver,
    Owners,
)
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.ingestion.source.boomi_data_integration.river_parser import (
    LogicStepJob,
    flatten_logic_steps,
    parse_source_to_target,
)
from datahub.ingestion.source.boomi_data_integration.run_history import (
    RunGroup,
    RunSelection,
    select_recent_runs,
)
from datahub.ingestion.source.boomi_data_integration.source_lineage import (
    JobLineage,
    SourceLineageResolver,
)
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    FineGrainedLineageClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn, DataProcessInstanceUrn
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

STAGE_ENVIRONMENTS = "Environments"
STAGE_RIVERS = "Rivers"
STAGE_RUNS = "Runs"

_API_ERRORS = (BoomiDataIntegrationApiError, requests.RequestException, ValueError)


class BoomiDIEnvironmentKey(ContainerKey):
    environment_id: str


class BoomiDIGroupKey(BoomiDIEnvironmentKey):
    group_id: str


@dataclass
class EnvironmentAccess:
    environment: Environment
    token: SecretStr


# river_cross_id -> the flow or job each run of that river instantiates.
RunTemplates = Dict[str, Union[DataFlowUrn, DataJobUrn]]


@dataclass
class RiverJobSpec:
    name: str
    display_name: str
    subtype: str
    upstream_jobs: List[str]
    custom_properties: Dict[str, str]
    # Whether a logic river step that calls this river waits for this job.
    is_exit: bool = True
    called_river_id: Optional[str] = None
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    fine_grained: List[FineGrainedLineageClass] = field(default_factory=list)

    def set_lineage(self, lineage: JobLineage) -> None:
        self.inputs = sorted(lineage.inputs)
        self.outputs = sorted(lineage.outputs)
        self.fine_grained = lineage.fine_grained()


@dataclass
class PendingRiver:
    # Jobs are emitted after every river of the environment, so a logic step
    # that calls another river can link to it only when that river is emitted.
    flow: DataFlow
    river_type: str
    owners: Optional[Owners]
    jobs: List[RiverJobSpec]

    def exit_job_urns(self) -> List[DataJobUrn]:
        return [
            DataJobUrn(self.flow.urn, spec.name) for spec in self.jobs if spec.is_exit
        ]


def _is_access_denied(error: Exception) -> bool:
    return (
        isinstance(error, BoomiDataIntegrationApiError)
        and error.status_code in ACCESS_DENIED_STATUS_CODES
    )


def _test_environment_access(
    client: BoomiDataIntegrationClient, entries: List[EnvironmentToken]
) -> TestConnectionReport:
    # Any token can read every environment record of the account, but only the
    # rivers of the environment it is scoped to, so probe the rivers. Ingestion
    # skips an environment it cannot read and carries on, so the connection
    # only fails when no environment can be read.
    unreachable: List[str] = []
    for entry in entries:
        try:
            client.check_river_access(entry.environment_id, entry.api_token)
        except _API_ERRORS as e:
            unreachable.append(f"{entry.environment_id} ({e})")
    if entries and len(unreachable) == len(entries):
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(
                capable=False,
                failure_reason=UNREACHABLE_ENVIRONMENTS_TEMPLATE.format(
                    environments=UNREACHABLE_SEPARATOR.join(unreachable)
                ),
            )
        )
    return TestConnectionReport(
        basic_connectivity=CapabilityReport(
            capable=True,
            mitigation_message=(
                UNREACHABLE_ENVIRONMENTS_TEMPLATE.format(
                    environments=UNREACHABLE_SEPARATOR.join(unreachable)
                )
                if unreachable
                else None
            ),
        )
    )


def _schedule(summary: RiverSummary, river: Optional[River]) -> Optional[str]:
    if river is not None and river.schedulers is not None:
        crons = [
            s.cron_expression
            for s in river.schedulers
            if s.is_enabled and s.cron_expression
        ]
    else:
        crons = list(summary.river_schedulers)
    return SCHEDULE_SEPARATOR.join(sorted(set(crons))) or None


@platform_name("Boomi Data Integration")
@config_class(BoomiDataIntegrationSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Environments and groups as containers")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "From the tables rivers and logic SQL steps read to the tables they load",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Multi-table column mappings and SQL in custom queries and logic SQL steps",
)
@capability(SourceCapability.OWNERSHIP, "Optionally enabled via `extract_owners`")
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class BoomiDataIntegrationSource(StatefulIngestionSourceBase, TestableSource):
    config: BoomiDataIntegrationSourceConfig
    report: BoomiDataIntegrationSourceReport
    platform: str = PLATFORM

    def __init__(self, config: BoomiDataIntegrationSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = BoomiDataIntegrationSourceReport()
        self.client = BoomiDataIntegrationClient(config, self.report)
        self.lineage = TargetLineageResolver(config, self.report)
        self._unknown_run_statuses: Set[str] = set()
        self.source_lineage = SourceLineageResolver(
            config, self.report, self.lineage, ctx.graph
        )
        self.owner_resolver = OwnerResolver(self.client, self.report)

    @classmethod
    def create(
        cls, config_dict: Dict, ctx: PipelineContext
    ) -> "BoomiDataIntegrationSource":
        config = BoomiDataIntegrationSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = BoomiDataIntegrationSourceConfig.model_validate(config_dict)
        except (ValidationError, ValueError) as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )
        client = BoomiDataIntegrationClient(config, BoomiDataIntegrationSourceReport())
        try:
            entries: List[EnvironmentToken] = list(config.environment_tokens or [])
            if config.api_token is not None:
                token = config.api_token
                entries = [
                    EnvironmentToken(
                        environment_id=environment.cross_id, api_token=token
                    )
                    for environment in client.list_environments(token)
                    if not environment.is_deleted
                ]
            return _test_environment_access(client, entries)
        except _API_ERRORS as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=str(e)
                )
            )
        finally:
            client.close()

    def get_report(self) -> BoomiDataIntegrationSourceReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        with self.report.new_stage(STAGE_ENVIRONMENTS):
            environments = self._discover_environments()
        run_templates: Dict[str, RunTemplates] = {}
        with self.report.new_stage(STAGE_RIVERS):
            for access in environments:
                templates = run_templates.setdefault(access.environment.cross_id, {})
                yield from self._process_environment(access, templates)
        if self.config.include_run_history:
            with self.report.new_stage(STAGE_RUNS):
                for access in environments:
                    yield from self._emit_runs(
                        access, run_templates[access.environment.cross_id]
                    )
        if self.report.rivers_without_definition:
            self.report.warning(
                title="River definitions unavailable",
                message=(
                    "The API refused to return the definition of some rivers "
                    "(HTTP 400), which it does for rivers created in the UI. "
                    "They are ingested without lineage or logic steps."
                ),
                context=f"{self.report.rivers_without_definition} rivers",
            )

    def _discover_environments(self) -> List[EnvironmentAccess]:
        found: List[EnvironmentAccess] = []
        if self.config.environment_tokens:
            for entry in sorted(
                self.config.environment_tokens, key=lambda e: e.environment_id
            ):
                try:
                    environment = self.client.get_environment(
                        entry.environment_id, entry.api_token
                    )
                except _API_ERRORS as e:
                    self._report_inaccessible_environment(entry.environment_id, e)
                    continue
                found.append(EnvironmentAccess(environment, entry.api_token))
        elif self.config.api_token is not None:
            token = self.config.api_token
            try:
                found = [
                    EnvironmentAccess(environment, token)
                    for environment in self.client.list_environments(token)
                ]
            except _API_ERRORS as e:
                self.report.failure(
                    title="Failed to list Boomi Data Integration environments",
                    message="Nothing was ingested. Check the token has the environment:list scope.",
                    exc=e,
                )
                return []
        allowed: List[EnvironmentAccess] = []
        for access in sorted(found, key=lambda a: a.environment.cross_id):
            environment = access.environment
            self.report.environments_scanned += 1
            if environment.is_deleted or not self.config.environment_pattern.allowed(
                environment.environment_name
            ):
                self.report.environments_filtered.append(environment.environment_name)
                continue
            allowed.append(access)
        return allowed

    def _report_inaccessible_environment(
        self, environment_id: str, error: Exception
    ) -> None:
        self.report.environments_inaccessible.append(environment_id)
        context = f"environment_id={environment_id}"
        # Anything short of a listed-but-out-of-scope environment is reported
        # as a failure: a skipped environment would otherwise look deleted to
        # stale entity removal, which only stands down after a failure.
        if self.config.environment_tokens is not None:
            self.report.failure(
                title="Configured environment not accessible",
                message=(
                    "An environment in environment_tokens could not be read, so it "
                    "was skipped and stale entity removal is skipped for this run. "
                    "Fix its token, or remove it from environment_tokens."
                ),
                context=context,
                exc=error,
            )
        elif _is_access_denied(error):
            self.report.warning(
                title="Environment not accessible",
                message=(
                    "The API token is not allowed to read this environment, so it "
                    "was skipped. Tokens are scoped to one environment; use "
                    "environment_tokens to ingest several."
                ),
                context=context,
                exc=error,
            )
        else:
            self.report.failure(
                title="Failed to read environment",
                message=(
                    "The environment was skipped and stale entity removal is "
                    "skipped for this run."
                ),
                context=context,
                exc=error,
            )

    def _environment_key(self, environment_id: str) -> BoomiDIEnvironmentKey:
        return BoomiDIEnvironmentKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            environment_id=environment_id,
        )

    def _group_key(self, environment_id: str, group_id: str) -> BoomiDIGroupKey:
        return BoomiDIGroupKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            environment_id=environment_id,
            group_id=group_id,
        )

    def _process_environment(
        self, access: EnvironmentAccess, templates: RunTemplates
    ) -> Iterable[Entity]:
        environment = access.environment
        try:
            summaries = sorted(
                self.client.list_rivers(environment.cross_id, access.token),
                key=lambda r: r.river_cross_id,
            )
        except _API_ERRORS as e:
            self._report_inaccessible_environment(environment.cross_id, e)
            return

        environment_container = Container(
            self._environment_key(environment.cross_id),
            display_name=environment.environment_name,
            subtype=DatasetContainerSubTypes.BOOMI_DI_ENVIRONMENT,
            extra_properties={
                PROP_ENVIRONMENT_ID: environment.cross_id,
                PROP_IS_DEFAULT: str(environment.is_default).lower(),
            },
        )
        self.report.environments_emitted += 1
        yield environment_container

        groups: Dict[str, Container] = {}
        denied_groups: Set[str] = set()
        pending_rivers: List[PendingRiver] = []
        river_exits: Dict[str, List[DataJobUrn]] = {}
        for summary in summaries:
            self.report.rivers_scanned += 1
            if not self.config.river_pattern.allowed(summary.name):
                self.report.rivers_filtered.append(summary.name)
                continue
            group_name = summary.group_name or summary.group_id or ""
            if not self.config.group_pattern.allowed(group_name):
                self.report.rivers_filtered.append(summary.name)
                if group_name not in denied_groups:
                    denied_groups.add(group_name)
                    self.report.groups_filtered.append(group_name)
                continue
            parent = environment_container
            if summary.group_id:
                if summary.group_id not in groups:
                    group = Container(
                        self._group_key(environment.cross_id, summary.group_id),
                        display_name=group_name,
                        subtype=DatasetContainerSubTypes.BOOMI_DI_GROUP,
                        parent_container=environment_container,
                        extra_properties={PROP_GROUP_ID: summary.group_id},
                    )
                    groups[summary.group_id] = group
                    self.report.groups_emitted += 1
                    yield group
                parent = groups[summary.group_id]
            try:
                pending = self._make_river(access, summary, parent)
            except Exception as e:
                self.report.warning(
                    title="Failed to process river",
                    message="The river was skipped.",
                    context=f"river_cross_id={summary.river_cross_id}, name={summary.name}",
                    exc=e,
                )
                continue
            yield pending.flow
            self.report.rivers_emitted += 1
            pending_rivers.append(pending)
            river_exits[summary.river_cross_id] = pending.exit_job_urns()
            templates[summary.river_cross_id] = self._run_template(pending)
        for pending in pending_rivers:
            # Jobs are built here rather than in the per-river try above, so
            # they need their own guard to keep one bad river from ending the
            # run.
            try:
                jobs = list(self._river_jobs(pending, river_exits))
            except Exception as e:
                templates.pop(pending.flow.urn.flow_id, None)
                self.report.warning(
                    title="Failed to process river jobs",
                    message="The river's jobs were skipped.",
                    context=f"river_cross_id={pending.flow.urn.flow_id}, name={pending.flow.display_name}",
                    exc=e,
                )
                continue
            yield from jobs

    def _river_url(
        self, environment_id: str, river_id: str, river_type: str
    ) -> Optional[str]:
        console_url = self.config.resolved_console_url
        if console_url is None or river_type not in CONSOLE_LINKED_RIVER_TYPES:
            return None
        return RIVER_CONSOLE_URL_TEMPLATE.format(
            console_url=console_url,
            account_id=self.config.account_id,
            environment_id=environment_id,
            river_id=river_id,
        )

    def _fetch_river(
        self, access: EnvironmentAccess, summary: RiverSummary
    ) -> Optional[River]:
        # The spec says only API-created rivers can be read, but other docs say
        # otherwise, so always try and fall back to the list fields.
        try:
            return self.client.get_river(
                access.environment.cross_id, summary.river_cross_id, access.token
            )
        except _API_ERRORS as e:
            if (
                isinstance(e, BoomiDataIntegrationApiError)
                and e.status_code == HTTP_BAD_REQUEST
            ):
                self.report.rivers_without_definition += 1
                self.report.rivers_without_definition_ids.append(summary.river_cross_id)
                return None
            self.report.river_definition_failures.append(summary.river_cross_id)
            self.report.warning(
                title="Failed to read river definition",
                message="The river is ingested without lineage or logic steps.",
                context=f"river_cross_id={summary.river_cross_id}, name={summary.name}",
                exc=e,
            )
            return None

    def _make_river(
        self,
        access: EnvironmentAccess,
        summary: RiverSummary,
        parent: Container,
    ) -> PendingRiver:
        river = self._fetch_river(access, summary)
        river_type = river.type if river is not None else summary.river_type
        custom_properties: Dict[str, str] = {PROP_RIVER_TYPE: river_type}
        river_status = (
            river.metadata.river_status if river is not None else None
        ) or summary.river_status
        if river_status:
            custom_properties[PROP_RIVER_STATUS] = river_status
        schedule = _schedule(summary, river)
        if schedule:
            custom_properties[PROP_SCHEDULE] = schedule
        if summary.datasource_id:
            custom_properties[PROP_DATASOURCE_ID] = summary.datasource_id

        jobs = self._job_specs(summary, river, river_type, custom_properties)
        owners: Optional[Owners] = None
        if self.config.extract_owners and river is not None:
            owners = self.owner_resolver.owners(
                river.metadata, access.environment.cross_id, access.token
            )

        description = (
            river.metadata.description if river is not None else None
        ) or summary.description
        created: Optional[datetime] = summary.created_at
        last_modified: Optional[datetime] = summary.last_updated_at
        if river is not None:
            created = river.metadata.created_at or created
            last_modified = river.metadata.last_updated_at or last_modified

        flow = DataFlow(
            platform=PLATFORM,
            name=summary.river_cross_id,
            display_name=river.name if river is not None else summary.name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=description or None,
            custom_properties=custom_properties,
            created=created,
            last_modified=last_modified,
            subtype=DataFlowSubTypes.BOOMI_DI_RIVER,
            owners=owners,
            parent_container=parent,
            external_url=self._river_url(
                access.environment.cross_id, summary.river_cross_id, river_type
            ),
        )
        return PendingRiver(flow=flow, river_type=river_type, owners=owners, jobs=jobs)

    @staticmethod
    def _run_template(pending: PendingRiver) -> Union[DataFlowUrn, DataJobUrn]:
        # A logic river run spans every step, so it instantiates the river's
        # flow; other rivers run as their single load or run job.
        if pending.river_type != RiverType.LOGIC:
            for spec in pending.jobs:
                if spec.name in (LOAD_JOB_ID, RUN_JOB_ID):
                    return DataJobUrn(pending.flow.urn, spec.name)
        return pending.flow.urn

    def _called_river_jobs(
        self,
        pending: PendingRiver,
        spec: RiverJobSpec,
        river_exits: Dict[str, List[DataJobUrn]],
    ) -> List[str]:
        if spec.called_river_id is None:
            return []
        called = river_exits.get(spec.called_river_id)
        # A river that was filtered out, failed, or lives in another
        # environment has no job here to link to, and a river calling itself
        # would only add a cycle.
        if not called or spec.called_river_id == pending.flow.urn.flow_id:
            self.report.river_step_links_unresolved += 1
            return []
        self.report.river_step_links_resolved += 1
        return [str(urn) for urn in called]

    def _river_jobs(
        self, pending: PendingRiver, river_exits: Dict[str, List[DataJobUrn]]
    ) -> Iterable[DataJob]:
        flow = pending.flow
        for spec in pending.jobs:
            input_jobs = [
                make_data_job_urn_with_flow(str(flow.urn), step_id)
                for step_id in spec.upstream_jobs
            ]
            input_jobs.extend(self._called_river_jobs(pending, spec, river_exits))
            # An empty input/output aspect would overwrite lineage added in the
            # UI or by other sources, so it is only written when there is some.
            io_aspects: List[DataJobInputOutputClass] = []
            if spec.inputs or spec.outputs or input_jobs:
                self.report.column_lineage_edges_emitted += len(spec.fine_grained)
                io_aspects.append(
                    DataJobInputOutputClass(
                        inputDatasets=spec.inputs,
                        outputDatasets=spec.outputs,
                        inputDatajobs=list(dict.fromkeys(input_jobs)),
                        fineGrainedLineages=spec.fine_grained or None,
                    )
                )
            yield DataJob(
                name=spec.name,
                flow=flow,
                display_name=spec.display_name,
                subtype=spec.subtype,
                custom_properties=spec.custom_properties,
                owners=pending.owners,
                extra_aspects=io_aspects,
            )

    def _job_specs(
        self,
        summary: RiverSummary,
        river: Optional[River],
        river_type: str,
        flow_properties: Dict[str, str],
    ) -> List[RiverJobSpec]:
        display_name = river.name if river is not None else summary.name
        context = f"river_cross_id={summary.river_cross_id}"
        props: Optional[SourceToTargetProperties] = None
        if river is not None and river_type == RiverType.SOURCE_TO_TARGET:
            try:
                props = parse_source_to_target(river)
            except ValidationError as e:
                self.report.river_definition_failures.append(summary.river_cross_id)
                self.report.warning(
                    title="Unexpected river definition",
                    message="The river definition did not match the expected shape. The river is ingested without lineage.",
                    context=context,
                    exc=e,
                )
        if props is not None:
            if props.source.run_type:
                flow_properties[PROP_RUN_TYPE] = props.source.run_type
            flow_properties[PROP_SOURCE_NAME] = props.source.name
            flow_properties[PROP_TARGET_NAME] = props.target.name
            spec = RiverJobSpec(
                name=LOAD_JOB_ID,
                display_name=display_name,
                subtype=DataJobSubTypes.BOOMI_DI_LOAD,
                upstream_jobs=[],
                custom_properties={},
            )
            if self.config.include_lineage:
                spec.set_lineage(self.source_lineage.river_lineage(props, context))
            return [spec]
        if river is not None and river_type == RiverType.LOGIC:
            return self._logic_job_specs(river, context)
        is_load = river_type == RiverType.SOURCE_TO_TARGET
        return [
            RiverJobSpec(
                name=LOAD_JOB_ID if is_load else RUN_JOB_ID,
                display_name=display_name,
                subtype=(
                    DataJobSubTypes.BOOMI_DI_LOAD
                    if is_load
                    else DataJobSubTypes.BOOMI_DI_RUN
                ),
                upstream_jobs=[],
                custom_properties={},
            )
        ]

    def _logic_job_specs(self, river: River, context: str) -> List[RiverJobSpec]:
        try:
            flattened = flatten_logic_steps(river)
        except ValidationError as e:
            self.report.river_definition_failures.append(river.cross_id)
            self.report.warning(
                title="Unexpected river definition",
                message="The river definition did not match the expected shape. The river is ingested without lineage.",
                context=context,
                exc=e,
            )
            return []
        self.report.logic_steps_disabled += flattened.disabled
        if flattened.invalid:
            self.report.logic_steps_invalid += flattened.invalid
            self.report.warning(
                title="Invalid logic steps",
                message="Some logic steps did not match the expected shape and were skipped.",
                context=f"{context}, skipped={flattened.invalid}",
            )
        exits = set(flattened.exit_step_ids)
        specs = [
            self._logic_step_spec(job, context, job.step_id in exits)
            for job in flattened.jobs
        ]
        self.report.logic_steps_emitted += len(specs)
        return specs

    def _logic_step_spec(
        self, job: LogicStepJob, context: str, is_exit: bool
    ) -> RiverJobSpec:
        node = job.node
        properties = {PROP_STEP_TYPE: node.type}
        called_river_id: Optional[str] = None
        if node.type == LogicNodeType.RIVER and node.river_id:
            called_river_id = node.river_id
            properties[PROP_CALLED_RIVER_ID] = called_river_id
        spec = RiverJobSpec(
            name=job.step_id,
            display_name=node.name or job.step_id,
            subtype=DataJobSubTypes.BOOMI_DI_LOGIC_STEP,
            upstream_jobs=job.upstream_step_ids,
            custom_properties=properties,
            is_exit=is_exit,
            called_river_id=called_river_id,
        )
        if self.config.include_lineage:
            spec.set_lineage(
                self.source_lineage.step_lineage(
                    node, f"{context}, step_id={job.step_id}"
                )
            )
        return spec

    def _emit_runs(
        self, access: EnvironmentAccess, templates: RunTemplates
    ) -> Iterable[MetadataWorkUnit]:
        if not templates:
            return
        environment_id = access.environment.cross_id
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=self.config.run_history_days)
        # Activities summarise the window per river, so runs are only listed
        # for rivers that actually ran.
        ran: Set[str] = set()
        try:
            for activity in self.client.list_activity_summaries(
                environment_id, access.token, start, end
            ):
                river_id = activity.emitted_river_id
                if river_id in templates:
                    ran.add(river_id)
                else:
                    self.report.runs_river_not_emitted += activity.run_count
        except _API_ERRORS as e:
            self._report_runs_unavailable(environment_id, e)
            return

        selections: Dict[str, RunSelection] = {}
        for river_id in sorted(ran):
            try:
                selection = select_recent_runs(
                    self.client.list_river_runs(
                        environment_id, river_id, access.token, start, end
                    ),
                    self.config.max_runs_per_river,
                )
            except _API_ERRORS as e:
                if _is_access_denied(e):
                    self._report_runs_unavailable(environment_id, e)
                    return
                self.report.warning(
                    title="Failed to list river runs",
                    message="Run history for this river is skipped.",
                    context=f"environment_id={environment_id}, river_cross_id={river_id}",
                    exc=e,
                )
                continue
            self.report.runs_scanned += selection.scanned
            self.report.runs_over_cap += selection.over_cap
            selections[river_id] = selection

        # A logic river triggers other rivers, whose runs name the logic run
        # that started them.
        run_groups: Dict[str, str] = {
            run_id: group.group_id
            for selection in selections.values()
            for group in selection.groups
            for run_id in group.run_ids
        }
        for river_id in sorted(selections):
            for group in selections[river_id].groups:
                yield from self._run_workunits(
                    group,
                    templates[river_id],
                    run_groups,
                    self._run_url(environment_id, river_id, group),
                )
                self.report.runs_emitted += 1

    def _run_url(
        self, environment_id: str, river_id: str, group: RunGroup
    ) -> Optional[str]:
        console_url = self.config.resolved_console_url
        # The console links runs by run group; rows without one have no
        # confirmed link format.
        if console_url is None or group.run_group_id is None:
            return None
        return RUN_CONSOLE_URL_TEMPLATE.format(
            console_url=console_url,
            account_id=self.config.account_id,
            environment_id=environment_id,
            river_id=river_id,
            run_group_id=group.run_group_id,
        )

    def _report_runs_unavailable(self, environment_id: str, error: Exception) -> None:
        self.report.runs_unavailable_environments.append(environment_id)
        context = f"environment_id={environment_id}"
        if _is_access_denied(error):
            self.report.warning(
                title="Cannot read Boomi Data Integration run history",
                message=(
                    "The API token lacks the activity:list scope, so run history "
                    "is skipped for this environment."
                ),
                context=context,
                exc=error,
            )
        else:
            # Runs are supplementary and are not part of stale state, so a
            # failure here must not stand down stale removal of the rivers
            # that were emitted correctly.
            self.report.warning(
                title="Failed to list Boomi Data Integration activities",
                message="Run history is skipped for this environment.",
                context=context,
                exc=error,
            )

    def _run_workunits(
        self,
        group: RunGroup,
        template: Union[DataFlowUrn, DataJobUrn],
        run_groups: Dict[str, str],
        url: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        unknown = group.unknown_statuses
        if unknown:
            self.report.runs_unknown_status += 1
            for status in sorted(unknown - self._unknown_run_statuses):
                self._unknown_run_statuses.add(status)
                self.report.warning(
                    title="Unknown Boomi Data Integration run status",
                    message="Runs with this status are emitted with a start event only.",
                    context=f"status={status}",
                )
        parent_group = (
            run_groups.get(group.triggered_by_run_id)
            if group.triggered_by_run_id
            else None
        )
        dpi = DataProcessInstance(
            id=group.group_id,
            orchestrator=PLATFORM,
            cluster=self.config.env,
            type=group.run_type,
            template_urn=template,
            parent_instance=(
                self._run_urn(parent_group)
                if parent_group and parent_group != group.group_id
                else None
            ),
            properties=group.properties(),
            data_platform_instance=self.config.platform_instance,
            url=url,
        )
        start_millis = group.start_millis
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        for mcp in dpi.start_event_mcp(start_millis):
            yield mcp.as_workunit()
        result = group.result
        if result is None:
            return
        end_millis = group.end_millis
        # Without an end time the result is recorded at the start time with no
        # duration rather than a made-up one.
        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=end_millis if end_millis is not None else start_millis,
            result=result,
            result_type=group.status,
            start_timestamp_millis=start_millis if end_millis is not None else None,
        ):
            yield mcp.as_workunit()

    def _run_urn(self, group_id: str) -> DataProcessInstanceUrn:
        # The key DataProcessInstance hashes, so the URN matches that run's own.
        return DataProcessInstanceUrn(
            DataProcessInstanceKey(
                cluster=self.config.env, orchestrator=PLATFORM, id=group_id
            ).guid()
        )
