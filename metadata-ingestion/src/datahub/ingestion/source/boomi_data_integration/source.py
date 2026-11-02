import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Union

import requests
from pydantic import SecretStr, ValidationError

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
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.ingestion.source.boomi_data_integration.river_parser import (
    LogicStepJob,
    flatten_logic_steps,
    parse_source_to_target,
)
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

STAGE_ENVIRONMENTS = "Environments"
STAGE_RIVERS = "Rivers"

_API_ERRORS = (BoomiDataIntegrationApiError, requests.RequestException, ValueError)


class BoomiDIEnvironmentKey(ContainerKey):
    environment_id: str


class BoomiDIGroupKey(BoomiDIEnvironmentKey):
    group_id: str


@dataclass
class EnvironmentAccess:
    environment: Environment
    token: SecretStr


@dataclass
class RiverJobSpec:
    name: str
    display_name: str
    subtype: str
    outputs: List[str]
    upstream_jobs: List[str]
    custom_properties: Dict[str, str]


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
    "Rivers and logic SQL steps to the target tables they load",
)
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
        with self.report.new_stage(STAGE_RIVERS):
            for access in environments:
                yield from self._process_environment(access)
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

    def _process_environment(self, access: EnvironmentAccess) -> Iterable[Entity]:
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
                yield from self._river_entities(access, summary, parent)
                self.report.rivers_emitted += 1
            except Exception as e:
                self.report.warning(
                    title="Failed to process river",
                    message="The river was skipped.",
                    context=f"river_cross_id={summary.river_cross_id}, name={summary.name}",
                    exc=e,
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

    def _river_entities(
        self,
        access: EnvironmentAccess,
        summary: RiverSummary,
        parent: Container,
    ) -> Iterable[Entity]:
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
            parent_container=parent,
        )
        yield flow
        for spec in jobs:
            yield DataJob(
                name=spec.name,
                flow=flow,
                display_name=spec.display_name,
                subtype=spec.subtype,
                custom_properties=spec.custom_properties,
                extra_aspects=[
                    DataJobInputOutputClass(
                        inputDatasets=[],
                        outputDatasets=spec.outputs,
                        inputDatajobs=[
                            make_data_job_urn_with_flow(str(flow.urn), step_id)
                            for step_id in spec.upstream_jobs
                        ],
                    )
                ],
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
            outputs = (
                self.lineage.river_targets(props, context)
                if self.config.include_lineage
                else []
            )
            return [
                RiverJobSpec(
                    name=LOAD_JOB_ID,
                    display_name=display_name,
                    subtype=DataJobSubTypes.BOOMI_DI_LOAD,
                    outputs=outputs,
                    upstream_jobs=[],
                    custom_properties={},
                )
            ]
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
                outputs=[],
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
        specs = [self._logic_step_spec(job, context) for job in flattened.jobs]
        self.report.logic_steps_emitted += len(specs)
        return specs

    def _logic_step_spec(self, job: LogicStepJob, context: str) -> RiverJobSpec:
        node = job.node
        properties = {PROP_STEP_TYPE: node.type}
        if node.type == LogicNodeType.RIVER and node.river_id:
            properties[PROP_CALLED_RIVER_ID] = node.river_id
        outputs: List[str] = []
        if self.config.include_lineage:
            urn = self.lineage.step_target(node, f"{context}, step_id={job.step_id}")
            if urn:
                outputs.append(urn)
        return RiverJobSpec(
            name=job.step_id,
            display_name=node.name or job.step_id,
            subtype=DataJobSubTypes.BOOMI_DI_LOGIC_STEP,
            outputs=outputs,
            upstream_jobs=job.upstream_step_ids,
            custom_properties=properties,
        )
