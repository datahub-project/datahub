from typing import Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_data_job_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import (
    EventBridgeClient,
    PipesClient,
)
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    EVENTBRIDGE_PIPE_JOB_ID,
    EVENTBRIDGE_PIPES_PLATFORM,
    EVENTBRIDGE_PLATFORM,
    EVENTBRIDGE_RULE_JOB_ID,
    AwsService,
)
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.mapper import AwsRegionKey
from datahub.ingestion.source.aws_messaging.models import (
    Arn,
    EventBridgePipe,
    EventBridgeRule,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    DatasetSubTypes,
)
from datahub.sdk import DataFlow, DataJob, Dataset
from datahub.sdk._shared import DatasetUrnOrStr


def _non_empty(props: Dict[str, Optional[str]]) -> Dict[str, str]:
    return {key: value for key, value in props.items() if value}


class EventBridgeLineage:
    """EventBridge rules and Pipes that route into or out of a queue or topic.

    A rule is a DataFlow with one `route` DataJob reading its event bus (a
    dataset) and writing its queue and topic targets. A pipe is a DataFlow with
    one `pipe` DataJob from its source to its target. Rules and pipes that only
    touch other services are left out."""

    def __init__(
        self,
        *,
        config: AwsMessagingSourceConfig,
        report: AwsMessagingSourceReport,
        events: EventBridgeClient,
        pipes: PipesClient,
        urns: UrnBuilder,
        resolver: ResourceResolver,
        lambdas: Optional[LambdaLineage],
        region_key: AwsRegionKey,
    ):
        self.config = config
        self.report = report
        self.events = events
        self.pipes = pipes
        self.urns = urns
        self.resolver = resolver
        self.lambdas = lambdas
        self.region_key = region_key

    def _job_urn(self, platform: str, flow_id: str, job_id: str, arn: Arn) -> str:
        return make_data_job_urn(
            orchestrator=platform,
            flow_id=flow_id,
            job_id=job_id,
            cluster=self.config.env,
            platform_instance=self.urns.instance_for(arn.account),
        )

    def _link_lambda(self, target: str, job_urn: str) -> None:
        arn = parse_arn(target)
        if self.lambdas is None or arn is None or arn.service != AwsService.LAMBDA:
            return
        edges = self.lambdas.function(target, job_urn)
        if edges is not None:
            edges.input_jobs.add(job_urn)

    def list_rules(self) -> Iterable[EventBridgeRule]:
        for bus_arn in self.events.list_event_bus_arns():
            bus = parse_arn(bus_arn)
            if bus is None:
                continue
            for rule in self.events.list_rules(bus.resource_name):
                targets = set(
                    self.events.list_target_arns(rule["Name"], bus.resource_name)
                )
                if not any(self.resolver.messaging_urn(parse_arn(t)) for t in targets):
                    continue
                yield EventBridgeRule(
                    bus_arn=bus,
                    name=rule["Name"],
                    state=rule.get("State"),
                    event_pattern=rule.get("EventPattern"),
                    target_arns=targets,
                )

    def rule_workunits(self) -> Iterable[MetadataWorkUnit]:
        emitted_buses: Set[str] = set()
        for rule in self.list_rules():
            bus = rule.bus_arn
            bus_urn = self.urns.dataset_urn(EVENTBRIDGE_PLATFORM, bus)
            if bus_urn not in emitted_buses:
                emitted_buses.add(bus_urn)
                yield from Dataset(
                    platform=EVENTBRIDGE_PLATFORM,
                    name=self.urns.resource_id(bus),
                    display_name=bus.resource_name,
                    env=self.config.env,
                    platform_instance=self.urns.instance_for(bus.account),
                    subtype=DatasetSubTypes.EVENTBRIDGE_EVENT_BUS.value,
                    parent_container=self.region_key,
                    custom_properties={"arn": str(bus)},
                ).as_workunits()
            flow_id = f"{bus.region}.{bus.resource_name}.{rule.name}"
            job_urn = self._job_urn(
                EVENTBRIDGE_PLATFORM, flow_id, EVENTBRIDGE_RULE_JOB_ID, bus
            )
            outlets: List[DatasetUrnOrStr] = []
            for target in sorted(rule.target_arns):
                urn = self.resolver.messaging_urn(parse_arn(target))
                if urn:
                    outlets.append(urn)
                self._link_lambda(target, job_urn)
            flow = DataFlow(
                platform=EVENTBRIDGE_PLATFORM,
                name=flow_id,
                display_name=rule.name,
                platform_instance=self.urns.instance_for(bus.account),
                env=self.config.env,
                subtype=DataFlowSubTypes.EVENTBRIDGE_RULE.value,
                custom_properties=_non_empty(
                    {
                        "event_bus": bus.resource_name,
                        "state": rule.state,
                        "event_pattern": rule.event_pattern,
                    }
                ),
            )
            job = DataJob(
                name=EVENTBRIDGE_RULE_JOB_ID,
                flow=flow,
                display_name=rule.name,
                subtype=DataJobSubTypes.EVENTBRIDGE_ROUTE.value,
                inlets=[bus_urn],
                outlets=outlets,
            )
            self.report.eventbridge_rules_emitted += 1
            yield from flow.as_workunits()
            yield from job.as_workunits()

    def pipe_workunits(self) -> Iterable[MetadataWorkUnit]:
        for pipe in self.pipes.list_pipes():
            yield from self._pipe_workunits(pipe)

    def _pipe_workunits(self, pipe: EventBridgePipe) -> Iterable[MetadataWorkUnit]:
        source_urn = self.resolver.messaging_urn(parse_arn(pipe.source))
        target_urn = self.resolver.messaging_urn(parse_arn(pipe.target))
        if source_urn is None and target_urn is None:
            return
        flow_id = self.urns.resource_id(pipe.arn)
        job_urn = self._job_urn(
            EVENTBRIDGE_PIPES_PLATFORM, flow_id, EVENTBRIDGE_PIPE_JOB_ID, pipe.arn
        )
        self._link_lambda(pipe.target, job_urn)
        flow = DataFlow(
            platform=EVENTBRIDGE_PIPES_PLATFORM,
            name=flow_id,
            display_name=pipe.name,
            platform_instance=self.urns.instance_for(pipe.arn.account),
            env=self.config.env,
            subtype=DataFlowSubTypes.EVENTBRIDGE_PIPE.value,
            custom_properties=_non_empty(
                {
                    "arn": str(pipe.arn),
                    "source": pipe.source,
                    "target": pipe.target,
                    "enrichment": pipe.enrichment,
                    "state": pipe.state,
                }
            ),
        )
        job = DataJob(
            name=EVENTBRIDGE_PIPE_JOB_ID,
            flow=flow,
            display_name=pipe.name,
            subtype=DataJobSubTypes.EVENTBRIDGE_PIPE_STEP.value,
            inlets=[source_urn] if source_urn else [],
            outlets=[target_urn] if target_urn else [],
        )
        self.report.eventbridge_pipes_emitted += 1
        yield from flow.as_workunits()
        yield from job.as_workunits()
