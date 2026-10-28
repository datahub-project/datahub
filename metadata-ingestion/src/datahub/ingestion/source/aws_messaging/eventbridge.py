from typing import Dict, Iterable, List, Optional, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import (
    EventBridgeClient,
    PipesClient,
)
from datahub.ingestion.source.aws_messaging.config import AwsMessagingConfigBase
from datahub.ingestion.source.aws_messaging.constants import (
    EVENTBRIDGE_PIPE_JOB_ID,
    EVENTBRIDGE_PIPES_PLATFORM,
    EVENTBRIDGE_PLATFORM,
    EVENTBRIDGE_RULE_JOB_ID,
    SNS_PLATFORM,
    SQS_PLATFORM,
    AwsService,
)
from datahub.ingestion.source.aws_messaging.jobs import job_urn, shared_job_workunits
from datahub.ingestion.source.aws_messaging.lambda_lineage import LambdaLineage
from datahub.ingestion.source.aws_messaging.lineage import (
    SchemaLookup,
    identity_field_lineage,
)
from datahub.ingestion.source.aws_messaging.mapper import (
    AwsRegionKey,
    non_empty,
    secondary,
)
from datahub.ingestion.source.aws_messaging.models import (
    Arn,
    EventBridgePipe,
    EventBridgeRule,
)
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    DatasetSubTypes,
)
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.sdk import DataFlow, DataJob, Dataset

MESSAGING_PLATFORMS: Dict[str, str] = {
    AwsService.SQS.value: SQS_PLATFORM,
    AwsService.SNS.value: SNS_PLATFORM,
}


class EventBridgeLineage:
    """EventBridge rules and Pipes that route into or out of this source's queues
    or topics.

    A rule is a DataFlow with one `route` DataJob from its event bus (a dataset)
    to its targets; a pipe is a DataFlow with one `pipe` DataJob from source to
    target. The sqs and sns sources each add only the edges whose dataset end
    they own, so a rule targeting both a queue and a topic gets both outputs."""

    def __init__(
        self,
        *,
        config: AwsMessagingConfigBase,
        report: AwsMessagingReportBase,
        events: EventBridgeClient,
        pipes: PipesClient,
        urns: UrnBuilder,
        resolver: ResourceResolver,
        schemas: SchemaLookup,
        lambdas: Optional[LambdaLineage],
        region_key: AwsRegionKey,
    ):
        self.config = config
        self.report = report
        self.events = events
        self.pipes = pipes
        self.urns = urns
        self.resolver = resolver
        self.schemas = schemas
        self.lambdas = lambdas
        self.region_key = region_key

    def _job_urn(self, platform: str, flow_id: str, job_id: str, arn: Arn) -> str:
        return job_urn(
            platform=platform,
            flow_id=flow_id,
            job_id=job_id,
            env=self.config.env,
            instance=self.urns.instance_for(arn.account),
        )

    def _link_lambda(self, target: str, upstream_job: str) -> None:
        arn = parse_arn(target)
        if self.lambdas is None or arn is None or arn.service != AwsService.LAMBDA:
            return
        edges = self.lambdas.function(target, upstream_job)
        if edges is not None:
            edges.input_jobs.add(upstream_job)

    def list_rules(self) -> Iterable[EventBridgeRule]:
        for bus_arn in self.events.list_event_bus_arns():
            bus = parse_arn(bus_arn)
            if bus is None:
                continue
            for rule in self.events.list_rules(bus.resource_name):
                targets = set(
                    self.events.list_target_arns(rule["Name"], bus.resource_name)
                )
                if not any(self.resolver.urn(parse_arn(t)) for t in targets):
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
            instance = self.urns.instance_for(bus.account)
            bus_urn = self.urns.dataset_urn(EVENTBRIDGE_PLATFORM, bus)
            if bus_urn not in emitted_buses:
                emitted_buses.add(bus_urn)
                yield from secondary(
                    Dataset(
                        platform=EVENTBRIDGE_PLATFORM,
                        name=self.urns.resource_id(bus),
                        display_name=bus.resource_name,
                        env=self.config.env,
                        platform_instance=instance,
                        subtype=DatasetSubTypes.EVENTBRIDGE_EVENT_BUS.value,
                        parent_container=self.region_key,
                        custom_properties={"arn": str(bus)},
                    ).as_workunits()
                )
            flow_id = f"{bus.region}.{bus.resource_name}.{rule.name}"
            rule_job = self._job_urn(
                EVENTBRIDGE_PLATFORM, flow_id, EVENTBRIDGE_RULE_JOB_ID, bus
            )
            outputs: List[str] = []
            for target in sorted(rule.target_arns):
                urn = self.resolver.urn(parse_arn(target))
                if urn:
                    outputs.append(urn)
                self._link_lambda(target, rule_job)
            flow = DataFlow(
                platform=EVENTBRIDGE_PLATFORM,
                name=flow_id,
                display_name=rule.name,
                platform_instance=instance,
                env=self.config.env,
                subtype=DataFlowSubTypes.EVENTBRIDGE_RULE.value,
                custom_properties=non_empty(
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
            )
            self.report.eventbridge_rules_emitted += 1
            yield from shared_job_workunits(
                flow=flow, job=job, inputs=[bus_urn], outputs=outputs
            )

    def pipe_workunits(self) -> Iterable[MetadataWorkUnit]:
        for pipe in self.pipes.list_pipes():
            yield from self._pipe_workunits(pipe)

    def _pipe_workunits(self, pipe: EventBridgePipe) -> Iterable[MetadataWorkUnit]:
        source_urn = self.resolver.urn(parse_arn(pipe.source))
        target_urn = self.resolver.urn(parse_arn(pipe.target))
        if source_urn is None and target_urn is None:
            return
        flow_id = self.urns.resource_id(pipe.arn)
        pipe_job = self._job_urn(
            EVENTBRIDGE_PIPES_PLATFORM, flow_id, EVENTBRIDGE_PIPE_JOB_ID, pipe.arn
        )
        self._link_lambda(pipe.target, pipe_job)
        flow = DataFlow(
            platform=EVENTBRIDGE_PIPES_PLATFORM,
            name=flow_id,
            display_name=pipe.name,
            platform_instance=self.urns.instance_for(pipe.arn.account),
            env=self.config.env,
            subtype=DataFlowSubTypes.EVENTBRIDGE_PIPE.value,
            custom_properties=non_empty(
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
        )
        self.report.eventbridge_pipes_emitted += 1
        yield from shared_job_workunits(
            flow=flow,
            job=job,
            inputs=[source_urn] if source_urn else [],
            outputs=[target_urn] if target_urn else [],
            # The target's owner adds the column lineage, so it's written once.
            fine_grained=self._pipe_column_lineage(pipe, target_urn)
            if target_urn
            else None,
        )

    def _pipe_column_lineage(
        self, pipe: EventBridgePipe, target_urn: str
    ) -> Optional[List[FineGrainedLineageClass]]:
        source_arn = parse_arn(pipe.source)
        platform = MESSAGING_PLATFORMS.get(source_arn.service) if source_arn else None
        if source_arn is None or platform is None:
            self.report.column_lineage_skipped[
                "pipe source is not a queue or topic"
            ] += 1
            return None
        if pipe.enrichment:
            self.report.column_lineage_skipped["pipe has an enrichment step"] += 1
            return None
        source_urn = self.urns.dataset_urn(platform, source_arn)
        source_schema = self.schemas.get(source_urn)
        target_schema = self.schemas.get(target_urn)
        if source_schema is None or target_schema is None:
            self.report.column_lineage_skipped["schema missing on one side"] += 1
            return None
        templated = self.pipes.has_input_template(pipe.name)
        if templated is not False:
            self.report.column_lineage_skipped["pipe has an input template"] += 1
            return None
        lineages = identity_field_lineage(
            upstream_urn=source_urn,
            upstream_schema=source_schema,
            downstream_urn=target_urn,
            downstream_schema=target_schema,
        )
        if not lineages:
            self.report.column_lineage_skipped["no fields in common"] += 1
            return None
        self.report.column_lineage_edges_emitted += len(lineages)
        return lineages
