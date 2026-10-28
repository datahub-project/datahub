import json
import re
from typing import Callable, Dict, Iterable, List, Optional, Set

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
    mapped_field_lineage,
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
    RuleTarget,
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


SIMPLE_JSON_PATH = re.compile(r"^\$(?:\.[A-Za-z0-9_-]+)*$")
# A template placeholder Pipes fills from the decoded SQS message body.
BODY_PLACEHOLDER = re.compile(r"^<\$\.body((?:\.[A-Za-z0-9_-]+)+)>$")
UNQUOTED_PLACEHOLDER = re.compile(r'(?<!")(<\$[^<>"]*>)(?!")')


def event_path_prefix(input_path: Optional[str]) -> Optional[str]:
    """`$.detail` -> `detail`, no InputPath -> "", anything with brackets,
    wildcards or filters -> None."""
    if not input_path or input_path == "$":
        return ""
    if not SIMPLE_JSON_PATH.match(input_path):
        return None
    return input_path[2:]


def template_field_map(template: str) -> Optional[Dict[str, str]]:
    """Target field path -> message field path, when every value in a JSON
    template is exactly one `<$.body.x>` placeholder. None for anything else:
    literals, concatenation, arrays, or placeholders outside the body."""
    # Pipes allows unquoted placeholders for objects and numbers.
    try:
        document = json.loads(UNQUOTED_PLACEHOLDER.sub(r'"\1"', template))
    except ValueError:
        return None
    if not isinstance(document, dict):
        return None
    mapping: Dict[str, str] = {}
    pending = [("", document)]
    while pending:
        prefix, node = pending.pop()
        for key, value in node.items():
            path = f"{prefix}{key}"
            if isinstance(value, dict):
                pending.append((f"{path}.", value))
                continue
            match = BODY_PLACEHOLDER.match(value) if isinstance(value, str) else None
            if match is None:
                return None
            mapping[path] = match.group(1)[1:]
    return mapping or None


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
            for rule in self.events.list_rules(bus.event_bus_name):
                targets = list(
                    self.events.list_targets(rule["Name"], bus.event_bus_name)
                )
                if not any(self.resolver.urn(parse_arn(t.arn)) for t in targets):
                    continue
                yield EventBridgeRule(
                    bus_arn=bus,
                    name=rule["Name"],
                    state=rule.get("State"),
                    event_pattern=rule.get("EventPattern"),
                    targets=targets,
                )

    def rule_workunits(self) -> Iterable[MetadataWorkUnit]:
        emitted_buses: Set[str] = set()
        for rule in self.list_rules():
            bus = rule.bus_arn
            instance = self.urns.instance_for(bus.account)
            bus_urn = self.urns.event_bus_urn(bus)
            if bus_urn not in emitted_buses:
                emitted_buses.add(bus_urn)
                yield from secondary(
                    Dataset(
                        platform=EVENTBRIDGE_PLATFORM,
                        name=self.urns.event_bus_id(bus),
                        display_name=bus.event_bus_name,
                        env=self.config.env,
                        platform_instance=instance,
                        subtype=DatasetSubTypes.EVENTBRIDGE_EVENT_BUS.value,
                        parent_container=self.region_key,
                        custom_properties={"arn": str(bus)},
                    ).as_workunits()
                )
            flow_id = f"{self.urns.event_bus_id(bus)}.{rule.name}"
            rule_job = self._job_urn(
                EVENTBRIDGE_PLATFORM, flow_id, EVENTBRIDGE_RULE_JOB_ID, bus
            )
            outputs: List[str] = []
            fine_grained: List[FineGrainedLineageClass] = []
            for target in sorted(rule.targets, key=lambda t: t.arn):
                urn = self.resolver.urn(parse_arn(target.arn))
                if urn:
                    outputs.append(urn)
                    if self.config.column_lineage:
                        fine_grained.extend(
                            self._rule_column_lineage(bus_urn, target, urn)
                        )
                self._link_lambda(target.arn, rule_job)
            flow = DataFlow(
                platform=EVENTBRIDGE_PLATFORM,
                name=flow_id,
                display_name=rule.name,
                platform_instance=instance,
                env=self.config.env,
                subtype=DataFlowSubTypes.EVENTBRIDGE_RULE.value,
                custom_properties=non_empty(
                    {
                        "event_bus": bus.event_bus_name,
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
                flow=flow,
                job=job,
                inputs=[bus_urn],
                outputs=outputs,
                fine_grained=fine_grained,
            )

    def _rule_column_lineage(
        self, bus_urn: str, target: RuleTarget, target_urn: str
    ) -> List[FineGrainedLineageClass]:
        """The target gets the whole event, or the subtree at InputPath, unless the
        rule replaces it with a constant or an input transformer. The upstream
        schema is the event bus dataset's schema in DataHub."""
        if target.reshaped:
            self._skip("rule target uses an input transformer or constant input")
            return []
        prefix = event_path_prefix(target.input_path)
        if prefix is None:
            self._skip("rule target InputPath is not a simple path")
            return []
        return self._mapped(
            bus_urn,
            target_urn,
            lambda path: f"{prefix}.{path}" if prefix else path,
        )

    def _skip(self, reason: str) -> None:
        self.report.column_lineage_skipped[reason] += 1

    def _mapped(
        self,
        upstream_urn: str,
        downstream_urn: str,
        upstream_path_for: Callable[[str], Optional[str]],
    ) -> List[FineGrainedLineageClass]:
        upstream_schema = self.schemas.get(upstream_urn)
        downstream_schema = self.schemas.get(downstream_urn)
        if upstream_schema is None or downstream_schema is None:
            self._skip("schema missing on one side")
            return []
        lineages = mapped_field_lineage(
            upstream_urn=upstream_urn,
            upstream_schema=upstream_schema,
            downstream_urn=downstream_urn,
            downstream_schema=downstream_schema,
            upstream_path_for=upstream_path_for,
        )
        if not lineages:
            self._skip("no fields in common")
        self.report.column_lineage_edges_emitted += len(lineages)
        return lineages

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
        """Pipes deliver the SQS record (message id, attributes, body, ...), not the
        message, unless an input template picks fields out of `$.body`. Only a
        template whose every value is a single `<$.body.path>` placeholder maps
        fields; see the implicit body parsing section of
        https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-pipes-input-transformation.html
        """
        if not self.config.column_lineage:
            return None
        source_arn = parse_arn(pipe.source)
        if source_arn is None or source_arn.service != AwsService.SQS:
            self._skip("pipe source is not a queue")
            return None
        if pipe.enrichment:
            self._skip("pipe has an enrichment step")
            return None
        source_urn = self.urns.queue_urn(source_arn)
        if self.schemas.get(source_urn) is None or self.schemas.get(target_urn) is None:
            self._skip("schema missing on one side")
            return None
        template = self.pipes.input_template(pipe.name)
        if template is None:
            return None
        if not template:
            self._skip("pipe without an input template delivers the SQS record")
            return None
        mapping = template_field_map(template)
        if mapping is None:
            self._skip("pipe input template is not a plain field mapping")
            return None
        lowered = {key.lower(): value for key, value in mapping.items()}
        return (
            self._mapped(source_urn, target_urn, lambda path: lowered.get(path.lower()))
            or None
        )
