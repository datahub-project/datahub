from typing import Dict, Iterable, Optional

from datahub.emitter.mce_builder import make_data_job_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import LambdaClient
from datahub.ingestion.source.aws_messaging.config import AwsMessagingSourceConfig
from datahub.ingestion.source.aws_messaging.constants import (
    LAMBDA_JOB_ID,
    LAMBDA_PLATFORM,
    AwsService,
)
from datahub.ingestion.source.aws_messaging.models import Arn, LambdaFunctionEdges
from datahub.ingestion.source.aws_messaging.report import AwsMessagingSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sdk import DataFlow, DataJob


class LambdaLineage:
    """Lambda functions that read from or write to a queue or topic.

    Each function becomes a DataFlow with a single `invoke` DataJob, the same
    shape the kinesis source uses for Firehose. Only functions in the recipe's
    own account and region are emitted, so two recipes never write the same
    function's inputs and outputs."""

    def __init__(
        self,
        *,
        config: AwsMessagingSourceConfig,
        report: AwsMessagingSourceReport,
        client: LambdaClient,
        urns: UrnBuilder,
        resolver: ResourceResolver,
    ):
        self.config = config
        self.report = report
        self.client = client
        self.urns = urns
        self.resolver = resolver
        self.functions: Dict[str, LambdaFunctionEdges] = {}

    def flow_id(self, arn: Arn) -> str:
        return self.urns.resource_id(arn)

    def job_urn(self, arn: Arn) -> str:
        return make_data_job_urn(
            orchestrator=LAMBDA_PLATFORM,
            flow_id=self.flow_id(arn),
            job_id=LAMBDA_JOB_ID,
            cluster=self.config.env,
            platform_instance=self.urns.instance_for(arn.account),
        )

    def function(
        self, function_arn: str, context: str
    ) -> Optional[LambdaFunctionEdges]:
        arn = parse_arn(function_arn)
        if arn is None or arn.service != AwsService.LAMBDA:
            self.report.report_edge_skipped(
                context, function_arn, "not a Lambda function ARN"
            )
            return None
        # Drop a version or alias qualifier: `function:name:alias`.
        resource_parts = arn.resource.split(":")
        unqualified = arn.model_copy(update={"resource": ":".join(resource_parts[:2])})
        if not self.resolver.is_local(unqualified):
            self.report.report_edge_skipped(
                context, function_arn, "Lambda function is in another account or region"
            )
            return None
        key = str(unqualified)
        if key not in self.functions:
            self.functions[key] = LambdaFunctionEdges(arn=unqualified)
        return self.functions[key]

    def collect(self) -> None:
        for mapping in self.client.list_event_source_mappings():
            source_urn = self.resolver.messaging_urn(
                parse_arn(mapping.get("EventSourceArn"))
            )
            failure = (
                (mapping.get("DestinationConfig") or {}).get("OnFailure") or {}
            ).get("Destination")
            failure_urn = self.resolver.messaging_urn(parse_arn(failure))
            if source_urn is None and failure_urn is None:
                continue
            edges = self.function(
                mapping.get("FunctionArn", ""), "event source mapping"
            )
            if edges is None:
                continue
            if source_urn:
                edges.inputs.add(source_urn)
            if failure_urn:
                edges.outputs.add(failure_urn)
        if self.config.lineage.lambda_async_destinations:
            for function in self.client.list_functions():
                for destination in self.client.list_async_destinations(
                    function["FunctionName"]
                ):
                    urn = self.resolver.messaging_urn(parse_arn(destination))
                    if urn is None:
                        continue
                    edges = self.function(
                        function["FunctionArn"], "async invoke destination"
                    )
                    if edges is not None:
                        edges.outputs.add(urn)

    def workunits(self) -> Iterable[MetadataWorkUnit]:
        for key in sorted(self.functions):
            edges = self.functions[key]
            arn = edges.arn
            instance = self.urns.instance_for(arn.account)
            console = (
                f"https://{arn.region}.console.aws.amazon.com/lambda/home"
                f"?region={arn.region}#/functions/{arn.resource_name}"
            )
            flow = DataFlow(
                platform=LAMBDA_PLATFORM,
                name=self.flow_id(arn),
                display_name=arn.resource_name,
                platform_instance=instance,
                env=self.config.env,
                subtype=DataFlowSubTypes.LAMBDA_FUNCTION.value,
                external_url=console,
                custom_properties={"arn": key},
            )
            job = DataJob(
                name=LAMBDA_JOB_ID,
                flow=flow,
                display_name=arn.resource_name,
                subtype=DataJobSubTypes.LAMBDA_INVOCATION.value,
                external_url=console,
                # The SDK has no setter for upstream jobs, so the whole aspect goes in here.
                extra_aspects=[
                    DataJobInputOutputClass(
                        inputDatasets=sorted(edges.inputs),
                        outputDatasets=sorted(edges.outputs),
                        inputDatajobs=sorted(edges.input_jobs),
                    )
                ],
            )
            self.report.lambda_functions_emitted += 1
            yield from flow.as_workunits()
            yield from job.as_workunits()
