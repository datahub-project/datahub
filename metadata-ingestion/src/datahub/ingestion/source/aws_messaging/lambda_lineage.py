from typing import Dict, Iterable, Optional, Union

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws_messaging.arn import UrnBuilder, parse_arn
from datahub.ingestion.source.aws_messaging.clients import LambdaClient
from datahub.ingestion.source.aws_messaging.config import AwsMessagingConfigBase
from datahub.ingestion.source.aws_messaging.constants import (
    ACCESS_DENIED_CODES,
    LAMBDA_JOB_ID,
    LAMBDA_PLATFORM,
    AwsService,
    JobEdgeKind,
)
from datahub.ingestion.source.aws_messaging.jobs import (
    JobEdgeTracker,
    job_urn,
    shared_job_workunits,
)
from datahub.ingestion.source.aws_messaging.mapper import AwsRegionKey
from datahub.ingestion.source.aws_messaging.models import LambdaFunctionEdges
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.sdk import DataFlow, DataJob


class LambdaLineage:
    """Lambda functions that read from or write to this source's queues or topics.

    Each function becomes a DataFlow with one `invoke` DataJob, the shape the
    kinesis source uses for Firehose. Only functions in the recipe's own account
    and region are emitted."""

    def __init__(
        self,
        *,
        config: AwsMessagingConfigBase,
        report: AwsMessagingReportBase,
        client: LambdaClient,
        urns: UrnBuilder,
        resolver: ResourceResolver,
        region_key: AwsRegionKey,
        jobs: JobEdgeTracker,
    ):
        self.config = config
        self.report = report
        self.client = client
        self.urns = urns
        self.resolver = resolver
        self.region_key = region_key
        self.jobs = jobs
        self.functions: Dict[str, LambdaFunctionEdges] = {}
        self._function_arns: Optional[Dict[str, str]] = None

    def function_arns_by_name(self) -> Dict[str, str]:
        if self._function_arns is None:
            self._function_arns = {
                function["FunctionName"]: function["FunctionArn"]
                for function in self.client.list_functions(
                    on_error=self._outputs_unknown
                )
            }
        return self._function_arns

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
        unqualified = arn.model_copy(
            update={"resource": ":".join(arn.resource.split(":")[:2])}
        )
        if not self.resolver.is_local(unqualified):
            self.report.report_edge_skipped(
                context, function_arn, "Lambda function is in another account or region"
            )
            return None
        key = str(unqualified)
        if key not in self.functions:
            self.functions[key] = LambdaFunctionEdges(arn=unqualified)
        return self.functions[key]

    def job_urn(self, edges: LambdaFunctionEdges) -> str:
        return job_urn(
            platform=LAMBDA_PLATFORM,
            flow_id=self.urns.resource_id(edges.arn),
            job_id=LAMBDA_JOB_ID,
            env=self.config.env,
            instance=self.urns.instance_for(edges.arn.account),
        )

    def _edges_unknown(self, _error: Union[ClientError, BotoCoreError]) -> None:
        self.jobs.mark_unknown(
            LAMBDA_PLATFORM, JobEdgeKind.CONSUMES, JobEdgeKind.PRODUCES
        )

    def _outputs_unknown(self, _error: Union[ClientError, BotoCoreError]) -> None:
        self.jobs.mark_unknown(LAMBDA_PLATFORM, JobEdgeKind.PRODUCES)

    def collect(self) -> None:
        for mapping in self.client.list_event_source_mappings(
            on_error=self._edges_unknown
        ):
            source_urn = self.resolver.urn(parse_arn(mapping.get("EventSourceArn")))
            failure = (
                (mapping.get("DestinationConfig") or {}).get("OnFailure") or {}
            ).get("Destination")
            failure_urn = self.resolver.urn(parse_arn(failure))
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
            self._collect_async_destinations()
        else:
            # Outputs also come from async destinations, which weren't read.
            self.jobs.mark_unknown(LAMBDA_PLATFORM, JobEdgeKind.PRODUCES)

    def _collect_async_destinations(self) -> None:
        denied = False

        def failed(error: Union[ClientError, BotoCoreError]) -> None:
            nonlocal denied
            self._outputs_unknown(error)
            denied = aws_error_code(error) in ACCESS_DENIED_CODES

        for name, function_arn in self.function_arns_by_name().items():
            for destination in self.client.list_async_destinations(
                name, on_error=failed
            ):
                urn = self.resolver.urn(parse_arn(destination))
                if urn is None:
                    continue
                edges = self.function(function_arn, "async invoke destination")
                if edges is not None:
                    edges.outputs.add(urn)
            if denied:
                # The permission applies to every function, so asking for the
                # rest would only repeat the same denial once per function.
                self.report.warning(
                    title="Async destinations not read",
                    message=(
                        "lambda:ListFunctionEventInvokeConfigs was denied, so no "
                        "further functions were checked. Grant it or turn off "
                        "lineage.lambda_async_destinations."
                    ),
                )
                return

    def workunits(self) -> Iterable[MetadataWorkUnit]:
        for key in sorted(self.functions):
            edges = self.functions[key]
            arn = edges.arn
            console = (
                f"https://{arn.region}.console.aws.amazon.com/lambda/home"
                f"?region={arn.region}#/functions/{arn.resource_name}"
            )
            flow = DataFlow(
                platform=LAMBDA_PLATFORM,
                name=self.urns.resource_id(arn),
                display_name=arn.resource_name,
                platform_instance=self.urns.instance_for(arn.account),
                env=self.config.env,
                subtype=DataFlowSubTypes.LAMBDA_FUNCTION.value,
                external_url=console,
                custom_properties={"arn": key},
                parent_container=self.region_key,
            )
            job = DataJob(
                name=LAMBDA_JOB_ID,
                flow=flow,
                display_name=arn.resource_name,
                subtype=DataJobSubTypes.LAMBDA_INVOCATION.value,
                external_url=console,
            )
            self.report.lambda_functions_emitted += 1
            self.jobs.record(str(job.urn), inputs=edges.inputs, outputs=edges.outputs)
            yield from shared_job_workunits(
                flow=flow,
                job=job,
                inputs=edges.inputs,
                outputs=edges.outputs,
                input_jobs=edges.input_jobs,
            )
