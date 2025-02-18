from typing import Dict, Iterable, List, Optional

from pydantic import BaseModel

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    InboundDatashare,
    OutboundDatashare,
    RedshiftTable,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping


class OutboundSharePlatformResource(BaseModel):
    namespace: str
    platform_instance: Optional[str]
    env: str
    source_database: str
    share_name: str

    def get_key(self) -> str:
        return f"{self.namespace}.{self.share_name}"


PLATFORM_RESOURCE_TYPE = "OUTBOUND_DATASHARE"


class RedshiftDatasharesHelper:
    """
    Redshift datashares lineage generation relies on PlatformResource entity
    to identify the producer namespace and its platform_instance and env

    Ingestion of any database in namespace will
    A. generate PlatformResource entity for all outbound shares in namespace.
    B. generate lineage with upstream tables from another namespace, if the database
        is created from an inbound share

    """

    def __init__(
        self,
        config: RedshiftConfig,
        report: RedshiftReport,
        graph: Optional[DataHubGraph],
    ):
        self.platform = "redshift"
        self.config = config
        self.report = report
        self.graph = graph

    def to_platform_resource(
        self, shares: List[OutboundDatashare]
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if not shares:
            self.report.outbound_shares_count = 0
            return

        self.report.outbound_shares_count = len(shares)
        # Producer namespace will be current namespace for all
        # outbound data shares

        for share in shares:
            producer_namespace = share.producer_namespace
            try:
                platform_resource_key = PlatformResourceKey(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    resource_type=PLATFORM_RESOURCE_TYPE,
                    primary_key=share.get_key(),
                )

                value = OutboundSharePlatformResource(
                    namespace=producer_namespace,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                    source_database=share.source_database,
                    share_name=share.share_name,
                )

                platform_resource = PlatformResource.create(
                    key=platform_resource_key,
                    value=value,
                )

                yield from platform_resource.to_mcps()

            except Exception as exc:
                self.report.warning(
                    title="Downstream lineage to outbound datashare may not work",
                    message="Failed to generate platform resource for outbound datashares",
                    context=f"Namespace {share.producer_namespace} Share {share.share_name}",
                    exc=exc,
                )

    def generate_lineage(
        self, share: Optional[InboundDatashare], tables: Dict[str, List[RedshiftTable]]
    ) -> Iterable[KnownLineageMapping]:
        if not share:
            self.report.database_created_from_share = False
            return

        self.report.database_created_from_share = True

        if not self.graph:
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message="Missing datahub graph. Either use the datahub-rest sink or "
                "set the top-level datahub_api config in the recipe",
            )
            return

        resources = list(
            PlatformResource.search_by_key(
                self.graph, key=share.get_key(), primary=True, is_exact=True
            )
        )

        if len(resources) == 0 or (
            not any(
                [
                    resource.resource_info is not None
                    and resource.resource_info.resource_type == PLATFORM_RESOURCE_TYPE
                    for resource in resources
                ]
            )
        ):
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message="Missing platform resource for share. Setup redshift ingestion for namespace if not already done"
                "If ingestion is setup, check whether ingestion user has permission to share",
                context=f"Namespace {share.producer_namespace} Share {share.share_name}",
            )
            return

        upstream_share: OutboundSharePlatformResource
        # Ideally we should get only one resource as primary key is namespace+share
        # and type is "OUTBOUND_DATASHARE"
        for resource in resources:
            try:
                assert resource.resource_info is not None
                upstream_share = OutboundSharePlatformResource.parse_obj(
                    resource.resource_info.value
                )
                break
            except Exception as e:
                self.report.warning(
                    title="Upstream lineage of inbound datashare will be missing",
                    message="Failed to parse platform resource for outbound datashare",
                    context=f"Namespace {share.producer_namespace} Share {share.share_name}",
                    exc=e,
                )
                return

        for schema in tables:
            for table in tables[schema]:
                dataset_urn = self.gen_dataset_urn(
                    f"{share.consumer_database}.{schema}.{table.name}",
                    self.config.platform_instance,
                    self.config.env,
                )

                upstream_dataset_urn = self.gen_dataset_urn(
                    f"{upstream_share.source_database}.{schema}.{table.name}",
                    upstream_share.platform_instance,
                    upstream_share.env,
                )

                yield KnownLineageMapping(
                    upstream_urn=upstream_dataset_urn, downstream_urn=dataset_urn
                )

    # TODO: Refactor and move to new RedshiftIdentifierBuilder class
    def gen_dataset_urn(
        self, datahub_dataset_name: str, platform_instance: Optional[str], env: str
    ) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=datahub_dataset_name,
            platform_instance=platform_instance,
            env=env,
        )
