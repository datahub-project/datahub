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


class NamespacePlatformResource(BaseModel):
    namespace: str
    platform_instance: Optional[str]
    env: str
    outbound_share_name_to_source_database: Dict[str, str]


PLATFORM_RESOURCE_TYPE = "OUTBOUND_DATASHARES"


class RedshiftDatasharesHelper:
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
        producer_namespace = shares[0].producer_namespace
        try:
            platform_resource_key = PlatformResourceKey(
                platform=self.platform,
                resource_type=PLATFORM_RESOURCE_TYPE,
                platform_instance=self.config.platform_instance,
                primary_key=producer_namespace,
            )

            value = NamespacePlatformResource(
                namespace=producer_namespace,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                outbound_share_name_to_source_database={
                    share.share_name: share.source_database for share in shares
                },
            )

            platform_resource = PlatformResource.create(
                key=platform_resource_key,
                value=value,
            )

            yield from platform_resource.to_mcps()

        except Exception as exc:
            self.report.warning(
                title="Downstream lineage to outbound datashares may not work",
                message="Failed to generate platform resource for outbound datashares",
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
                self.graph, key=share.producer_namespace, primary=True, is_exact=True
            )
        )

        if len(resources) == 0:
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message="Missing platform resource. Setup ingestion for "
                f"{share.producer_namespace} namespae in redshift",
            )
            return

        upstream_namespace: Optional[NamespacePlatformResource] = None
        upstream_database: Optional[str] = None
        # We may receive multiple resources, since ingestion is run per database
        # and namespace may have multiple databases.
        for resource in resources:
            if (
                resource.resource_info
                and resource.resource_info.resource_type == PLATFORM_RESOURCE_TYPE
            ):
                upstream_namespace = NamespacePlatformResource.parse_obj(
                    resource.resource_info.value
                )
                if (
                    upstream_namespace.namespace == share.producer_namespace
                    and share.share_name
                    in upstream_namespace.outbound_share_name_to_source_database
                ):
                    upstream_database = (
                        upstream_namespace.outbound_share_name_to_source_database[
                            share.share_name
                        ]
                    )
                    break

        if upstream_namespace is None:
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message=f"Platform resource not found for {share.producer_namespace} namespace",
            )
            return
        elif upstream_database is None:
            # TODO: document permissions required for datashare lineage
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message=f"Upstream datashare {share.share_name} not found in "
                f"{share.producer_namespace} namespace. "
                " Please check permissions for ingestion with platform"
                f"instance {upstream_namespace.platform_instance}",
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
                    f"{upstream_database}.{schema}.{table.name}",
                    upstream_namespace.platform_instance,
                    upstream_namespace.env,
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
