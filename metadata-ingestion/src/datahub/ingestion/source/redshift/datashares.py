import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Union

from pydantic import BaseModel

from datahub.api.entities.platformresource.platform_resource import (
    ElasticPlatformResourceQuery,
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    InboundDatashare,
    OutboundDatashare,
    PartialInboundDatashare,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.sql_parsing.sql_parsing_aggregator import KnownLineageMapping
from datahub.utilities.search_utils import LogicalOperator

logger: logging.Logger = logging.getLogger(__name__)


class OutboundSharePlatformResource(BaseModel):
    namespace: str
    platform_instance: Optional[str] = None
    env: str
    source_database: str
    share_name: str
    # Optional for backward-compat with resources written before this field was added.
    ingested_at: Optional[datetime] = None

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
                    ingested_at=datetime.now(timezone.utc),
                )

                platform_resource = PlatformResource.create(
                    key=platform_resource_key,
                    value=value,
                    secondary_keys=[share.share_name, share.producer_namespace],
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
        self,
        share: Union[InboundDatashare, PartialInboundDatashare],
        tables: Dict[str, List[Union[RedshiftTable, RedshiftView]]],
    ) -> Iterable[KnownLineageMapping]:
        upstream_share = self.find_upstream_share(share)

        if not upstream_share:
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

    def find_upstream_share(
        self, share: Union[InboundDatashare, PartialInboundDatashare]
    ) -> Optional[OutboundSharePlatformResource]:
        if not self.graph:
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message="Missing datahub graph. Either use the datahub-rest sink or "
                "set the top-level datahub_api config in the recipe",
            )
        else:
            resources = self.get_platform_resources(self.graph, share)

            if len(resources) == 0 or (
                not any(
                    [
                        resource.resource_info is not None
                        and resource.resource_info.resource_type
                        == PLATFORM_RESOURCE_TYPE
                        for resource in resources
                    ]
                )
            ):
                self.report.info(
                    title="Upstream lineage of inbound datashare will be missing",
                    message="Missing platform resource for share. "
                    "Setup redshift ingestion for namespace if not already done. If ingestion is setup, "
                    "check whether ingestion user has ALTER/SHARE permission to share.",
                    context=share.get_description(),
                )
            else:
                # Ideally we should get only one resource as primary key is namespace+share
                # and type is "OUTBOUND_DATASHARE"; if duplicates exist they are resolved below.
                parsed: List[OutboundSharePlatformResource] = []
                for resource in resources:
                    if (
                        resource.resource_info is None
                        or resource.resource_info.value is None
                    ):
                        continue
                    if resource.resource_info.resource_type != PLATFORM_RESOURCE_TYPE:
                        continue
                    try:
                        parsed.append(
                            resource.resource_info.value.as_pydantic_object(
                                OutboundSharePlatformResource, True
                            )
                        )
                    except Exception as e:
                        self.report.warning(
                            title="Upstream lineage of inbound datashare will be missing",
                            message="Failed to parse platform resource for outbound datashare",
                            context=f"{share.get_description()} resource={resource.id!r}",
                            exc=e,
                        )

                if not parsed:
                    self.report.warning(
                        title="Upstream lineage of inbound datashare will be missing",
                        message="Could not parse any matching OUTBOUND_DATASHARE PlatformResource for this share.",
                        context=share.get_description(),
                    )
                    return None

                _epoch = datetime.min.replace(tzinfo=timezone.utc)

                def _aware(ts: Optional[datetime]) -> datetime:
                    # Treat naive datetimes as UTC to avoid TypeError in sort.
                    if ts is None:
                        return _epoch
                    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)

                by_recency = sorted(
                    parsed, key=lambda p: _aware(p.ingested_at), reverse=True
                )

                if by_recency[0].ingested_at is not None:
                    candidates = by_recency
                else:
                    # No timestamps: prefer resources with platform_instance, as those come from newer ingestion runs.
                    with_pi = [p for p in by_recency if p.platform_instance]
                    without_pi = [p for p in by_recency if not p.platform_instance]
                    if with_pi and without_pi:
                        logger.warning(
                            f"find_upstream_share: {len(with_pi)} resource(s) have "
                            f"platform_instance and {len(without_pi)} do not (none "
                            f"have ingested_at — legacy resources). Preferring "
                            f"resource(s) with platform_instance set. Stale "
                            f"resource(s) without platform_instance should be "
                            f"cleaned up: "
                            f"{[(p.namespace, p.share_name, p.env) for p in without_pi]}"
                        )
                    candidates = with_pi or without_pi

                if len(candidates) > 1:
                    chosen = candidates[0]
                    logger.warning(
                        f"find_upstream_share: {len(candidates)} candidate "
                        f"resources remain; picking the most recently ingested. "
                        f"Chosen: namespace={chosen.namespace!r} "
                        f"share_name={chosen.share_name!r} "
                        f"platform_instance={chosen.platform_instance!r} "
                        f"env={chosen.env!r} "
                        f"ingested_at={chosen.ingested_at!r}. "
                        f"Consider cleaning up stale duplicates."
                    )
                    self.report.warning(
                        title="Multiple matching outbound datashare PlatformResources",
                        message=(
                            "Found more than one OUTBOUND_DATASHARE PlatformResource "
                            "matching this inbound share. Picking the most recently "
                            "ingested; consider cleaning up duplicates."
                        ),
                        context=share.get_description(),
                    )

                return candidates[0]

        return None

    def get_platform_resources(
        self,
        graph: DataHubGraph,
        share: Union[InboundDatashare, PartialInboundDatashare],
    ) -> List[PlatformResource]:
        # NOTE: ideally we receive InboundDatashare and not PartialInboundDatashare.
        # however due to varchar(128) type of database table that captures datashare options
        # we may receive only partial information about inbound share
        # Alternate option to get InboundDatashare using svv_datashares requires superuser
        if isinstance(share, PartialInboundDatashare):
            return list(
                PlatformResource.search_by_filters(
                    graph,
                    ElasticPlatformResourceQuery.create_from()
                    .group(LogicalOperator.AND)
                    .add_field_match(
                        PlatformResourceSearchFields.RESOURCE_TYPE,
                        PLATFORM_RESOURCE_TYPE,
                    )
                    .add_field_match(
                        PlatformResourceSearchFields.PLATFORM, self.platform
                    )
                    .add_field_match(
                        PlatformResourceSearchFields.SECONDARY_KEYS,
                        share.share_name,
                    )
                    .add_wildcard(
                        PlatformResourceSearchFields.SECONDARY_KEYS.field_name,
                        f"{share.producer_namespace_prefix}*",
                    )
                    .end(),
                )
            )
        return list(
            PlatformResource.search_by_key(
                graph, key=share.get_key(), primary=True, is_exact=True
            )
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
