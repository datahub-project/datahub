import logging
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

# DATASHARE-DIAG: temporary diagnostic prefix for Morningstar datashare lineage
# investigation. This branch only adds read-only logging; revert before merge.
_DIAG = "[DATASHARE-DIAG]"
_MAX_LOGGED_EDGES = 25  # per (schema, share) — to avoid log spam on large shares


class OutboundSharePlatformResource(BaseModel):
    namespace: str
    platform_instance: Optional[str] = None
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
        logger.info(
            f"{_DIAG} to_platform_resource: outbound_shares_count={len(shares)} "
            f"platform_instance={self.config.platform_instance!r} env={self.config.env!r}"
        )
        if not shares:
            self.report.outbound_shares_count = 0
            return

        self.report.outbound_shares_count = len(shares)
        # Producer namespace will be current namespace for all
        # outbound data shares

        for share in shares:
            producer_namespace = share.producer_namespace
            logger.info(
                f"{_DIAG} outbound_share: share_name={share.share_name!r} "
                f"producer_namespace={producer_namespace!r} "
                f"source_database={share.source_database!r} "
                f"platform_resource_key={share.get_key()!r}"
            )
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
        logger.info(
            f"{_DIAG} generate_lineage: inbound_share={share.get_description()} "
            f"consumer_database={getattr(share, 'consumer_database', None)!r} "
            f"local_schemas={list(tables.keys())} "
            f"local_table_count={sum(len(v) for v in tables.values())}"
        )
        upstream_share = self.find_upstream_share(share)

        if not upstream_share:
            logger.warning(
                f"{_DIAG} generate_lineage: NO upstream share resolved → "
                f"bridge lineage will be EMPTY for inbound share "
                f"{share.get_description()}"
            )
            return

        logger.info(
            f"{_DIAG} resolved upstream share: "
            f"namespace={upstream_share.namespace!r} "
            f"share_name={upstream_share.share_name!r} "
            f"source_database={upstream_share.source_database!r} "
            f"platform_instance={upstream_share.platform_instance!r} "
            f"env={upstream_share.env!r}"
        )

        edges_emitted = 0
        edges_logged = 0
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

                if edges_logged < _MAX_LOGGED_EDGES:
                    logger.info(
                        f"{_DIAG} bridge_edge[{edges_logged}]: "
                        f"upstream={upstream_dataset_urn} "
                        f"downstream={dataset_urn}"
                    )
                    edges_logged += 1

                edges_emitted += 1
                yield KnownLineageMapping(
                    upstream_urn=upstream_dataset_urn, downstream_urn=dataset_urn
                )

        logger.info(
            f"{_DIAG} generate_lineage DONE: "
            f"share={share.get_description()} edges_emitted={edges_emitted} "
            f"(first {min(edges_logged, _MAX_LOGGED_EDGES)} logged above)"
        )

    def find_upstream_share(
        self, share: Union[InboundDatashare, PartialInboundDatashare]
    ) -> Optional[OutboundSharePlatformResource]:
        logger.info(
            f"{_DIAG} find_upstream_share: looking up share={share.get_description()} "
            f"share_type={type(share).__name__}"
        )
        if not self.graph:
            logger.warning(
                f"{_DIAG} find_upstream_share: graph is None — cannot search "
                f"PlatformResources. Configure datahub-rest sink or top-level datahub_api."
            )
            self.report.warning(
                title="Upstream lineage of inbound datashare will be missing",
                message="Missing datahub graph. Either use the datahub-rest sink or "
                "set the top-level datahub_api config in the recipe",
            )
        else:
            resources = self.get_platform_resources(self.graph, share)
            logger.info(
                f"{_DIAG} find_upstream_share: PlatformResource search returned "
                f"{len(resources)} result(s)"
            )
            for idx, r in enumerate(resources):
                ri = r.resource_info
                logger.info(
                    f"{_DIAG}   resource[{idx}]: "
                    f"primary_key={getattr(ri, 'primary_key', None)!r} "
                    f"resource_type={getattr(ri, 'resource_type', None)!r} "
                    f"secondary_keys={getattr(ri, 'secondary_keys', None)!r} "
                    f"value={getattr(ri, 'value', None)!r}"
                )

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
                logger.warning(
                    f"{_DIAG} find_upstream_share: no matching {PLATFORM_RESOURCE_TYPE} "
                    f"PlatformResource for {share.get_description()} — bridge lineage "
                    f"WILL NOT be generated."
                )
                self.report.info(
                    title="Upstream lineage of inbound datashare will be missing",
                    message="Missing platform resource for share. "
                    "Setup redshift ingestion for namespace if not already done. If ingestion is setup, "
                    "check whether ingestion user has ALTER/SHARE permission to share.",
                    context=share.get_description(),
                )
            else:
                # Ideally we should get only one resource as primary key is namespace+share
                # and type is "OUTBOUND_DATASHARE". However in practice multiple
                # PlatformResources can exist for the same (namespace, share) when the
                # producer ingestion has been re-run with a different platform_instance
                # or env (e.g. an early run had no platform_instance and was later
                # corrected). The PlatformResource URN includes platform_instance, so
                # those become distinct entities and both come back from the search.
                #
                # Strategy:
                #   1. Parse all resources of the right type.
                #   2. Among them, prefer ones that have a non-empty platform_instance
                #      set on the OutboundSharePlatformResource value. A producer
                #      recipe without platform_instance is the legacy/stale case and
                #      generates upstream URNs that do not actually exist in DataHub
                #      once the producer has been re-ingested with platform_instance.
                #   3. If multiple candidates remain, pick the first and warn so the
                #      operator can clean up the stale resource.
                parsed: List[OutboundSharePlatformResource] = []
                for resource in resources:
                    try:
                        assert (
                            resource.resource_info is not None
                            and resource.resource_info.value is not None
                        )
                        if (
                            resource.resource_info.resource_type
                            != PLATFORM_RESOURCE_TYPE
                        ):
                            continue
                        parsed.append(
                            resource.resource_info.value.as_pydantic_object(
                                OutboundSharePlatformResource, True
                            )
                        )
                    except Exception as e:
                        self.report.warning(
                            title="Upstream lineage of inbound datashare will be missing",
                            message="Failed to parse platform resource for outbound datashare",
                            context=share.get_description(),
                            exc=e,
                        )

                if not parsed:
                    return None

                with_pi = [p for p in parsed if p.platform_instance]
                without_pi = [p for p in parsed if not p.platform_instance]

                if with_pi and without_pi:
                    logger.warning(
                        f"{_DIAG} find_upstream_share: found "
                        f"{len(with_pi)} resource(s) with platform_instance and "
                        f"{len(without_pi)} without — preferring resource(s) with "
                        f"platform_instance set. Stale resource(s) without "
                        f"platform_instance should be cleaned up: "
                        f"{[(p.namespace, p.share_name, p.env) for p in without_pi]}"
                    )

                preferred = with_pi or without_pi

                if len(preferred) > 1:
                    chosen = preferred[0]
                    logger.warning(
                        f"{_DIAG} find_upstream_share: {len(preferred)} candidate "
                        f"resources remain after preference filter; picking first. "
                        f"Chosen: namespace={chosen.namespace!r} "
                        f"share_name={chosen.share_name!r} "
                        f"platform_instance={chosen.platform_instance!r} "
                        f"env={chosen.env!r}. "
                        f"All candidates: "
                        f"{[(p.namespace, p.share_name, p.platform_instance, p.env) for p in preferred]}"
                    )
                    self.report.warning(
                        title="Multiple matching outbound datashare PlatformResources",
                        message=(
                            "Found more than one OUTBOUND_DATASHARE PlatformResource "
                            "matching this inbound share even after preferring those "
                            "with a platform_instance. Picking the first; consider "
                            "cleaning up duplicates."
                        ),
                        context=share.get_description(),
                    )

                return preferred[0]

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
            logger.info(
                f"{_DIAG} get_platform_resources: PARTIAL search "
                f"share_name={share.share_name!r} "
                f"producer_namespace_prefix={share.producer_namespace_prefix!r}*"
            )
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
        logger.info(
            f"{_DIAG} get_platform_resources: EXACT search by primary_key={share.get_key()!r}"
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
