import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_config import (
    DatabaseId,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    ShareOrigin,
    SnowflakeDatabase,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCommonMixin
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import DataPlatformInstancePropertiesClass

logger: logging.Logger = logging.getLogger(__name__)

# Cap on the number of share-grant rows we consume from QUERY_HISTORY. The
# SQL fetches one extra row (limit+1) so we can detect truncation deterministically:
# exactly LIMIT rows means "complete", more than LIMIT means "truncated".
SHARE_GRANT_HISTORY_QUERY_LIMIT = 1000

# Skip query texts above this size — usually large stored procedure bodies
# that don't contain GRANT statements. Prevents pathological CPU on huge rows.
_MAX_QUERY_TEXT_BYTES = 1_000_000

# Matches `GRANT USAGE ON DATABASE <db> TO SHARE <share>` allowing whitespace
# variation and quoted identifiers.
_IDENT = r'(?:"[^"]+"|[\w$]+)'
_SHARE_GRANT_RE = re.compile(
    rf"GRANT\s+USAGE\s+ON\s+DATABASE\s+({_IDENT})\s+TO\s+SHARE\s+({_IDENT})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ResolvedProducer:
    platform_instance: str
    database: str


@dataclass(frozen=True)
class ShareGrantDiscovery:
    """Result of mining QUERY_HISTORY for `GRANT USAGE ON DATABASE ... TO SHARE ...` DDL.

    Distinguishes "no grants found" (empty `mapping`, zero skipped) from
    "we skipped some queries that might have contained grants" (non-zero
    `skipped_oversized_queries`). Callers can use `skipped_oversized_queries`
    to report observability about partial coverage.
    """

    mapping: Dict[str, str] = field(default_factory=dict)
    skipped_oversized_queries: int = 0


def _normalize_identifier(identifier: str) -> str:
    """Match Snowflake's storage rules: unquoted -> uppercase, quoted -> preserved."""
    if len(identifier) >= 2 and identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    return identifier.upper()


def parse_share_grants(query_texts: Iterable[str]) -> ShareGrantDiscovery:
    """Parse `GRANT USAGE ON DATABASE ... TO SHARE ...` DDL.

    Returns a `ShareGrantDiscovery` with `mapping` (share_name -> database_name)
    and `skipped_oversized_queries` (count of queries skipped for being above
    `_MAX_QUERY_TEXT_BYTES`). Identifiers are normalized to match Snowflake's
    storage (unquoted -> uppercase, quoted -> preserved). The first occurrence
    of a given share name wins; callers should pass query texts in
    reverse-chronological order so that the most recent grant determines the
    mapping.
    """
    mapping: Dict[str, str] = {}
    skipped = 0
    for query_text in query_texts:
        if not query_text:
            continue
        if len(query_text) > _MAX_QUERY_TEXT_BYTES:
            skipped += 1
            continue
        for match in _SHARE_GRANT_RE.finditer(query_text):
            db = _normalize_identifier(match.group(1))
            share = _normalize_identifier(match.group(2))
            mapping.setdefault(share, db)
    return ShareGrantDiscovery(mapping=mapping, skipped_oversized_queries=skipped)


class SnowflakeSharesHandler(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        graph: Optional[DataHubGraph] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.graph = graph

    def get_shares_workunits(
        self,
        databases: List[SnowflakeDatabase],
    ) -> Iterable[MetadataWorkUnit]:
        # Both dicts are upper-cased by config; Snowflake also returns
        # identifiers in upper-case, so direct lookup is correct.
        inbounds = self.config.inbounds()
        outbounds = self.config.outbounds()
        if not (inbounds or outbounds):
            return

        logger.debug("Checking databases for inbound or outbound shares.")
        for db in databases:
            db_upper = db.name.upper()
            outbound_consumers = outbounds.get(db_upper)
            inbound_source = inbounds.get(db_upper)

            if not (outbound_consumers or inbound_source):
                logger.debug(f"database {db.name} is not shared.")
                continue

            sibling_dbs = (
                list(outbound_consumers) if outbound_consumers else [inbound_source]  # type: ignore[list-item]
            )

            for schema in db.schemas:
                for table_name in schema.tables + schema.views:
                    yield from self.gen_siblings(
                        db.name,
                        schema.name,
                        table_name,
                        primary=bool(outbound_consumers),
                        sibling_databases=sibling_dbs,
                    )

                    if inbound_source is not None:
                        yield self.get_upstream_lineage_with_primary_sibling(
                            db.name, schema.name, table_name, inbound_source
                        )

        self.report_missing_databases(
            databases, list(inbounds.keys()), list(outbounds.keys())
        )

    def discover_share_database_mapping(
        self, connection: SnowflakeConnection
    ) -> Dict[str, str]:
        """Mine QUERY_HISTORY for share -> database grants on the producer side.

        Returns share_name -> database_name. Empty dict on any failure — callers
        must treat absence as "unknown", not as "no grants exist".
        """
        try:
            rows = connection.query(
                SnowflakeQuery.share_grant_history(
                    limit=SHARE_GRANT_HISTORY_QUERY_LIMIT + 1
                )
            )
        except Exception as e:
            self.report.warning(
                title="Share-to-database mining skipped",
                message=(
                    "Could not query SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY for "
                    "share grants. Cross-account consumers ingesting shares "
                    "from this account will need explicit "
                    "`share_database_mapping` config in their consumer recipe "
                    "to resolve producer database names."
                ),
                exc=e,
            )
            return {}

        # We over-fetch by 1 — receiving more than LIMIT rows means there are
        # older grants we couldn't see, so the mapping is incomplete.
        rows_list = list(rows)
        if len(rows_list) > SHARE_GRANT_HISTORY_QUERY_LIMIT:
            self.report.warning(
                title="Share grant history may be incomplete",
                message=(
                    "ACCOUNT_USAGE.QUERY_HISTORY returned more rows than the "
                    "configured cap. Older grants may be missing from the "
                    "published `share_database_mapping`. Consumers can fall back "
                    "to manual `share_database_mapping` config for affected shares."
                ),
                context=f"cap={SHARE_GRANT_HISTORY_QUERY_LIMIT}",
            )
            rows_list = rows_list[:SHARE_GRANT_HISTORY_QUERY_LIMIT]

        query_texts = [row["QUERY_TEXT"] for row in rows_list if row.get("QUERY_TEXT")]
        result = parse_share_grants(query_texts)
        if result.skipped_oversized_queries:
            # Stored procedure bodies and similar mega-queries are skipped to
            # bound CPU. Surface the count so users can tell whether a missing
            # mapping is "no grant exists" vs "grant text was too large to scan".
            self.report.num_share_grant_queries_skipped_oversized = (
                result.skipped_oversized_queries
            )
        logger.info(
            "Discovered share->database mapping for %d shares from QUERY_HISTORY.",
            len(result.mapping),
        )
        return result.mapping

    def get_auto_share_workunits(
        self,
        databases: List[SnowflakeDatabase],
    ) -> Iterable[MetadataWorkUnit]:
        """Auto-discover inbound shares from each database's `origin` field."""
        if not self.config.auto_discover_inbound_shares:
            return

        # Keys from inbounds()/outbounds() are already upper-cased.
        manual_dbs_upper = set(self.config.inbounds()) | set(self.config.outbounds())

        for db in databases:
            origin = db.share_origin
            if origin is None:
                continue
            if db.name.upper() in manual_dbs_upper:
                logger.debug(
                    f"Skipping auto-share for {db.name} — covered by manual `shares` config."
                )
                continue

            resolved = self._resolve_producer(db, origin)
            if resolved is None:
                continue

            for schema in db.schemas:
                for table_name in schema.tables + schema.views:
                    yield from self._emit_auto_share_workunits(
                        consumer_db=db.name,
                        schema_name=schema.name,
                        table_name=table_name,
                        producer_platform_instance=resolved.platform_instance,
                        producer_database=resolved.database,
                    )

            self.report.num_auto_shares_discovered += 1

    def _resolve_producer(
        self, db: SnowflakeDatabase, origin: ShareOrigin
    ) -> Optional[ResolvedProducer]:
        """Resolve the producer platform_instance and database for a shared DB.

        Emits a structured warning and returns None when either side is unresolved.
        Warnings use static messages with dynamic values in `context` so the report
        can deduplicate by `title-message`.
        """
        producer_platform_instance = self.config.resolve_account_to_platform_instance(
            origin.account_identifier, account_locator=origin.account_name
        )
        if producer_platform_instance is None:
            self.report.num_auto_shares_skipped_unresolved_producer += 1
            self.report.warning(
                title="Auto-share lineage skipped — producer unresolved",
                message=(
                    "Could not resolve a Snowflake account to a DataHub "
                    "platform_instance. Add it to `account_mapping` or enable "
                    "`account_locator_fallback`."
                ),
                context=(
                    f"db={db.name}, account={origin.account_identifier}, "
                    f"share={origin.share_name}"
                ),
            )
            return None

        producer_database = self._resolve_share_database(
            origin.share_name, producer_platform_instance
        )
        if producer_database is None:
            self.report.num_auto_shares_skipped_unknown_share_db += 1
            self.report.warning(
                title="Auto-share lineage skipped — share-to-database mapping unknown",
                message=(
                    "Could not resolve a Snowflake share to a producer database "
                    "name. Either ingest the producer first (so its "
                    "`share_database_mapping` is published to the graph) or set "
                    "`share_database_mapping` in the consumer recipe."
                ),
                context=(
                    f"db={db.name}, share={origin.share_name}, "
                    f"account={origin.account_identifier}"
                ),
            )
            return None

        return ResolvedProducer(
            platform_instance=producer_platform_instance,
            database=producer_database,
        )

    def _resolve_share_database(
        self, share_name: str, producer_platform_instance: str
    ) -> Optional[str]:
        # Both the graph mapping (parsed via `parse_share_grants`) and the local
        # config (normalized by `_uppercase_keys` validator) use upper-case keys.
        share_upper = share_name.upper()
        graph_mapping = self._fetch_published_share_mapping(producer_platform_instance)
        return graph_mapping.get(share_upper) or self.config.share_database_mapping.get(
            share_upper
        )

    def _fetch_published_share_mapping(
        self, producer_platform_instance: str
    ) -> Dict[str, str]:
        if self.graph is None:
            return {}

        producer_dpi_urn = make_dataplatform_instance_urn(
            self.identifiers.platform, producer_platform_instance
        )
        try:
            props = self.graph.get_aspect(
                producer_dpi_urn, DataPlatformInstancePropertiesClass
            )
        except Exception as e:
            self.report.warning(
                title="Producer share mapping graph lookup failed",
                message=(
                    "Could not fetch the producer's `DataPlatformInstance` aspect "
                    "from the graph. Falling back to consumer-side "
                    "`share_database_mapping` config — share lineage may be "
                    "incomplete until the graph is reachable again."
                ),
                context=f"producer_urn={producer_dpi_urn}",
                exc=e,
            )
            return {}

        if props is None or not props.customProperties:
            return {}
        mapping_json = props.customProperties.get("share_database_mapping")
        if not mapping_json:
            return {}
        try:
            mapping = json.loads(mapping_json)
        except json.JSONDecodeError as e:
            self.report.warning(
                title="Producer published invalid share_database_mapping",
                message=(
                    "The producer's `share_database_mapping` custom property is "
                    "not valid JSON. Falling back to consumer-side config."
                ),
                context=f"producer_urn={producer_dpi_urn}",
                exc=e,
            )
            return {}
        if not isinstance(mapping, dict):
            self.report.warning(
                title="Producer published invalid share_database_mapping",
                message=(
                    "The producer's `share_database_mapping` is valid JSON but "
                    "not a dict. Falling back to consumer-side config."
                ),
                context=f"producer_urn={producer_dpi_urn}, type={type(mapping).__name__}",
            )
            return {}
        return {str(k): str(v) for k, v in mapping.items()}

    def _emit_auto_share_workunits(
        self,
        consumer_db: str,
        schema_name: str,
        table_name: str,
        producer_platform_instance: str,
        producer_database: str,
    ) -> Iterable[MetadataWorkUnit]:
        producer = DatabaseId(
            database=producer_database, platform_instance=producer_platform_instance
        )

        # Opt-in: skip if the producer URN doesn't exist in the graph yet.
        # Off by default so that consumers ingested before producers still
        # establish lineage that activates when the producer ingests.
        if self.config.validate_producer_urns_in_graph and self.graph is not None:
            producer_urn = make_dataset_urn_with_platform_instance(
                self.identifiers.platform,
                self.identifiers.get_dataset_identifier(
                    table_name, schema_name, producer_database
                ),
                producer_platform_instance,
            )
            try:
                exists = self.graph.exists(producer_urn)
            except Exception as e:
                # Fail open: a transient graph error shouldn't strip lineage
                # for the rest of the ingestion. Surface a warning so the user
                # knows validation was skipped.
                self.report.warning(
                    title="Producer URN existence check failed",
                    message=(
                        "Could not verify the producer URN in the graph. "
                        "Emitting share lineage anyway; the link will resolve "
                        "once the producer is ingested."
                    ),
                    context=f"producer_urn={producer_urn}",
                    exc=e,
                )
            else:
                if not exists:
                    self.report.num_auto_shares_skipped_producer_urn_missing += 1
                    return

        # Reuse the manual-shares emission helpers — same Siblings(primary=False)
        # + UpstreamLineage(COPY) shape the manual `shares` config produces, so
        # both code paths emit identical aspect content for the same producer.
        yield from self.gen_siblings(
            consumer_db,
            schema_name,
            table_name,
            primary=False,
            sibling_databases=[producer],
        )
        yield self.get_upstream_lineage_with_primary_sibling(
            consumer_db, schema_name, table_name, producer
        )

    def report_missing_databases(
        self,
        databases: List[SnowflakeDatabase],
        inbounds: List[str],
        outbounds: List[str],
    ) -> None:
        db_names_upper = {db.name.upper() for db in databases}
        missing_dbs = [
            db for db in inbounds + outbounds if db.upper() not in db_names_upper
        ]

        if missing_dbs and self.config.platform_instance:
            self.report.warning(
                title="Extra Snowflake share configurations",
                message="Some databases referenced by the share configs were not ingested. Siblings/lineage will not be set for these.",
                context=f"{missing_dbs}",
            )
        elif missing_dbs:
            logger.debug(
                f"Databases {missing_dbs} were not ingested in this recipe.",
            )

    def gen_siblings(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        primary: bool,
        sibling_databases: List[DatabaseId],
    ) -> Iterable[MetadataWorkUnit]:
        if not sibling_databases:
            return
        dataset_identifier = self.identifiers.get_dataset_identifier(
            table_name, schema_name, database_name
        )
        urn = self.identifiers.gen_dataset_urn(dataset_identifier)

        sibling_urns = [
            make_dataset_urn_with_platform_instance(
                self.identifiers.platform,
                self.identifiers.get_dataset_identifier(
                    table_name, schema_name, sibling_db.database
                ),
                sibling_db.platform_instance,
            )
            for sibling_db in sibling_databases
        ]

        self.report.num_siblings_emitted += 1
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=Siblings(primary=primary, siblings=sorted(sibling_urns)),
        ).as_workunit()

    def get_upstream_lineage_with_primary_sibling(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        primary_sibling_db: DatabaseId,
    ) -> MetadataWorkUnit:
        dataset_identifier = self.identifiers.get_dataset_identifier(
            table_name, schema_name, database_name
        )
        urn = self.identifiers.gen_dataset_urn(dataset_identifier)

        upstream_urn = make_dataset_urn_with_platform_instance(
            self.identifiers.platform,
            self.identifiers.get_dataset_identifier(
                table_name, schema_name, primary_sibling_db.database
            ),
            primary_sibling_db.platform_instance,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=UpstreamLineage(
                upstreams=[Upstream(dataset=upstream_urn, type=DatasetLineageType.COPY)]
            ),
        ).as_workunit()
