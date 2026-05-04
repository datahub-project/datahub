import json
import logging
import re
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

# Must match the LIMIT in SnowflakeQuery.share_grant_history.
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


def _normalize_identifier(identifier: str) -> str:
    """Match Snowflake's storage rules: unquoted -> uppercase, quoted -> preserved."""
    if len(identifier) >= 2 and identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    return identifier.upper()


def parse_share_grants(query_texts: Iterable[str]) -> Dict[str, str]:
    """Parse `GRANT USAGE ON DATABASE ... TO SHARE ...` DDL.

    Returns share_name -> database_name. Identifiers are normalized to match
    Snowflake's storage (unquoted -> uppercase, quoted -> preserved). The
    first occurrence of a given share name wins; callers should pass query
    texts in reverse-chronological order so that the most recent grant
    determines the mapping.
    """
    mapping: Dict[str, str] = {}
    for query_text in query_texts:
        if not query_text or len(query_text) > _MAX_QUERY_TEXT_BYTES:
            continue
        for match in _SHARE_GRANT_RE.finditer(query_text):
            db = _normalize_identifier(match.group(1))
            share = _normalize_identifier(match.group(2))
            mapping.setdefault(share, db)
    return mapping


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
        inbounds = self.config.inbounds()
        outbounds = self.config.outbounds()
        if not (inbounds or outbounds):
            return

        # Snowflake returns identifiers in uppercase; config values may use any case.
        inbounds_upper: Dict[str, str] = {k.upper(): k for k in inbounds}
        outbounds_upper: Dict[str, str] = {k.upper(): k for k in outbounds}

        logger.debug("Checking databases for inbound or outbound shares.")
        for db in databases:
            db_upper = db.name.upper()
            is_inbound = db_upper in inbounds_upper
            is_outbound = db_upper in outbounds_upper

            if not (is_inbound or is_outbound):
                logger.debug(f"database {db.name} is not shared.")
                continue

            sibling_dbs = (
                list(outbounds[outbounds_upper[db_upper]])
                if is_outbound
                else [inbounds[inbounds_upper[db_upper]]]
            )

            for schema in db.schemas:
                for table_name in schema.tables + schema.views:
                    yield from self.gen_siblings(
                        db.name,
                        schema.name,
                        table_name,
                        is_outbound,
                        sibling_dbs,
                    )

                    if is_inbound:
                        assert len(sibling_dbs) == 1
                        yield self.get_upstream_lineage_with_primary_sibling(
                            db.name, schema.name, table_name, sibling_dbs[0]
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
            rows = connection.query(SnowflakeQuery.share_grant_history())
        except Exception as e:
            self.report.warning(
                title="Share-to-database mining skipped",
                message=(
                    "Could not query SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY for "
                    "share grants. Consumers will need explicit "
                    "`share_database_mapping` config to resolve producer "
                    "database names."
                ),
                exc=e,
            )
            return {}

        # The query has a LIMIT — if we hit it, the mapping may be incomplete.
        rows_list = list(rows)
        if len(rows_list) >= SHARE_GRANT_HISTORY_QUERY_LIMIT:
            self.report.warning(
                title="Share grant history may be incomplete",
                message=(
                    f"`SHOW QUERY_HISTORY` returned the maximum "
                    f"{SHARE_GRANT_HISTORY_QUERY_LIMIT} rows. Older grants may be "
                    "missing from the published `share_database_mapping`. "
                    "Consumers can fall back to manual `share_database_mapping` "
                    "config for affected shares."
                ),
            )

        query_texts = [row["QUERY_TEXT"] for row in rows_list if row.get("QUERY_TEXT")]
        mapping = parse_share_grants(query_texts)
        logger.info(
            "Discovered share->database mapping for %d shares from QUERY_HISTORY.",
            len(mapping),
        )
        return mapping

    def get_auto_share_workunits(
        self,
        databases: List[SnowflakeDatabase],
    ) -> Iterable[MetadataWorkUnit]:
        """Auto-discover inbound shares from each database's `origin` field."""
        if not self.config.auto_discover_inbound_shares:
            return

        manual_dbs_upper = {db.upper() for db in self.config.inbounds()} | {
            db.upper() for db in self.config.outbounds()
        }

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
            producer_platform_instance, producer_database = resolved

            for schema in db.schemas:
                for table_name in schema.tables + schema.views:
                    yield from self._emit_auto_share_workunits(
                        consumer_db=db.name,
                        schema_name=schema.name,
                        table_name=table_name,
                        producer_platform_instance=producer_platform_instance,
                        producer_database=producer_database,
                    )

            self.report.num_auto_shares_discovered += 1

    def _resolve_producer(
        self, db: SnowflakeDatabase, origin: ShareOrigin
    ) -> Optional[tuple]:
        """Resolve (producer_platform_instance, producer_database) for a shared DB.

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

        return producer_platform_instance, producer_database

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
        consumer_identifier = self.identifiers.get_dataset_identifier(
            table_name, schema_name, consumer_db
        )
        consumer_urn = self.identifiers.gen_dataset_urn(consumer_identifier)

        producer_urn = make_dataset_urn_with_platform_instance(
            self.identifiers.platform,
            self.identifiers.get_dataset_identifier(
                table_name, schema_name, producer_database
            ),
            producer_platform_instance,
        )

        # Opt-in: skip if the producer URN doesn't exist in the graph yet.
        # Off by default so that consumers ingested before producers still
        # establish lineage that activates when the producer ingests.
        if (
            self.config.validate_producer_urns_in_graph
            and self.graph is not None
            and not self.graph.exists(producer_urn)
        ):
            self.report.num_auto_shares_skipped_producer_urn_missing += 1
            return

        self.report.num_siblings_emitted += 1
        yield MetadataChangeProposalWrapper(
            entityUrn=consumer_urn,
            aspect=Siblings(primary=False, siblings=[producer_urn]),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=consumer_urn,
            aspect=UpstreamLineage(
                upstreams=[Upstream(dataset=producer_urn, type=DatasetLineageType.COPY)]
            ),
        ).as_workunit()

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
