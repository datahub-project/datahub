import logging
import re
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    DatabaseId,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeDatabase
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCommonMixin
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
    UpstreamLineage,
)

logger: logging.Logger = logging.getLogger(__name__)

# Matches `GRANT USAGE ON DATABASE <db> TO SHARE <share>` allowing whitespace
# variation and quoted identifiers. Used by Phase E.2 producer-side mining.
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
        if not query_text:
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
    ) -> None:
        self.config = config
        self.report = report

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
        """Phase E.2: mine producer-side QUERY_HISTORY for share -> database grants.

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

        query_texts = [row["QUERY_TEXT"] for row in rows if row.get("QUERY_TEXT")]
        mapping = parse_share_grants(query_texts)
        if mapping:
            logger.info(
                "Discovered share->database mapping for %d shares from QUERY_HISTORY.",
                len(mapping),
            )
        return mapping

    def get_auto_share_workunits(
        self,
        databases: List[SnowflakeDatabase],
    ) -> Iterable[MetadataWorkUnit]:
        """Phase E.1: auto-discover inbound shares from each database's `origin` field."""
        if not self.config.auto_discover_inbound_shares:
            return

        manual_dbs_upper = {db.upper() for db in self.config.inbounds()} | {
            db.upper() for db in self.config.outbounds()
        }

        for db in databases:
            if not db.is_shared_database():
                continue
            if db.name.upper() in manual_dbs_upper:
                logger.debug(
                    f"Skipping auto-share for {db.name} — covered by manual `shares` config."
                )
                continue

            share_origin = db.get_share_origin()
            if share_origin is None:
                continue

            org, account, share_name = share_origin
            account_identifier = f"{org}.{account}" if org else account

            producer_platform_instance = (
                self.config.resolve_account_to_platform_instance(
                    account_identifier, account_locator=account
                )
            )
            if producer_platform_instance is None:
                self.report.num_auto_shares_skipped_unresolved_producer += 1
                self.report.warning(
                    title="Auto-share lineage skipped — producer unresolved",
                    message=(
                        f"Could not resolve account {account_identifier!r} to a "
                        "platform_instance. Add it to `account_mapping` or enable "
                        "`account_locator_fallback`."
                    ),
                    context=f"db={db.name}, share={share_name}",
                )
                continue

            producer_database = self._resolve_share_database(share_name)
            if producer_database is None:
                self.report.num_auto_shares_skipped_unknown_share_db += 1
                self.report.warning(
                    title="Auto-share lineage skipped — share-to-database mapping unknown",
                    message=(
                        f"Could not resolve share {share_name!r} to producer "
                        "database name. Add it to `share_database_mapping` (consumer "
                        "recipe) or wait for Phase E.2 (QUERY_HISTORY mining on "
                        "producer side)."
                    ),
                    context=f"db={db.name}, producer_account={account_identifier}",
                )
                continue

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

    def _resolve_share_database(self, share_name: str) -> Optional[str]:
        for key, value in self.config.share_database_mapping.items():
            if key.upper() == share_name.upper():
                return value
        return None

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
