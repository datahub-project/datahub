import logging
from typing import Dict, FrozenSet, Iterable, List, Optional

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
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeOutboundShare,
    SnowflakeShareObject,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCommonMixin
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
    UpstreamLineage,
)

logger: logging.Logger = logging.getLogger(__name__)


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
        connection: SnowflakeConnection,
    ) -> Iterable[MetadataWorkUnit]:
        inbounds = self.config.inbounds()
        outbounds = self.config.outbounds()
        if not (inbounds or outbounds):
            return

        # Snowflake returns identifiers in uppercase; config values may use any case.
        inbounds_upper: Dict[str, str] = {k.upper(): k for k in inbounds}
        outbounds_upper: Dict[str, str] = {k.upper(): k for k in outbounds}

        granted_objects_by_db: Optional[Dict[str, FrozenSet[SnowflakeShareObject]]] = (
            None
        )
        if self.config.enumerate_share_objects and outbounds:
            granted_objects_by_db = self._discover_share_objects(
                connection, set(outbounds.keys())
            )

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

            granted: Optional[FrozenSet[SnowflakeShareObject]] = None
            if is_outbound and granted_objects_by_db is not None:
                granted = granted_objects_by_db.get(db_upper)

            for schema in db.schemas:
                for table_name in schema.tables + schema.views:
                    if is_outbound and granted is not None:
                        obj = SnowflakeShareObject(
                            database=db.name.lower(),
                            schema=schema.name.lower(),
                            name=table_name.lower(),
                        )
                        if obj not in granted:
                            self.report.num_ghost_siblings_prevented += 1
                            logger.debug(
                                f"{db.name}.{schema.name}.{table_name} is not in any outbound share grant — skipping siblings."
                            )
                            continue

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

    def _discover_share_objects(
        self,
        connection: SnowflakeConnection,
        outbound_databases: set,
    ) -> Dict[str, FrozenSet[SnowflakeShareObject]]:
        """
        Queries SHOW SHARES and DESCRIBE SHARE to find the exact objects granted
        to each outbound share, keyed by uppercase database name.

        Requires ACCOUNTADMIN role or share ownership. On failure, falls back to
        config-only behavior (all objects in shared DBs get siblings).

        A database is only included in the result if ALL its shares were successfully
        described. If any DESCRIBE SHARE fails, that database is excluded so the
        caller falls back to emitting siblings for all its objects.
        """
        outbound_shares = self._fetch_outbound_shares(connection, outbound_databases)
        if not outbound_shares:
            return {}

        # None signals that DESCRIBE SHARE failed for this DB — exclude it from
        # filtering so siblings are emitted for all its objects.
        result: Dict[str, Optional[FrozenSet[SnowflakeShareObject]]] = {}
        for share in outbound_shares:
            granted = self._fetch_share_grants(connection, share)
            db = share.database_name.upper()
            if result.get(db) is None and db in result:
                continue
            if granted is None:
                result[db] = None
            else:
                existing = set(result.get(db) or frozenset())
                result[db] = frozenset(existing | granted)
                self.report.num_shared_objects_discovered += len(granted)

        return {db: grants for db, grants in result.items() if grants is not None}

    def _fetch_outbound_shares(
        self,
        connection: SnowflakeConnection,
        outbound_databases: set,
    ) -> List[SnowflakeOutboundShare]:
        """Runs SHOW SHARES and returns outbound shares matching our configured databases."""
        try:
            rows = connection.query(SnowflakeQuery.show_shares())
        except Exception as e:
            self.report.shares_discovery_failed = True
            self.report.warning(
                title="Share object enumeration failed — ghost sibling nodes may appear",
                message="SHOW SHARES failed. The role used for ingestion may lack ACCOUNTADMIN "
                "or share ownership privileges. All objects in shared databases will get siblings emitted. "
                "Disable enumerate_share_objects to suppress this warning.",
                exc=e,
            )
            return []

        outbound_databases_upper = {db.upper() for db in outbound_databases}
        upper_to_config: Dict[str, str] = {db.upper(): db for db in outbound_databases}

        shares = []
        for row in rows:
            kind = str(row.get("kind", "")).upper()
            if kind != "OUTBOUND":
                continue
            db_name_upper = str(row.get("database_name", "")).upper()
            share_name = str(row.get("name", ""))
            if db_name_upper not in outbound_databases_upper:
                continue
            shares.append(
                SnowflakeOutboundShare(
                    share_name=share_name,
                    database_name=upper_to_config[db_name_upper],
                )
            )

        self.report.num_outbound_shares_discovered = len(shares)
        return shares

    def _fetch_share_grants(
        self,
        connection: SnowflakeConnection,
        share: SnowflakeOutboundShare,
    ) -> Optional[FrozenSet[SnowflakeShareObject]]:
        """
        Runs DESCRIBE SHARE to get the exact objects granted to a share.
        Returns None if the query fails, signalling that siblings should be
        emitted for all objects in the database (no filtering).
        """
        try:
            rows = connection.query(SnowflakeQuery.describe_share(share.share_name))
        except Exception as e:
            self.report.warning(
                title="Share object enumeration failed — ghost sibling nodes may appear",
                message=f"DESCRIBE SHARE failed for share {share.share_name!r}. "
                "Siblings will be emitted for all objects in the shared database.",
                exc=e,
            )
            return None

        granted: set = set()
        for row in rows:
            kind = str(row.get("kind", "")).upper()
            if kind not in ("TABLE", "VIEW"):
                continue
            # DESCRIBE SHARE returns name as DATABASE.SCHEMA.OBJECT
            fqn = str(row.get("name", ""))
            parts = fqn.split(".")
            if len(parts) != 3:
                logger.debug(f"Unexpected DESCRIBE SHARE name format: {fqn!r}")
                continue
            db, schema, name = parts
            granted.add(
                SnowflakeShareObject(
                    database=db.lower(),
                    schema=schema.lower(),
                    name=name.lower(),
                )
            )

        return frozenset(granted)

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
