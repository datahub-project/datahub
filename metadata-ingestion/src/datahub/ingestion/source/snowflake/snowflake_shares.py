import logging
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    DatabaseId,
    SnowflakeShareConfig,
    SnowflakeV2Config,
)
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


@dataclass
class SharedDatabase:
    """
    Represents shared database from current platform instance
    This is either created from an inbound share or included in an outbound share.
    """

    name: str
    created_from_share: bool

    # This will have exactly entry if created_from_share = True
    shares: List[str]


class SnowflakeSharesHandler(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        dataset_urn_builder: Callable[[str], str],
    ) -> None:
        self.config = config
        self.shares = self.config.shares or {}
        self.platform_instance = self.config.platform_instance or ""
        self.report = report
        self.logger = logger
        self.dataset_urn_builder = dataset_urn_builder

    def _get_shared_databases(
        self, shares: Dict[str, SnowflakeShareConfig], platform_instance: str
    ) -> Dict[str, SharedDatabase]:
        shared_databases: Dict[str, SharedDatabase] = {}

        for share_name, share_details in shares.items():
            if share_details.platform_instance == platform_instance:
                if share_details.database not in shared_databases:
                    shared_databases[share_details.database] = SharedDatabase(
                        name=share_details.database,
                        created_from_share=False,
                        shares=[share_name],
                    )

                else:
                    shared_databases[share_details.database].shares.append(share_name)

            else:
                for consumer in share_details.consumers:
                    if consumer.platform_instance == platform_instance:
                        shared_databases[consumer.database] = SharedDatabase(
                            name=share_details.database,
                            created_from_share=True,
                            shares=[share_name],
                        )
                        break
                else:
                    self.report_warning(
                        f"Skipping Share, as it does not include current platform instance {platform_instance}",
                        share_name,
                    )

        return shared_databases

    def get_shares_workunits(
        self, databases: List[SnowflakeDatabase]
    ) -> Iterable[MetadataWorkUnit]:
        shared_databases = self._get_shared_databases(
            self.shares, self.platform_instance
        )

        # None of the databases are shared
        if not shared_databases:
            return

        logger.debug("Checking databases for inbound or outbound shares.")
        for db in databases:
            if db.name not in shared_databases:
                logger.debug(f"database {db.name} is not shared.")
                continue

            sibling_dbs = self.get_sibling_databases(shared_databases[db.name])

            for schema in db.schemas:
                for table_name in schema.tables + schema.views:
                    # TODO: If this is outbound database,
                    # 1. attempt listing shares using `show shares` to identify name of share associated with this database (cache query result).
                    # 2. if corresponding share is listed, then run `show grants to share <share_name>` to identify exact tables, views included in share.
                    # 3. emit siblings only for the objects listed above.
                    # This will work only if the configured role has accountadmin role access OR is owner of share.
                    # Otherwise ghost nodes may be shown in "Composed Of" section for tables/views in original database which are not granted to share.
                    yield from self.gen_siblings(
                        db.name,
                        schema.name,
                        table_name,
                        not shared_databases[db.name].created_from_share,
                        sibling_dbs,
                    )

                    if shared_databases[db.name].created_from_share:
                        assert len(sibling_dbs) == 1
                        # SnowflakeLineageExtractor is unaware of database->schema->table hierarchy
                        # hence this lineage code is not written in SnowflakeLineageExtractor
                        # also this is not governed by configs include_table_lineage and include_view_lineage
                        yield self.get_upstream_lineage_with_primary_sibling(
                            db.name, schema.name, table_name, sibling_dbs[0]
                        )

        self.report_missing_databases(databases, shared_databases)

    def get_sibling_databases(self, db: SharedDatabase) -> List[DatabaseId]:
        sibling_dbs: List[DatabaseId] = []
        if db.created_from_share:
            share_details = self.shares[db.shares[0]]
            logger.debug(
                f"database {db.name} is created from inbound share {db.shares[0]}."
            )
            sibling_dbs = [
                DatabaseId(share_details.database, share_details.platform_instance)
            ]

        else:  # not created from share, but is in fact included in share
            logger.debug(
                f"database {db.name} is included as outbound share(s) {db.shares}."
            )
            sibling_dbs = [
                consumer
                for share_name in db.shares
                for consumer in self.shares[share_name].consumers
            ]

        return sibling_dbs

    def report_missing_databases(
        self,
        databases: List[SnowflakeDatabase],
        shared_databases: Dict[str, SharedDatabase],
    ) -> None:
        db_names = [db.name for db in databases]
        missing_dbs = [db for db in shared_databases.keys() if db not in db_names]

        if missing_dbs:
            self.report_warning(
                "snowflake-shares",
                f"Databases {missing_dbs} were not ingested. Siblings/Lineage will not be set for these.",
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
        dataset_identifier = self.get_dataset_identifier(
            table_name, schema_name, database_name
        )
        urn = self.dataset_urn_builder(dataset_identifier)

        sibling_urns = [
            make_dataset_urn_with_platform_instance(
                self.platform,
                self.get_dataset_identifier(
                    table_name, schema_name, sibling_db.database
                ),
                sibling_db.platform_instance,
            )
            for sibling_db in sibling_databases
        ]

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
        dataset_identifier = self.get_dataset_identifier(
            table_name, schema_name, database_name
        )
        urn = self.dataset_urn_builder(dataset_identifier)

        upstream_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            self.get_dataset_identifier(
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
