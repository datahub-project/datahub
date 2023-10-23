import logging
from typing import Callable, Iterable, List

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    DatabaseId,
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


class SnowflakeSharesHandler(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        dataset_urn_builder: Callable[[str], str],
    ) -> None:
        self.config = config
        self.report = report
        self.logger = logger
        self.dataset_urn_builder = dataset_urn_builder

    def get_shares_workunits(
        self, databases: List[SnowflakeDatabase]
    ) -> Iterable[MetadataWorkUnit]:
        inbounds = self.config.inbounds()
        outbounds = self.config.outbounds()
        # None of the databases are shared
        if not (inbounds or outbounds):
            return

        logger.debug("Checking databases for inbound or outbound shares.")
        for db in databases:
            is_inbound = db.name in inbounds
            is_outbound = db.name in outbounds

            if not (is_inbound or is_outbound):
                logger.debug(f"database {db.name} is not shared.")
                continue

            sibling_dbs = (
                list(outbounds[db.name]) if is_outbound else [inbounds[db.name]]
            )

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
                        is_outbound,
                        sibling_dbs,
                    )

                    if is_inbound:
                        assert len(sibling_dbs) == 1
                        # SnowflakeLineageExtractor is unaware of database->schema->table hierarchy
                        # hence this lineage code is not written in SnowflakeLineageExtractor
                        # also this is not governed by configs include_table_lineage and include_view_lineage
                        yield self.get_upstream_lineage_with_primary_sibling(
                            db.name, schema.name, table_name, sibling_dbs[0]
                        )

        self.report_missing_databases(
            databases, list(inbounds.keys()), list(outbounds.keys())
        )

    def report_missing_databases(
        self,
        databases: List[SnowflakeDatabase],
        inbounds: List[str],
        outbounds: List[str],
    ) -> None:
        db_names = [db.name for db in databases]
        missing_dbs = [db for db in inbounds + outbounds if db not in db_names]

        if missing_dbs and self.config.platform_instance:
            self.report_warning(
                "snowflake-shares",
                f"Databases {missing_dbs} were not ingested. Siblings/Lineage will not be set for these.",
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
