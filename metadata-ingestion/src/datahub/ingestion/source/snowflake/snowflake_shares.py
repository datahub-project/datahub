import logging
from typing import Dict, Iterable, List

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
