import logging
from typing import Callable, Iterable, List

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeDatabaseDataHubId,
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

    def get_workunits(
        self, databases: List[SnowflakeDatabase]
    ) -> Iterable[MetadataWorkUnit]:
        for db in databases:
            inbound = (
                self.config.inbound_shares_map.get(db.name)
                if self.config.inbound_shares_map
                and db.name in self.config.inbound_shares_map
                else None
            )
            outbounds = (
                self.config.outbound_shares_map.get(db.name)
                if self.config.outbound_shares_map
                and db.name in self.config.outbound_shares_map
                else None
            )
            sibling_dbs: List[SnowflakeDatabaseDataHubId]
            if inbound:
                sibling_dbs = [inbound]
            elif outbounds:
                sibling_dbs = outbounds
            else:
                continue
            # TODO: pydantic validator to check that database key in inbound_shares_map is not present in outbound_shares_map
            # TODO: logger statements and exception handling
            for schema in db.schemas:
                for table_name in schema.tables:
                    yield from self.get_siblings(
                        db.name,
                        schema.name,
                        table_name,
                        True if outbounds else False,
                        sibling_dbs,
                    )

                    if inbound:
                        # TODO: Should this be governed by any config flag ? Should this be part of lineage_extractor ?
                        yield self.get_upstream_lineage_with_primary_sibling(
                            db.name, schema.name, table_name, inbound
                        )

    def get_siblings(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        primary: bool,
        sibling_databases: List[SnowflakeDatabaseDataHubId],
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
                    table_name, schema_name, sibling_db.database_name
                ),
                sibling_db.platform_instance,
            )
            for sibling_db in sibling_databases
        ]

        sibling_urns.append(urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=Siblings(primary=primary, siblings=sibling_urns)
        ).as_workunit()

    def get_upstream_lineage_with_primary_sibling(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        primary_sibling_db: SnowflakeDatabaseDataHubId,
    ) -> MetadataWorkUnit:
        dataset_identifier = self.get_dataset_identifier(
            table_name, schema_name, database_name
        )
        urn = self.dataset_urn_builder(dataset_identifier)

        upstream_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            self.get_dataset_identifier(
                table_name, schema_name, primary_sibling_db.database_name
            ),
            primary_sibling_db.platform_instance,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=UpstreamLineage(
                upstreams=[Upstream(dataset=upstream_urn, type=DatasetLineageType.COPY)]
            ),
        ).as_workunit()
