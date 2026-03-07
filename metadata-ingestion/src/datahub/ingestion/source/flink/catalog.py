import logging
from typing import Iterable, Set

from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.constants import FLINK_TYPE_MAP
from datahub.ingestion.source.flink.report import FlinkSourceReport
from datahub.ingestion.source.flink.sql_gateway_client import FlinkSQLGatewayClient
from datahub.sdk.dataset import Dataset

logger = logging.getLogger(__name__)


class FlinkCatalogKey(ContainerKey):
    catalog_name: str


class FlinkCatalogDatabaseKey(FlinkCatalogKey):
    database_name: str


def _map_flink_type(flink_type: str) -> str:
    """Map a Flink SQL type string to a DataHub type string."""
    normalized = flink_type.upper().split("(")[0].strip()
    return FLINK_TYPE_MAP.get(normalized, flink_type)


class FlinkCatalogExtractor:
    """Extracts catalog metadata (databases, tables, schemas) via SQL Gateway."""

    def __init__(
        self,
        config: FlinkSourceConfig,
        sql_client: FlinkSQLGatewayClient,
        report: FlinkSourceReport,
    ) -> None:
        self.config = config
        self.sql_client = sql_client
        self.report = report
        self._emitted_catalogs: Set[str] = set()

    def extract(self) -> Iterable[MetadataWorkUnit]:
        try:
            catalogs = self.sql_client.get_catalogs()
        except Exception as e:
            self.report.warning(
                title="Failed to list catalogs",
                message="Could not retrieve catalogs from SQL Gateway.",
                context=str(e),
                exc=e,
            )
            return

        for catalog in catalogs:
            if not self.config.catalog_pattern.allowed(catalog):
                continue
            self.report.report_catalog_scanned()
            yield from self._emit_catalog_container(catalog)
            yield from self._extract_catalog(catalog)

    def _extract_catalog(self, catalog: str) -> Iterable[MetadataWorkUnit]:
        try:
            databases = self.sql_client.get_databases(catalog)
        except Exception as e:
            self.report.warning(
                title="Failed to list databases",
                message="Could not retrieve databases from catalog.",
                context=f"catalog={catalog}, error={e}",
                exc=e,
            )
            return

        for database in databases:
            self.report.report_database_scanned()
            yield from self._emit_database_container(catalog, database)
            yield from self._extract_database(catalog, database)

    def _extract_database(
        self, catalog: str, database: str
    ) -> Iterable[MetadataWorkUnit]:
        try:
            tables = self.sql_client.get_tables(catalog, database)
        except Exception as e:
            self.report.warning(
                title="Failed to list tables",
                message="Could not retrieve tables from database.",
                context=f"database={catalog}.{database}, error={e}",
                exc=e,
            )
            return

        for table in tables:
            try:
                yield from self._process_table(catalog, database, table)
                self.report.report_table_scanned()
            except Exception as e:
                self.report.warning(
                    title="Failed to process table",
                    message="Table metadata extraction failed.",
                    context=f"table={catalog}.{database}.{table}, error={e}",
                    exc=e,
                )

    def _catalog_key(self, catalog: str) -> FlinkCatalogKey:
        return FlinkCatalogKey(
            platform="flink",
            catalog_name=catalog,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _emit_catalog_container(self, catalog: str) -> Iterable[MetadataWorkUnit]:
        if catalog in self._emitted_catalogs:
            return
        self._emitted_catalogs.add(catalog)
        yield from gen_containers(
            container_key=self._catalog_key(catalog),
            name=catalog,
            sub_types=[DatasetContainerSubTypes.CATALOG],
            qualified_name=catalog,
        )

    def _emit_database_container(
        self, catalog: str, database: str
    ) -> Iterable[MetadataWorkUnit]:
        db_key = FlinkCatalogDatabaseKey(
            platform="flink",
            catalog_name=catalog,
            database_name=database,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from gen_containers(
            container_key=db_key,
            name=database,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            parent_container_key=self._catalog_key(catalog),
            qualified_name=f"{catalog}.{database}",
            extra_properties={"catalog": catalog, "database": database},
        )

    def _process_table(
        self, catalog: str, database: str, table: str
    ) -> Iterable[MetadataWorkUnit]:
        columns = self.sql_client.get_table_schema(catalog, database, table)

        schema_fields = [
            (col.name, _map_flink_type(col.type), col.comment or "") for col in columns
        ]

        dataset_name = f"{catalog}.{database}.{table}"
        db_key = FlinkCatalogDatabaseKey(
            platform="flink",
            catalog_name=catalog,
            database_name=database,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        dataset = Dataset(
            platform="flink",
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=f"Flink catalog table: {dataset_name}",
            qualified_name=dataset_name,
            custom_properties={
                "catalog": catalog,
                "database": database,
                "table": table,
            },
            schema=schema_fields if schema_fields else None,
            subtype="TABLE",
            parent_container=db_key,
        )
        yield from dataset.as_workunits()
