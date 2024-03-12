import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List

from databricks.sdk.service.catalog import ColumnTypeName
from databricks.sdk.service.sql import QueryStatementType

from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    CatalogType,
    Column,
    Metastore,
    Query,
    Schema,
    ServicePrincipal,
    Table,
    TableType,
)
from tests.performance import data_model
from tests.performance.data_generation import SeedMetadata
from tests.performance.data_model import ColumnType, StatementType


class UnityCatalogApiProxyMock:
    """Mimics UnityCatalogApiProxy for performance testing."""

    def __init__(
        self,
        seed_metadata: SeedMetadata,
        queries: Iterable[data_model.Query] = (),
        num_service_principals: int = 0,
    ) -> None:
        self.seed_metadata = seed_metadata
        self.queries = queries
        self.num_service_principals = num_service_principals
        self.warehouse_id = "invalid-warehouse-id"

        # Cache for performance
        self._schema_to_table: Dict[str, List[data_model.Table]] = defaultdict(list)
        for table in seed_metadata.all_tables:
            self._schema_to_table[table.container.name].append(table)

    def check_basic_connectivity(self) -> bool:
        return True

    def assigned_metastore(self) -> Metastore:
        container = self.seed_metadata.containers[0][0]
        return Metastore(
            id=container.name,
            name=container.name,
            global_metastore_id=container.name,
            metastore_id=container.name,
            comment=None,
            owner=None,
            cloud=None,
            region=None,
        )

    def catalogs(self, metastore: Metastore) -> Iterable[Catalog]:
        for container in self.seed_metadata.containers[1]:
            if not container.parent or metastore.name != container.parent.name:
                continue

            yield Catalog(
                id=f"{metastore.id}.{container.name}",
                name=container.name,
                metastore=metastore,
                comment=None,
                owner=None,
                type=CatalogType.MANAGED_CATALOG,
            )

    def schemas(self, catalog: Catalog) -> Iterable[Schema]:
        for container in self.seed_metadata.containers[2]:
            # Assumes all catalog names are unique
            if not container.parent or catalog.name != container.parent.name:
                continue

            yield Schema(
                id=f"{catalog.id}.{container.name}",
                name=container.name,
                catalog=catalog,
                comment=None,
                owner=None,
            )

    def tables(self, schema: Schema) -> Iterable[Table]:
        for table in self._schema_to_table[schema.name]:
            columns = []
            for i, col_name in enumerate(table.columns):
                column = table.columns[col_name]
                columns.append(
                    Column(
                        id=column.name,
                        name=column.name,
                        type_name=_convert_column_type(column.type),
                        type_text=column.type.value,
                        nullable=column.nullable,
                        position=i,
                        comment=None,
                        type_precision=0,
                        type_scale=0,
                    )
                )

            yield Table(
                id=f"{schema.id}.{table.name}",
                name=table.name,
                schema=schema,
                table_type=TableType.VIEW if table.is_view() else TableType.MANAGED,
                columns=columns,
                created_at=datetime.now(tz=timezone.utc),
                comment=None,
                owner=None,
                storage_location=None,
                data_source_format=None,
                generation=None,
                created_by="",
                updated_at=None,
                updated_by=None,
                table_id="",
                view_definition=table.definition
                if isinstance(table, data_model.View)
                else None,
                properties={},
            )

    def service_principals(self) -> Iterable[ServicePrincipal]:
        for i in range(self.num_service_principals):
            yield ServicePrincipal(
                id=str(i),
                application_id=str(uuid.uuid4()),
                display_name=f"user-{i}",
                active=True,
            )

    def query_history(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Iterable[Query]:
        for i, query in enumerate(self.queries):
            yield Query(
                query_id=str(i),
                query_text=query.text,
                statement_type=_convert_statement_type(query.type),
                start_time=query.timestamp,
                end_time=query.timestamp,
                user_id=hash(query.actor),
                user_name=query.actor,
                executed_as_user_id=hash(query.actor),
                executed_as_user_name=None,
            )

    def table_lineage(self, table: Table) -> None:
        pass

    def get_column_lineage(self, table: Table) -> None:
        pass


def _convert_column_type(t: ColumnType) -> ColumnTypeName:
    if t == ColumnType.INTEGER:
        return ColumnTypeName.INT
    elif t == ColumnType.FLOAT:
        return ColumnTypeName.DOUBLE
    elif t == ColumnType.STRING:
        return ColumnTypeName.STRING
    elif t == ColumnType.BOOLEAN:
        return ColumnTypeName.BOOLEAN
    elif t == ColumnType.DATETIME:
        return ColumnTypeName.TIMESTAMP
    else:
        raise ValueError(f"Unknown column type: {t}")


def _convert_statement_type(t: StatementType) -> QueryStatementType:
    if t == "CUSTOM" or t == "UNKNOWN":
        return QueryStatementType.OTHER
    else:
        return QueryStatementType[t]
