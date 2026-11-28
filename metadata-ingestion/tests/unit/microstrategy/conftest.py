from typing import Dict, List, Optional

import pytest

from datahub.ingestion.source.microstrategy.client import MicroStrategyAPIError
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    ModelTablesResponse,
    SqlView,
)
from datahub.ingestion.source.microstrategy.source import MicroStrategySource


@pytest.fixture
def mixed_datasources() -> List[Datasource]:
    return [
        Datasource.model_validate(
            {
                "id": "AAA111",
                "name": "oracle_ds",
                "database": {"type": "oracle", "name": "ORA_DB", "schema": "ORA"},
            }
        ),
        Datasource.model_validate(
            {
                "id": "BBB222",
                "name": "snowflake_ds",
                "database": {"type": "snow_flake", "name": "SNOW_DB", "schema": "SNOW"},
            }
        ),
    ]


def install_client(source: MicroStrategySource, client: object) -> None:
    source.client = client  # type: ignore[assignment]  # duck-typed fake


def model_table(
    name: str, fact_id: str, datasource_id: Optional[str]
) -> Dict[str, object]:
    table: Dict[str, object] = {
        "information": {"objectId": f"logical-{fact_id}", "name": name},
        "physicalTable": {"tableName": name},
        "facts": [
            {"information": {"objectId": fact_id}, "expression": {"text": "AMOUNT"}}
        ],
        "attributes": [],
    }
    if datasource_id:
        table["primaryDataSource"] = {"objectId": datasource_id, "name": "ignored"}
    return table


class ModelTablesClient:
    def __init__(self, tables: List[Dict[str, object]]) -> None:
        self.tables = tables
        self.fields_requested: List[str] = []
        self.offsets_requested: List[int] = []

    def list_model_tables(
        self,
        project_id: str,
        limit: Optional[int] = None,
        offset: int = 0,
        fields: str = "",
    ) -> ModelTablesResponse:
        self.fields_requested.append(fields)
        self.offsets_requested.append(offset)
        page = self.tables[offset : offset + limit] if limit else self.tables[offset:]
        return ModelTablesResponse(tables=page, total=len(self.tables))


class ConnectionModelTablesClient(ModelTablesClient):
    def __init__(
        self,
        tables: List[Dict[str, object]],
        fail_connection: bool = False,
        connections: Optional[Dict[str, Dict[str, object]]] = None,
    ) -> None:
        super().__init__(tables)
        self.fail_connection = fail_connection
        self.connections = connections or {}
        self.connection_requests: List[str] = []

    def get_datasource_connection(
        self, connection_id: str, project_id: Optional[str] = None
    ) -> DatasourceConnection:
        self.connection_requests.append(connection_id)
        if self.fail_connection:
            raise MicroStrategyAPIError("connection lookup failed")
        return DatasourceConnection.model_validate(
            {
                "id": connection_id,
                "name": "oracle_connection",
                "database": {"type": "oracle"},
                "connectionString": "SCHEMA=CONN_SCHEMA",
                **self.connections.get(connection_id, {}),
            }
        )


class ReportSqlModelTablesClient(ConnectionModelTablesClient):
    def create_report_instance(self, project_id: str, report_id: str) -> str:
        return f"ri-{report_id}"

    def get_report_sql_view(
        self, project_id: str, report_id: str, instance_id: str
    ) -> SqlView:
        return SqlView(sql_statement="select amount from orders")

    def delete_report_instance(self, *args: object) -> bool:
        return True
