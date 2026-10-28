"""End-to-end runs of UnityCatalogSource.get_workunits_internal against a mocked
Databricks WorkspaceClient (the only system boundary, together with the
databricks-sql ``connect`` used for information_schema tag queries).

Everything between the SDK and the emitted workunits is real code: the API
proxy, the lineage parsing, the container / dataset aspect builders, the usage
extractor and the ANALYZE profiler.
"""

from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, cast
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    DataSourceFormat,
    ForeignKeyConstraint,
    GetMetastoreSummaryResponse,
    PrimaryKeyConstraint,
    SchemaInfo,
    TableConstraint,
    TableType,
)
from databricks.sdk.service.iam import Group, ServicePrincipal
from databricks.sdk.service.sql import (
    StatementResponse,
    StatementState,
    StatementStatus,
)
from databricks.sdk.service.workspace import Language, ObjectInfo, ObjectType

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import TableInfoWithGeneration
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    ContainerClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    GlobalTagsClass,
    OwnershipClass,
    SchemaMetadataClass,
    SiblingsClass,
    StatusClass,
    UpstreamLineageClass,
)

WORKSPACE_URL = "https://my-workspace.cloud.databricks.com"
METASTORE = "primary"
CATALOG = "main"
SCHEMA = "sales"
NOTEBOOK_ID = 42
NOTEBOOK_PATH = "/Users/me/etl"
SERVICE_PRINCIPAL_APP_ID = "app-1234"
GROUP_NAME = "data-eng"
_ANALYZE_STATS = {
    "spark.sql.statistics.numRows": "100",
    "spark.sql.statistics.totalSize": "4096",
    "spark.sql.statistics.colStats.id.nullCount": "0",
    "spark.sql.statistics.colStats.id.distinctCount": "100",
}


def _column(name: str, type_text: str = "int", position: int = 0) -> ColumnInfo:
    return ColumnInfo(name=name, type_text=type_text, position=position)


def _table_info(
    name: str,
    table_type: TableType,
    columns: Sequence[ColumnInfo],
    **extra: Any,
) -> TableInfoWithGeneration:
    return TableInfoWithGeneration(
        name=name,
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        table_type=table_type,
        columns=list(columns),
        **extra,
    )


def _lineage_api_response(path: str, body: Dict[str, Any]) -> Any:
    """Serve the HTTP lineage endpoints (lineage_data_source=API)."""
    if path.endswith("/table-lineage"):
        if body["table_name"] != f"{CATALOG}.{SCHEMA}.orders":
            return {}
        return {
            "upstreams": [
                {
                    "tableInfo": {
                        "catalog_name": CATALOG,
                        "schema_name": SCHEMA,
                        "name": "raw_orders",
                    },
                    "notebookInfos": [{"notebook_id": NOTEBOOK_ID}],
                },
                {
                    "fileInfo": {
                        "path": "s3://bucket/topics/orders/{20260101,20260102}",
                        "has_permission": True,
                        "securable_type": "EXTERNAL_LOCATION",
                    }
                },
                {"fileInfo": {"path": "abfss://container@acct/orders"}},
                {"fileInfo": {"path": ""}},
            ],
            "downstreams": [{"notebookInfos": [{"notebook_id": NOTEBOOK_ID}]}],
        }
    if path.endswith("/column-lineage"):
        if body["column_name"] != "id":
            return {}
        return {
            "upstream_cols": [
                {
                    "catalog_name": CATALOG,
                    "schema_name": SCHEMA,
                    "table_name": "raw_orders",
                    "name": "order_id",
                }
            ]
        }
    raise AssertionError(f"unexpected API call {path}")


def _tag_rows(query: str) -> List[Any]:
    if "schema_tags" in query:
        return [(CATALOG, SCHEMA, "domain", "sales")]
    if "catalog_tags" in query:
        return [(CATALOG, "tier", "gold")]
    if "table_tags" in query:
        return [(CATALOG, SCHEMA, "orders", "pii", None)]
    if "column_tags" in query:
        return [(CATALOG, SCHEMA, "orders", "id", "key", "primary")]
    return []


def _workspace_client() -> MagicMock:
    ws = MagicMock(spec=WorkspaceClient)
    ws.config.host = WORKSPACE_URL
    ws.config.warehouse_id = "wh-1"
    ws.metastores.summary.return_value = GetMetastoreSummaryResponse(name=METASTORE)
    ws.catalogs.list.return_value = [
        CatalogInfo(name=CATALOG, owner=GROUP_NAME, comment="main catalog"),
        CatalogInfo(name=None),
    ]
    ws.schemas.list.return_value = [
        SchemaInfo(name=SCHEMA, owner=SERVICE_PRINCIPAL_APP_ID),
        SchemaInfo(name="denied"),
    ]
    ws.tables.list.return_value = [
        _table_info(
            "orders",
            TableType.MANAGED,
            [_column("id"), _column("amount", "decimal(10,2)", 1)],
            owner=SERVICE_PRINCIPAL_APP_ID,
            created_at=1_700_000_000_000,
            created_by="alice@example.com",
            updated_at=1_700_000_100_000,
            updated_by="bob@example.com",
            generation=3,
            table_id="tbl-1",
            properties={"delta.minReaderVersion": "1"},
        ),
        _table_info(
            "orders_view",
            TableType.VIEW,
            [_column("id")],
            view_definition=f"SELECT id FROM {CATALOG}.{SCHEMA}.orders",
        ),
        _table_info(
            "orders_external",
            TableType.EXTERNAL,
            [_column("id")],
            data_source_format=DataSourceFormat.DELTA,
            storage_location="s3://bucket/lake/orders_external",
        ),
        _table_info("internal_scratch", TableType.MANAGED, [_column("id")]),
    ]
    ws.tables.get.return_value = TableInfoWithGeneration(
        name="orders",
        properties=_ANALYZE_STATS,
        columns=[_column("id")],
        table_constraints=[
            TableConstraint(
                primary_key_constraint=PrimaryKeyConstraint(
                    name="pk_orders", child_columns=["id"]
                )
            ),
            TableConstraint(
                foreign_key_constraint=ForeignKeyConstraint(
                    name="fk_customer",
                    child_columns=["id"],
                    parent_table=f"{CATALOG}.{SCHEMA}.customers",
                    parent_columns=["customer_id"],
                )
            ),
            TableConstraint(
                foreign_key_constraint=ForeignKeyConstraint(
                    name="fk_bad_parent",
                    child_columns=["id"],
                    parent_table="not_three_parts",
                    parent_columns=["x"],
                )
            ),
            TableConstraint(
                foreign_key_constraint=ForeignKeyConstraint(
                    name="fk_empty_columns",
                    child_columns=[],
                    parent_table=f"{CATALOG}.{SCHEMA}.customers",
                    parent_columns=[],
                )
            ),
        ],
    )
    ws.registered_models.list.return_value = []
    ws.service_principals.list.return_value = [
        ServicePrincipal(
            id="sp-1",
            display_name="etl-service",
            application_id=SERVICE_PRINCIPAL_APP_ID,
        ),
        ServicePrincipal(id="sp-2", display_name="no-app-id"),
    ]
    ws.groups.list.return_value = [Group(display_name=GROUP_NAME)]
    ws.workspace.list.return_value = [
        ObjectInfo(
            object_type=ObjectType.NOTEBOOK,
            object_id=NOTEBOOK_ID,
            path=NOTEBOOK_PATH,
            language=Language.PYTHON,
            created_at=1_700_000_000_000,
            modified_at=1_700_000_200_000,
        ),
        ObjectInfo(
            object_type=ObjectType.NOTEBOOK,
            object_id=43,
            path="/Shared/ignored",
        ),
        ObjectInfo(object_type=ObjectType.DIRECTORY, object_id=1, path="/Users"),
    ]
    ws.api_client.do.side_effect = lambda method="GET", path="", body=None, **_: (
        _lineage_api_response(path, body or {})
    )
    return ws


def _config(**extra: Any) -> UnityCatalogSourceConfig:
    return UnityCatalogSourceConfig.model_validate(
        {
            "token": "t",
            "workspace_url": WORKSPACE_URL,
            "warehouse_id": "wh-1",
            "include_hive_metastore": False,
            "include_usage_statistics": False,
            **extra,
        }
    )


@dataclass
class _Run:
    source: UnityCatalogSource
    workunits: List[MetadataWorkUnit]

    def aspects(self, urn: str, aspect_type: type) -> List[Any]:
        found: List[Any] = []
        for wu in self.workunits:
            mcp = wu.metadata
            if getattr(mcp, "entityUrn", None) != urn:
                continue
            aspect = getattr(mcp, "aspect", None)
            if isinstance(aspect, aspect_type):
                found.append(aspect)
        return found

    def aspect(self, urn: str, aspect_type: type) -> Any:
        aspects = self.aspects(urn, aspect_type)
        assert len(aspects) == 1, f"expected one {aspect_type.__name__} on {urn}"
        return aspects[0]

    def urns(self, aspect_type: type) -> List[str]:
        return [
            cast(str, getattr(wu.metadata, "entityUrn", None))
            for wu in self.workunits
            if isinstance(getattr(wu.metadata, "aspect", None), aspect_type)
        ]


def _run(ws: MagicMock, config: UnityCatalogSourceConfig) -> _Run:
    fake_cursor = MagicMock()
    fake_cursor.fetchall.side_effect = lambda: _tag_rows(
        fake_cursor.execute.call_args[0][0]
    )
    fake_cursor.fetchmany.return_value = []
    fake_connection = MagicMock()
    fake_connection.__enter__.return_value = fake_connection
    fake_connection.cursor.return_value.__enter__.return_value = fake_cursor
    with (
        patch(
            "datahub.ingestion.source.unity.source.create_workspace_client",
            return_value=ws,
        ),
        patch(
            "datahub.ingestion.source.unity.proxy.connect",
            return_value=fake_connection,
        ),
    ):
        source = UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=config)
        workunits = list(source.get_workunits_internal())
        source.close()
    return _Run(source=source, workunits=workunits)


def _dataset_urn(run: _Run, name: str) -> str:
    """URN the source itself would mint for a table in the ingested schema."""
    metastore = METASTORE if run.source.config.include_metastore else None
    return run.source.gen_dataset_urn(
        TableReference(metastore=metastore, catalog=CATALOG, schema=SCHEMA, table=name)
    )


# --------------------------------------------------------------------------- #
# Full metadata run: metastore, ownership, notebooks, HTTP lineage, tags, FKs  #
# --------------------------------------------------------------------------- #


@pytest.fixture
def full_run() -> Iterator[_Run]:
    config = _config(
        include_metastore=True,
        include_ownership=True,
        include_notebooks=True,
        include_table_lineage=True,
        include_column_lineage=True,
        include_external_lineage=True,
        include_tags=True,
        include_table_constraints=True,
        emit_siblings=True,
        lineage_data_source="API",
        schema_pattern={"deny": [".*\\.denied"]},
        table_pattern={"deny": [".*internal_.*"]},
        notebook_pattern={"deny": ["/Shared/.*"]},
    )
    yield _run(_workspace_client(), config)


def test_containers_are_nested_metastore_catalog_schema(full_run: _Run) -> None:
    report = full_run.source.report
    assert list(report.metastores.processed_entities) == [METASTORE]
    assert list(report.catalogs.processed_entities) == [f"{METASTORE}.{CATALOG}"]
    assert list(report.schemas.processed_entities) == [
        f"{METASTORE}.{CATALOG}.{SCHEMA}"
    ]
    assert list(report.schemas.dropped_entities) == [f"{METASTORE}.{CATALOG}.denied"]
    # A catalog without a name is skipped, not emitted as an empty container.
    assert report.num_catalogs_missing_name == 1

    containers = {
        u for u in full_run.urns(StatusClass) if u.startswith("urn:li:container")
    }
    assert len(containers) == 3
    orders = full_run.aspect(_dataset_urn(full_run, "orders"), ContainerClass)
    assert orders.container in containers


def test_table_pattern_and_notebook_pattern_drop_entities(full_run: _Run) -> None:
    report = full_run.source.report
    assert sorted(report.tables.processed_entities) == [
        f"{METASTORE}.{CATALOG}.{SCHEMA}.orders",
        f"{METASTORE}.{CATALOG}.{SCHEMA}.orders_external",
        f"{METASTORE}.{CATALOG}.{SCHEMA}.orders_view",
    ]
    assert list(report.tables.dropped_entities) == [
        f"{METASTORE}.{CATALOG}.{SCHEMA}.internal_scratch"
    ]
    assert list(report.notebooks.processed_entities) == [NOTEBOOK_PATH]
    assert list(report.notebooks.dropped_entities) == ["/Shared/ignored"]
    assert _dataset_urn(full_run, "internal_scratch") not in full_run.urns(
        DatasetPropertiesClass
    )


def test_owners_resolve_groups_and_service_principals(full_run: _Run) -> None:
    schema_urn = full_run.aspect(
        _dataset_urn(full_run, "orders"), ContainerClass
    ).container
    schema_ownership = full_run.aspect(schema_urn, OwnershipClass)
    # A service-principal application id is replaced by its display name.
    assert schema_ownership.owners[0].owner == "urn:li:corpuser:etl-service"

    table_ownership = full_run.aspect(_dataset_urn(full_run, "orders"), OwnershipClass)
    assert table_ownership.owners[0].owner == "urn:li:corpuser:etl-service"

    catalog_urn = full_run.aspect(schema_urn, ContainerClass).container
    catalog_ownership = full_run.aspect(catalog_urn, OwnershipClass)
    # An owner that is a workspace group becomes a group urn, not a user urn.
    assert catalog_ownership.owners[0].owner == f"urn:li:corpGroup:{GROUP_NAME}"


def test_http_lineage_merges_tables_columns_notebooks_and_external_paths(
    full_run: _Run,
) -> None:
    lineage = full_run.aspect(_dataset_urn(full_run, "orders"), UpstreamLineageClass)
    by_dataset = {u.dataset: u for u in lineage.upstreams}

    raw_orders = by_dataset[_dataset_urn(full_run, "raw_orders")]
    assert raw_orders.type == DatasetLineageTypeClass.TRANSFORMED

    notebook_urn = full_run.source.gen_notebook_urn(NOTEBOOK_ID)
    assert notebook_urn in by_dataset

    # The brace partition set is illegal in a URN and is stripped to the parent path.
    s3 = [
        u for u in by_dataset if u.startswith("urn:li:dataset:(urn:li:dataPlatform:s3")
    ]
    assert s3 == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/topics/orders,PROD)"]
    assert by_dataset[s3[0]].type == DatasetLineageTypeClass.COPY

    assert lineage.fineGrainedLineages is not None
    [fine] = lineage.fineGrainedLineages
    assert fine.downstreams == [
        f"urn:li:schemaField:({_dataset_urn(full_run, 'orders')},id)"
    ]
    assert fine.upstreams == [
        f"urn:li:schemaField:({_dataset_urn(full_run, 'raw_orders')},order_id)"
    ]

    report = full_run.source.report
    assert report.num_external_upstreams_partition_stripped == 1
    assert report.num_external_upstreams_unsupported == 1
    assert report.num_external_upstreams_lacking_permissions == 1


def test_notebook_gets_properties_and_copy_lineage_from_its_table(
    full_run: _Run,
) -> None:
    notebook_urn = full_run.source.gen_notebook_urn(NOTEBOOK_ID)
    props = full_run.aspect(notebook_urn, DatasetPropertiesClass)
    assert props.name == "etl"
    assert props.customProperties == {"path": NOTEBOOK_PATH, "language": "PYTHON"}
    assert props.externalUrl == f"{WORKSPACE_URL}#notebook/{NOTEBOOK_ID}"
    assert props.created is not None and props.lastModified is not None

    # orders lists the notebook as a downstream, so the notebook copies from orders.
    lineage = full_run.aspect(notebook_urn, UpstreamLineageClass)
    assert [(u.dataset, u.type) for u in lineage.upstreams] == [
        (_dataset_urn(full_run, "orders"), DatasetLineageTypeClass.COPY)
    ]


def test_external_delta_table_gets_delta_lake_sibling_and_lineage(
    full_run: _Run,
) -> None:
    urn = _dataset_urn(full_run, "orders_external")
    delta_urn = "urn:li:dataset:(urn:li:dataPlatform:delta-lake,bucket/lake/orders_external,PROD)"
    assert full_run.aspect(urn, SiblingsClass).siblings == [delta_urn]
    assert full_run.aspect(delta_urn, SiblingsClass).primary
    lineage = full_run.aspect(urn, UpstreamLineageClass)
    assert [u.dataset for u in lineage.upstreams] == [delta_urn]


def test_constraints_become_primary_and_foreign_keys(full_run: _Run) -> None:
    schema = full_run.aspect(_dataset_urn(full_run, "orders"), SchemaMetadataClass)
    assert schema.primaryKeys == ["id"]
    by_name = {f.fieldPath: f for f in schema.fields}
    assert by_name["id"].isPartOfKey
    assert not by_name["amount"].isPartOfKey

    assert schema.foreignKeys is not None
    # Unparseable parent tables and empty column lists are skipped, not emitted.
    [fk] = schema.foreignKeys
    assert fk.name == "fk_customer"
    assert fk.foreignDataset == _dataset_urn(full_run, "customers")
    assert fk.sourceFields == [
        f"urn:li:schemaField:({_dataset_urn(full_run, 'orders')},id)"
    ]


def test_table_properties_carry_audit_and_storage_details(full_run: _Run) -> None:
    props = full_run.aspect(_dataset_urn(full_run, "orders"), DatasetPropertiesClass)
    assert props.customProperties["table_type"] == "MANAGED"
    assert props.customProperties["generation"] == "3"
    assert props.customProperties["table_id"] == "tbl-1"
    assert props.customProperties["owner"] == SERVICE_PRINCIPAL_APP_ID
    assert props.customProperties["delta.minReaderVersion"] == "1"
    assert props.created is not None
    assert props.created.actor == "urn:li:corpuser:alice@example.com"
    assert props.lastModified is not None
    assert props.lastModified.actor == "urn:li:corpuser:bob@example.com"
    assert props.lastModified.time > props.created.time


def test_information_schema_tags_land_on_tables_columns_and_containers(
    full_run: _Run,
) -> None:
    table_tags = full_run.aspect(_dataset_urn(full_run, "orders"), GlobalTagsClass)
    assert [t.tag for t in table_tags.tags] == ["urn:li:tag:pii"]

    schema = full_run.aspect(_dataset_urn(full_run, "orders"), SchemaMetadataClass)
    id_field = next(f for f in schema.fields if f.fieldPath == "id")
    assert id_field.globalTags is not None
    assert [t.tag for t in id_field.globalTags.tags] == ["urn:li:tag:key:primary"]

    schema_urn = full_run.aspect(
        _dataset_urn(full_run, "orders"), ContainerClass
    ).container
    assert [t.tag for t in full_run.aspect(schema_urn, GlobalTagsClass).tags] == [
        "urn:li:tag:domain:sales"
    ]
    catalog_urn = full_run.aspect(schema_urn, ContainerClass).container
    assert [t.tag for t in full_run.aspect(catalog_urn, GlobalTagsClass).tags] == [
        "urn:li:tag:tier:gold"
    ]


def test_view_definition_is_emitted_and_view_is_not_profiled(full_run: _Run) -> None:
    from datahub.metadata.schema_classes import ViewPropertiesClass

    view = full_run.aspect(_dataset_urn(full_run, "orders_view"), ViewPropertiesClass)
    assert "FROM" in view.viewLogic
    assert full_run.source.view_refs == {
        next(
            ref
            for ref in full_run.source.view_refs | full_run.source.table_refs
            if ref.table == "orders_view"
        )
    }


# --------------------------------------------------------------------------- #
# Warehouse-dependent stages: profiling and usage                              #
# --------------------------------------------------------------------------- #


def _analyze_ws() -> MagicMock:
    ws = _workspace_client()
    ws.statement_execution.execute_statement.return_value = StatementResponse(
        statement_id="s", status=StatementStatus(state=StatementState.PENDING)
    )
    ws.statement_execution.get_statement.return_value = StatementResponse(
        statement_id="s", status=StatementStatus(state=StatementState.SUCCEEDED)
    )
    return ws


def test_analyze_profiling_emits_profiles_for_ingested_tables() -> None:
    config = _config(
        profiling={"enabled": True, "method": "analyze", "warehouse_id": "wh-1"},
        include_tags=False,
    )
    with patch("datahub.ingestion.source.unity.proxy_profiling.time"):
        run = _run(_analyze_ws(), config)

    profile = run.aspect(_dataset_urn(run, "orders"), DatasetProfileClass)
    assert profile.rowCount == 100
    assert profile.sizeInBytes == 4096
    # The warehouse is started (again) before profiling so that it is awake.
    ws = cast(MagicMock, run.source.unity_catalog_api_proxy._workspace_client)
    assert ws.warehouses.start.call_count == 1


def test_profiling_without_a_warehouse_fails_after_metadata_is_emitted() -> None:
    from databricks.sdk.errors import NotFound

    ws = _workspace_client()
    ws.warehouses.start.side_effect = NotFound("no such warehouse")
    config = _config(
        profiling={"enabled": True, "method": "analyze", "warehouse_id": "wh-1"},
        include_tags=False,
    )
    run = _run(ws, config)

    assert _dataset_urn(run, "orders") in run.urns(DatasetPropertiesClass)
    assert run.urns(DatasetProfileClass) == []
    assert any("warehouse" in f.message.lower() for f in run.source.report.failures)


def _query_history_pages(start_ms: int) -> Dict[str, Dict[str, Any]]:
    def query(qid: str, text: str) -> Dict[str, Any]:
        return {
            "query_id": qid,
            "query_text": text,
            "query_start_time_ms": start_ms,
            "query_end_time_ms": start_ms + 1000,
            "user_name": "analyst@example.com",
            "user_id": 7,
            "status": "FINISHED",
            "statement_type": "SELECT",
        }

    return {
        "first": {
            "res": [query("q1", f"SELECT id FROM {CATALOG}.{SCHEMA}.orders")],
            "next_page_token": "page-2",
        },
        "second": {
            "res": [
                query("q2", f"SELECT amount FROM {CATALOG}.{SCHEMA}.orders"),
                {"query_id": "q3", "query_text": None},
            ]
        },
    }


def test_usage_via_query_history_api_pages_and_aggregates() -> None:
    config = _config(
        include_usage_statistics=True,
        usage_data_source="API",
        include_tags=False,
        include_table_lineage=False,
    )
    start_ms = int(config.start_time.timestamp() * 1000) + 3_600_000
    pages = _query_history_pages(start_ms)
    ws = _workspace_client()

    def api(
        method: str, path: str, body: Optional[Dict[str, Any]] = None, **_: Any
    ) -> Any:
        if path.endswith("/queries"):
            return pages["second"] if (body or {}).get("page_token") else pages["first"]
        return _lineage_api_response(path, body or {})

    ws.api_client.do.side_effect = api
    run = _run(ws, config)

    usage = run.aspects(_dataset_urn(run, "orders"), DatasetUsageStatisticsClass)
    assert usage, "usage statistics expected for the queried table"
    assert sum(u.totalSqlQueries or 0 for u in usage) == 2
    report = run.source.report
    assert report.num_queries == 2
    # The third page entry has no query text and is dropped before buffering.
    assert report.num_queries_missing_info == 1


# --------------------------------------------------------------------------- #
# Run-level failures and alternative catalog discovery                         #
# --------------------------------------------------------------------------- #


def test_missing_metastore_fails_the_run_before_any_catalog() -> None:
    ws = _workspace_client()
    ws.metastores.summary.return_value = GetMetastoreSummaryResponse(name=None)
    run = _run(ws, _config(include_metastore=True, include_tags=False))

    assert run.workunits == []
    assert any("metastore" in f.message.lower() for f in run.source.report.failures)
    ws.catalogs.list.assert_not_called()


def test_pinned_catalogs_are_fetched_by_name_and_missing_ones_skipped() -> None:
    ws = _workspace_client()
    ws.catalogs.get.side_effect = lambda name, **_: (
        CatalogInfo(name=name) if name == CATALOG else CatalogInfo(name=None)
    )
    run = _run(ws, _config(catalogs=[CATALOG, "retired"], include_tags=False))

    ws.catalogs.list.assert_not_called()
    assert list(run.source.report.catalogs.processed_entities) == [CATALOG]
    assert _dataset_urn(run, "orders") in run.urns(DatasetPropertiesClass)


def test_schema_whose_tables_cannot_be_listed_is_skipped_not_fatal() -> None:
    ws = _workspace_client()
    ws.schemas.list.return_value = [SchemaInfo(name="broken"), SchemaInfo(name=SCHEMA)]
    good_tables = ws.tables.list.return_value

    def list_tables(catalog_name: str, schema_name: str, **_: Any) -> List[Any]:
        if schema_name == "broken":
            raise RuntimeError("PERMISSION_DENIED")
        return cast(List[Any], good_tables)

    ws.tables.list.side_effect = list_tables
    run = _run(ws, _config(include_tags=False))

    report = run.source.report
    assert list(report.schemas.processed_entities) == [f"{CATALOG}.{SCHEMA}"]
    assert any("schema" in w.message.lower() for w in report.warnings)
    assert _dataset_urn(run, "orders") in run.urns(DatasetPropertiesClass)


def test_sqlalchemy_profiler_only_receives_tables_matching_the_profile_pattern() -> (
    None
):
    config = _config(
        include_tags=False,
        schema_pattern={"deny": [".*\\.denied"]},
        profiling={
            "enabled": True,
            "method": "sqlalchemy",
            "warehouse_id": "wh-1",
            "pattern": {"deny": [".*orders_external"]},
        },
    )
    with patch(
        "datahub.ingestion.source.unity.source.UnityCatalogProfiler"
    ) as profiler_cls:
        profiler_cls.return_value.get_workunits.return_value = iter([])
        run = _run(_workspace_client(), config)

    (handed_over,) = cast(MagicMock, profiler_cls.return_value.get_workunits).call_args[
        0
    ]
    # Views are never profiled and the deny pattern drops the external table.
    assert sorted(t.name for t in handed_over) == ["internal_scratch", "orders"]
    assert run.source.report.failures == []


# --------------------------------------------------------------------------- #
# Tag platform resources (DataHub graph present)                               #
# --------------------------------------------------------------------------- #


def _source_with_graph() -> UnityCatalogSource:
    from datahub.ingestion.graph.client import DataHubGraph

    ctx = PipelineContext(run_id="t", graph=MagicMock(spec=DataHubGraph))
    with patch(
        "datahub.ingestion.source.unity.source.create_workspace_client",
        return_value=_workspace_client(),
    ):
        return UnityCatalogSource(ctx=ctx, config=_config())


def test_platform_resource_is_emitted_once_per_tag_and_linked_to_its_datahub_tag() -> (
    None
):
    from datahub.api.entities.external.unity_catalog_external_entites import (
        UnityCatalogTag,
    )
    from datahub.ingestion.source.unity.tag_entities import (
        UnityCatalogTagPlatformResource,
        UnityCatalogTagPlatformResourceId,
    )

    source = _source_with_graph()
    repository = MagicMock()
    source.platform_resource_repository = repository
    tag = UnityCatalogTag(key="pii", value="email")
    resource = UnityCatalogTagPlatformResource.create_default(
        UnityCatalogTagPlatformResourceId(
            tag_key="pii", tag_value="email", platform_instance=None
        ),
        managed_by_datahub=False,
    )
    repository.get_entity_from_datahub.return_value = resource

    first = list(source.gen_platform_resources([tag]))
    second = list(source.gen_platform_resources([tag]))

    assert len(first) >= 1 and all(
        wu.id.startswith("platform_resource-") for wu in first
    )
    assert resource.datahub_linked_resources().urns == [tag.to_datahub_tag_urn().urn()]
    # Already linked on the second pass: nothing new to emit.
    assert second == []


def test_platform_resource_lookup_failure_is_a_warning_and_skips_the_tag() -> None:
    from datahub.api.entities.external.unity_catalog_external_entites import (
        UnityCatalogTag,
    )

    source = _source_with_graph()
    repository = MagicMock()
    repository.get_entity_from_datahub.side_effect = RuntimeError("graph down")
    source.platform_resource_repository = repository

    assert list(source.gen_platform_resources([UnityCatalogTag(key="pii")])) == []
    assert len(source.report.warnings) == 1


# --------------------------------------------------------------------------- #
# Legacy hive_metastore catalog (SQLAlchemy over the SQL warehouse)            #
# --------------------------------------------------------------------------- #

_HiveTableRow = namedtuple("_HiveTableRow", "database tableName isTemporary")
_HiveViewRow = namedtuple(
    "_HiveViewRow", "namespace viewName isTemporary isMaterialized"
)
_DescribeRow = namedtuple("_DescribeRow", "col_name data_type comment")

HIVE_SCHEMA = "sales"
HIVE_RESPONSES: Dict[str, Any] = {
    f"SHOW VIEWS FROM `{HIVE_SCHEMA}`": [
        _HiveViewRow(HIVE_SCHEMA, "v_orders", False, False)
    ],
    f"SHOW TABLES FROM `{HIVE_SCHEMA}`": [
        _HiveTableRow(HIVE_SCHEMA, "orders", False),
        _HiveTableRow(HIVE_SCHEMA, "v_orders", False),
    ],
    f"DESCRIBE EXTENDED `{HIVE_SCHEMA}`.`orders`": [
        _DescribeRow("col_name", "data_type", "comment"),
        _DescribeRow("id", "int", None),
        _DescribeRow("", "", ""),
        _DescribeRow("# Detailed Table Information", "", ""),
        _DescribeRow("Type", "MANAGED", ""),
        _DescribeRow("Provider", "delta", ""),
    ],
    f"DESCRIBE EXTENDED `{HIVE_SCHEMA}`.`v_orders`": [
        _DescribeRow("id", "int", None),
        _DescribeRow("", "", ""),
        _DescribeRow("# Detailed Table Information", "", ""),
        _DescribeRow("Type", "VIEW", ""),
    ],
    f"SHOW CREATE TABLE `{HIVE_SCHEMA}`.`v_orders`": [
        (f"CREATE VIEW {HIVE_SCHEMA}.v_orders AS SELECT id FROM {HIVE_SCHEMA}.orders",)
    ],
}


def _hive_connection() -> MagicMock:
    from sqlalchemy.engine import Connection

    conn = MagicMock(spec=Connection)

    def execute(statement: Any) -> MagicMock:
        result = MagicMock()
        result.fetchall.return_value = HIVE_RESPONSES.get(str(statement), [])
        return result

    conn.execute.side_effect = execute
    return conn


def _hive_run(
    ws: MagicMock, config: UnityCatalogSourceConfig
) -> "tuple[_Run, MagicMock]":
    conn = _hive_connection()
    inspector = MagicMock()
    inspector.get_schema_names.return_value = [HIVE_SCHEMA]
    inspector.bind = conn
    with (
        patch("datahub.ingestion.source.unity.hive_metastore_proxy.create_engine"),
        patch(
            "datahub.ingestion.source.unity.hive_metastore_proxy.inspect",
            return_value=inspector,
        ),
    ):
        run = _run(ws, config)
    return run, conn


def _hive_urn(run: _Run, table: str) -> str:
    return run.source.gen_dataset_urn(
        TableReference(
            metastore=None, catalog="hive_metastore", schema=HIVE_SCHEMA, table=table
        )
    )


def test_hive_metastore_views_get_lineage_parsed_from_their_definition() -> None:
    config = _config(
        include_hive_metastore=True, include_table_lineage=True, include_tags=False
    )
    run, conn = _hive_run(_workspace_client(), config)

    assert run.source.report.hive_metastore_catalog_found
    assert _hive_urn(run, "orders") in run.urns(DatasetPropertiesClass)
    lineage = run.aspect(_hive_urn(run, "v_orders"), UpstreamLineageClass)
    assert [u.dataset for u in lineage.upstreams] == [_hive_urn(run, "orders")]
    # The warehouse-backed SQL connection is released when the source closes.
    cast(MagicMock, conn.close).assert_called_once()


def test_hive_metastore_connection_failure_is_a_warning_and_unity_catalog_proceeds() -> (
    None
):
    config = _config(include_hive_metastore=True, include_tags=False)
    with patch(
        "datahub.ingestion.source.unity.hive_metastore_proxy.create_engine",
        side_effect=RuntimeError("warehouse unreachable"),
    ):
        run = _run(_workspace_client(), config)

    assert not run.source.report.hive_metastore_catalog_found
    assert any("hive_metastore" in w.message for w in run.source.report.warnings)
    assert _dataset_urn(run, "orders") in run.urns(DatasetPropertiesClass)


def test_hive_metastore_without_a_warehouse_aborts_before_any_metadata() -> None:
    from databricks.sdk.errors import NotFound

    ws = _workspace_client()
    ws.warehouses.start.side_effect = NotFound("no such warehouse")
    run, _ = _hive_run(ws, _config(include_hive_metastore=True, include_tags=False))

    assert run.workunits == []
    assert any("warehouse" in f.message.lower() for f in run.source.report.failures)
