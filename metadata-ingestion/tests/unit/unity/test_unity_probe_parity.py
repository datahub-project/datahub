from typing import Any, Dict, Iterator, List, Set
from unittest import mock

import pytest
from databricks.sdk.service.catalog import (
    CatalogInfo,
    GetMetastoreSummaryResponse,
    SchemaInfo,
    TableType,
)
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.unity.proxy import TableInfoWithGeneration
from datahub.metadata.schema_classes import DatasetPropertiesClass, SubTypesClass
from datahub.metadata.urns import DatasetUrn, Urn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    by_name,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global secret
# registry, as the CLI does.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

if getattr(TableType, "METRIC_VIEW", None) is None:
    pytest.skip(
        "Installed databricks-sdk lacks TableType.METRIC_VIEW", allow_module_level=True
    )

_SOURCE_TYPE = "unity-catalog"
_WORKSPACE_URL = "https://dummy.cloud.databricks.com"
_LIMIT = 1000

_SCHEMAS: Dict[str, List[str]] = {
    "main": ["analytics", "staging", "information_schema"],
    "other": ["scratch"],
}
_TABLES: Dict[str, List[tuple]] = {
    "analytics": [
        ("orders", TableType.MANAGED),
        ("tmp_load", TableType.MANAGED),
        ("v_orders", TableType.VIEW),
        ("v_hidden", TableType.VIEW),
        ("tmp_view", TableType.MATERIALIZED_VIEW),
        ("kpi", TableType.METRIC_VIEW),
        ("mv_hidden", TableType.METRIC_VIEW),
        ("tmp_kpi", TableType.METRIC_VIEW),
    ],
    "staging": [("t_stage", TableType.MANAGED)],
}
_NOTEBOOKS = ["/Shared/keep_etl", "/Shared/drop_scratch"]

_COLUMNS = [
    {
        "name": "col_a",
        "type_text": "int",
        "type_json": '{"name":"col_a","type":"integer","nullable":true,"metadata":{}}',
        "type_name": "INT",
        "type_precision": 0,
        "type_scale": 0,
        "position": 0,
        "nullable": True,
    }
]


def _table(
    catalog: str, schema: str, name: str, table_type: TableType
) -> TableInfoWithGeneration:
    return TableInfoWithGeneration.from_dict(
        {
            "name": name,
            "catalog_name": catalog,
            "schema_name": schema,
            "table_type": table_type.value,
            "data_source_format": "DELTA",
            "columns": _COLUMNS,
            "full_name": f"{catalog}.{schema}.{name}",
            "table_id": f"id-{catalog}-{schema}-{name}",
        }
    )


def _workspace_client() -> mock.MagicMock:
    ws = mock.MagicMock()
    ws.metastores.summary.return_value = GetMetastoreSummaryResponse(
        name="my_metastore", metastore_id="m-1"
    )
    ws.catalogs.list.side_effect = lambda **kwargs: [
        CatalogInfo(name=name) for name in _SCHEMAS
    ]
    ws.catalogs.get.side_effect = lambda name, **kwargs: CatalogInfo(name=name)
    ws.schemas.list.side_effect = lambda catalog_name, **kwargs: [
        SchemaInfo(name=name, catalog_name=catalog_name)
        for name in _SCHEMAS.get(catalog_name, [])
    ]
    ws.tables.list.side_effect = lambda catalog_name, schema_name, **kwargs: [
        _table(catalog_name, schema_name, name, table_type)
        for name, table_type in _TABLES.get(schema_name, [])
        if catalog_name == "main"
    ]
    ws.workspace.list.side_effect = lambda *args, **kwargs: [
        ObjectInfo(
            object_type=ObjectType.NOTEBOOK, object_id=i, path=path, language=None
        )
        for i, path in enumerate(_NOTEBOOKS, start=1)
    ]
    for listing in (
        ws.service_principals.list,
        ws.groups.list,
        ws.registered_models.list,
        ws.model_versions.list,
    ):
        listing.return_value = []
    return ws


@pytest.fixture
def workspace_client(requests_mock: Any) -> Iterator[mock.MagicMock]:
    requests_mock.get(f"{_WORKSPACE_URL}/", json={"value": []})
    ws = _workspace_client()
    # The client both ingestion and the probe's for_config build.
    with mock.patch(
        "datahub.ingestion.source.unity.connection.WorkspaceClient", return_value=ws
    ):
        yield ws


def _datasets(index: EmittedIndex, sub_type: str) -> Set[str]:
    return {
        DatasetUrn.from_string(urn).name
        for urn, aspects in index.aspects.items()
        if Urn.from_string(urn).entity_type == "dataset"
        and any(
            isinstance(a, SubTypesClass) and sub_type in a.typeNames for a in aspects
        )
    }


def _notebook_paths(index: EmittedIndex) -> Set[str]:
    notebooks = {
        urn
        for urn, aspects in index.aspects.items()
        if any(
            isinstance(a, SubTypesClass) and DatasetSubTypes.NOTEBOOK in a.typeNames
            for a in aspects
        )
    }
    return {
        a.customProperties["path"]
        for urn in notebooks
        for a in index.aspects[urn]
        if isinstance(a, DatasetPropertiesClass)
    }


def _table_listing(label: str, command: str, sub_type: str) -> ParityListing:
    return ParityListing(
        label=label,
        command=command,
        emitted=lambda index: _datasets(index, sub_type),
        kwargs={"catalog": "main", "limit": _LIMIT},
        fan_out=FanOut(
            parent_command="schemas",
            param="schema",
            parent_kwargs={"catalog": "main", "limit": _LIMIT},
        ),
    )


@pytest.mark.parametrize("include_metric_views", [True, False])
def test_probe_verdicts_match_unity_ingestion(
    workspace_client: mock.MagicMock, tmp_path: Any, include_metric_views: bool
) -> None:
    recipe: Dict[str, object] = {
        "workspace_url": _WORKSPACE_URL,
        "token": "fake-token-value",
        "include_hive_metastore": False,
        "include_ownership": False,
        "include_table_lineage": False,
        "include_column_lineage": False,
        "include_usage_statistics": False,
        "include_tags": False,
        "include_table_constraints": False,
        "include_partition_keys": False,
        "include_metric_views": include_metric_views,
        "include_notebooks": True,
        "catalog_pattern": {"deny": ["^other$"]},
        "schema_pattern": {"deny": [r"^main\.staging$"]},
        "table_pattern": {"deny": [r"^main\.analytics\.tmp_.*"]},
        "view_pattern": {"deny": [r".*\.v_hidden$"]},
        "metric_view_pattern": {"deny": [r".*\.mv_hidden$"]},
        "notebook_pattern": {"allow": ["^/Shared/keep_.*"]},
    }
    listings = [
        ParityListing(
            label="catalogs",
            command="catalogs",
            emitted=lambda index: index.container_names(
                DatasetContainerSubTypes.CATALOG
            ),
            kwargs={"limit": _LIMIT},
        ),
        ParityListing(
            label="schemas",
            command="schemas",
            emitted=lambda index: index.container_names(
                DatasetContainerSubTypes.SCHEMA
            ),
            fan_out=FanOut(
                parent_command="catalogs",
                param="catalog",
                parent_kwargs={"limit": _LIMIT},
            ),
            kwargs={"limit": _LIMIT},
            # Container names are bare; the fixture's schema names are unique.
            identity=by_name,
        ),
        _table_listing("tables", "tables", DatasetSubTypes.TABLE),
        _table_listing("views", "views", DatasetSubTypes.VIEW),
        _table_listing("metric_views", "metric_views", DatasetSubTypes.METRIC_VIEW),
        ParityListing(
            label="notebooks",
            command="notebooks",
            emitted=_notebook_paths,
            kwargs={"limit": _LIMIT},
        ),
    ]

    report = assert_probe_parity(
        _SOURCE_TYPE, recipe, pipeline_ingestion(_SOURCE_TYPE, tmp_path), listings
    )

    assert report.excluded_by("catalogs") == {"other": "catalog_pattern"}
    assert report.excluded_by("tables")["main.analytics.tmp_load"] == "table_pattern"
    assert report.excluded_by("tables")["main.staging.t_stage"] == "schema_pattern"
    # process_tables reads table_pattern first, for views too.
    assert report.excluded_by("views")["main.analytics.tmp_view"] == "table_pattern"
    assert report.excluded_by("views")["main.analytics.v_hidden"] == "view_pattern"
    metric_views = report.excluded_by("metric_views")
    assert metric_views["main.analytics.tmp_kpi"] == "table_pattern"
    # metric_view_pattern is read only while include_metric_views is on.
    assert ("main.analytics.mv_hidden" in metric_views) is include_metric_views
    assert report.excluded_by("notebooks") == {
        "/Shared/drop_scratch": "notebook_pattern"
    }
