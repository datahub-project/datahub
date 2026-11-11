import json
from typing import Dict, Iterable, List, Optional, Tuple
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pytest
import requests_mock as requests_mock_module

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.app_state import AppCheckpoint
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    DataModelTable,
    LineageTable,
    ListedDataset,
    QlikDataset,
    UnchangedApp,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    MetadataChangeProposalClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

BASE_URL = "https://tenant.example.com/api/v1"
CLOUD_CONFIG = {"tenant_hostname": "tenant.example.com", "api_key": "key"}

ETL_APP = "qri:app:sense://etl-app"
REPORT_APP = "qri:app:sense://report-app"
SOURCE_QVD = "qri:qdf:space://AbC#SrcQvd"
STORED_QVD = "qri:qdf:space://AbC#StoredQvd"
HIDDEN_QVD = "qri:qdf:space://XyZ#HiddenQvd"


def _expand_url(qri: str) -> str:
    node = quote(qri, safe="")
    return (
        f"{BASE_URL}/lineage-graphs/nodes/{node}/actions/expand?node={node}&level=TABLE"
    )


def test_table_lineage_graph_is_read(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    requests_mock.get(
        _expand_url(REPORT_APP),
        json={
            "graph": {
                "nodes": {
                    f"{REPORT_APP}#Tbl": {"label": "Prices"},
                    # A neighbouring resource's table is not the app's own.
                    f"{STORED_QVD}#Curve": {"label": "Curve"},
                },
                "edges": [
                    {"source": f"{STORED_QVD}#Curve", "target": f"{REPORT_APP}#Tbl"},
                    {"source": f"{SOURCE_QVD}#Other", "target": "qri:not-a-node"},
                ],
            }
        },
    )
    requests_mock.get(_expand_url(ETL_APP), status_code=404)

    assert api.get_table_lineage(REPORT_APP) == [
        LineageTable(
            qri=f"{REPORT_APP}#Tbl", label="Prices", source_qris=[f"{STORED_QVD}#Curve"]
        )
    ]
    assert api.get_table_lineage(ETL_APP) is None


def _app(app_id: str, lineage_tables: List[LineageTable]) -> App:
    return App.model_validate(
        {
            "id": app_id,
            "qTitle": app_id,
            "createdDate": "2024-01-09T18:05:36.000Z",
            "modifiedDate": "2024-01-09T18:05:36.000Z",
            "spaceId": "space-1",
            "lineage_tables": lineage_tables,
        }
    )


def _dataset(name: str, secure_qri: str, stored_from: List[str]) -> QlikDataset:
    return QlikDataset.model_validate(
        {
            "id": name,
            "name": name,
            "itemId": name,
            "spaceId": "space-1",
            "secureQri": secure_qri,
            "type": "qvd",
            "createdTime": "2024-01-09T18:05:36.000Z",
            "lastModifiedTime": "2024-01-09T18:05:36.000Z",
            "operational": {"size": 1, "rowCount": 1},
            "schema": {"dataFields": []},
            "stored_from_qris": stored_from,
        }
    )


def _urn(name: str) -> str:
    return make_dataset_urn("qlik-sense", name.lower())


POSTGRES_ORDERS = make_dataset_urn("postgres", "db.public.orders")


def _lineage(
    workunits: Iterable[MetadataWorkUnit],
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """Upstreams written whole, and upstreams only added by a patch, by URN."""
    written: Dict[str, List[str]] = {}
    added: Dict[str, List[str]] = {}
    for wu in workunits:
        lineage = wu.get_aspect_of_type(UpstreamLineageClass)
        if lineage:
            written[wu.get_urn()] = [u.dataset for u in lineage.upstreams]
        elif (
            isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
            and wu.metadata.aspect is not None
        ):
            added.setdefault(wu.get_urn(), []).extend(
                op["value"]["dataset"]
                for op in json.loads(wu.metadata.aspect.value)
                if op["op"] == "add" and op["path"].startswith("/upstreams/")
            )
    return written, added


def _run(
    mock_api_cls: MagicMock,
    datasets: Optional[List[ListedDataset]],
    keep_unread_items: bool = False,
    graph: Optional[MagicMock] = None,
    script_read_failed: bool = False,
) -> Tuple[QlikSenseSource, Dict[str, List[str]], Dict[str, List[str]]]:
    space = MagicMock(id="space-1")
    space.name = "Finance"
    api = mock_api_cls.return_value
    api.get_spaces.return_value = [space]
    if not keep_unread_items:
        api.unread_items = []
    api.failed_user_lookups = {}
    api.change_check_failures = []
    api.lineage_read_failures = []
    api.get_user_name.return_value = None
    api.list_datasets.return_value = datasets
    api.get_items.return_value = [
        _app(
            "etl-app",
            [
                LineageTable(
                    qri=f"{ETL_APP}#Curve",
                    label="Curve",
                    source_qris=[f"{SOURCE_QVD}#Curve", f"{HIDDEN_QVD}#Curve"],
                )
            ],
        ),
        _app(
            "report-app",
            [
                LineageTable(
                    qri=f"{REPORT_APP}#Tbl",
                    label="Prices",
                    source_qris=[f"{STORED_QVD}#Curve"],
                )
            ],
        ),
        _dataset(
            "stored.qvd",
            STORED_QVD,
            [f"{ETL_APP}#Curve", "qri:app:sense://unread-app#Tbl"],
        ),
    ]
    for item in api.get_items.return_value:
        if isinstance(item, App):
            item.script_read_failed = script_read_failed
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )
    written, added = _lineage(source.get_workunits_internal())
    return source, written, added


DATASETS = [
    ListedDataset(space_id="space-1", name="src.qvd", secure_qri=SOURCE_QVD),
    ListedDataset(space_id="space-1", name="stored.qvd", secure_qri=STORED_QVD),
    # In a space the recipe leaves out, so it is never ingested.
    ListedDataset(space_id="space-2", name="hidden.qvd", secure_qri=HIDDEN_QVD),
]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_qvd_lineage_links_apps_through_datasets(mock_api_cls: MagicMock) -> None:
    source, written, added = _run(mock_api_cls, DATASETS)

    curve = _urn("etl-app.curve")
    assert written == {
        curve: [_urn("space-1.src.qvd")],
        _urn("report-app.prices"): [_urn("space-1.stored.qvd")],
    }
    # Without DataHub's current lineage, a dataset's storing tables can only be
    # added, as the dataset is not built by one app.
    assert added == {_urn("space-1.stored.qvd"): [curve]}
    assert not source.reporter.failures


ORACLE_PRICES = make_dataset_urn("oracle", "erp.prices")


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_lineage_replaces_only_app_table_edges(
    mock_api_cls: MagicMock,
) -> None:
    graph = MagicMock()
    # The app table that stored into the QVD before, and lineage added in
    # DataHub by hand.
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=_urn("old-app.curve"), type="COPY"),
            UpstreamClass(dataset=ORACLE_PRICES, type="TRANSFORMED"),
        ]
    )
    _, written, _ = _run(mock_api_cls, DATASETS, graph=graph)

    assert written[_urn("space-1.stored.qvd")] == [
        ORACLE_PRICES,
        _urn("etl-app.curve"),
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_no_app_stores_into_loses_its_store_edge(
    mock_api_cls: MagicMock,
) -> None:
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=_urn("etl-app.curve"), type="COPY"),
            UpstreamClass(dataset=ORACLE_PRICES, type="TRANSFORMED"),
        ]
    )
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )
    mock_api_cls.return_value.unread_items = []
    mock_api_cls.return_value.list_datasets.return_value = DATASETS
    # Qlik's lineage graph for the QVD is read and no app stores into it any more.
    source._dataset_store_sources = {_urn("space-1.stored.qvd"): []}

    written, added = _lineage(source._gen_dataset_store_lineage())

    assert written == {_urn("space-1.stored.qvd"): [ORACLE_PRICES]}
    assert not added


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_script_table_in_lineage_graph_keeps_its_urn(mock_api_cls: MagicMock) -> None:
    # A graph table that is also a load script table is ingested once, with
    # the upstreams of both, under the URN script tables already had.
    script_table = ScriptTableLineage(
        label="Curve", identifier="etl-app.curve", upstream_urns=[POSTGRES_ORDERS]
    )
    with patch.object(
        QlikSenseSource,
        "_build_script_tables",
        lambda self, app: [script_table] if app.id == "etl-app" else [],
    ):
        _, written, added = _run(mock_api_cls, DATASETS)

    curve = _urn("etl-app.curve")
    assert written[curve] == [_urn("space-1.src.qvd"), POSTGRES_ORDERS]
    assert added[_urn("space-1.stored.qvd")] == [curve]
    assert _urn(f"{ETL_APP}#Curve") not in written


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_table_urn_does_not_depend_on_script_sql(
    mock_api_cls: MagicMock,
) -> None:
    # Without a resolvable SQL load the table is not a script table, but it must
    # keep the same URN, or a failed connection would soft-delete it.
    _, written, _ = _run(mock_api_cls, DATASETS)
    assert _urn("etl-app.curve") in written


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_failed_dataset_listing_keeps_table_lineage(mock_api_cls: MagicMock) -> None:
    script_table = ScriptTableLineage(
        label="Curve", identifier="etl-app.curve", upstream_urns=[POSTGRES_ORDERS]
    )
    with patch.object(
        QlikSenseSource,
        "_build_script_tables",
        lambda self, app: [script_table] if app.id == "etl-app" else [],
    ):
        source, written, added = _run(mock_api_cls, None)

    # Both tables load datasets, so their lineage from the last run is kept
    # rather than replaced by lineage without the dataset edges, while the SQL
    # lineage read this run is still added.
    assert _urn("etl-app.curve") not in written
    assert _urn("report-app.prices") not in written
    assert added[_urn("etl-app.curve")] == [POSTGRES_ORDERS]
    assert "Lineage of Qlik app table only added to" in [
        w.title for w in source.reporter.warnings
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_denied_load_script_only_adds_to_table_lineage(
    mock_api_cls: MagicMock,
) -> None:
    # The SQL lineage a run that could read the script wrote stays in place.
    _, written, added = _run(mock_api_cls, DATASETS, script_read_failed=True)

    assert _urn("etl-app.curve") not in written
    assert added[_urn("etl-app.curve")] == [_urn("space-1.src.qvd")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_lineage_is_kept_when_a_storing_app_was_not_read(
    mock_api_cls: MagicMock,
) -> None:
    # A space or item could not be read, so `unread-app` may still store into
    # the dataset; rewriting the lineage without it would drop its edge.
    mock_api_cls.return_value.unread_items = [
        UnreadItem(name="items of space space-2", cause="timeout")
    ]
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=_urn("unread-app.tbl"), type="COPY")]
    )
    source, written, _ = _run(
        mock_api_cls, DATASETS, keep_unread_items=True, graph=graph
    )

    assert written[_urn("space-1.stored.qvd")] == [
        _urn("unread-app.tbl"),
        _urn("etl-app.curve"),
    ]
    assert "Lineage of Qlik dataset only added to" in [
        w.title for w in source.reporter.warnings
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_app_tables_resolve_from_its_checkpoint(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    mock_api_cls.return_value.unread_items = []
    source.app_state = MagicMock()
    source.stale_entity_removal_handler = MagicMock()
    source._fingerprint_salt = "salt"
    curve = _urn("etl-app.curve")
    source.app_state.previous.return_value = AppCheckpoint(
        urns=[curve], table_urns_by_qri={f"{ETL_APP}#Curve": curve}
    )
    source._keep_unchanged_app(UnchangedApp(id="etl-app", fingerprint="f"))
    source._dataset_store_sources[_urn("space-1.stored.qvd")] = [f"{ETL_APP}#Curve"]

    _, added = _lineage(source._gen_dataset_store_lineage())
    assert added == {_urn("space-1.stored.qvd"): [curve]}
    recorded = source.app_state.record.call_args.args[1]
    assert recorded.table_urns_by_qri == {f"{ETL_APP}#Curve": curve}


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_kept_from_older_state_leaves_dataset_lineage(
    mock_api_cls: MagicMock,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    mock_api_cls.return_value.unread_items = []
    source.app_state = MagicMock()
    source.stale_entity_removal_handler = MagicMock()
    source._fingerprint_salt = "salt"
    # Recorded before table URNs were kept, so the app's tables are unknown.
    source.app_state.previous.return_value = AppCheckpoint(urns=[_urn("etl-app.curve")])
    source._keep_unchanged_app(UnchangedApp(id="etl-app", fingerprint="f"))
    source._dataset_store_sources[_urn("space-1.stored.qvd")] = [f"{ETL_APP}#Curve"]

    assert not list(source._gen_dataset_store_lineage())


@pytest.mark.parametrize("had_tables, read_complete", [(True, False), (False, True)])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_missing_lineage_graph(
    mock_api_cls: MagicMock, had_tables: bool, read_complete: bool
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source.app_state = MagicMock()
    source.app_state.previous.return_value = AppCheckpoint(
        urns=["urn:li:container:x"],
        table_urns_by_qri={f"{ETL_APP}#Curve": _urn("etl-app.curve")}
        if had_tables
        else {},
    )
    app = _app("etl-app", [])
    app.lineage_graph_missing = True

    source._check_missing_lineage_graph(app)

    # A 404 for an app whose last read had tables is not trusted to mean the
    # tables are gone, as Qlik also answers 404 before it recomputes lineage.
    assert app.read_complete is read_complete
    assert app.tables_read_failed is not read_complete
    assert list(source.reporter.apps_without_lineage_graph) == (
        [] if had_tables else ["etl-app"]
    )


def test_datasets_are_listed(requests_mock: requests_mock_module.Mocker) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    first = f"{BASE_URL}/items?resourceType=dataset"
    second = f"{first}&next=abc"
    requests_mock.get(
        first,
        complete_qs=True,
        json={
            "data": [
                {
                    "name": "src.qvd",
                    "spaceId": "space-1",
                    "resourceAttributes": {"secureQri": SOURCE_QVD},
                },
                # Not a dataset Qlik traces, so it has no secure QRI.
                {"name": "notes.txt", "spaceId": "space-1", "resourceAttributes": {}},
            ],
            "links": {"next": {"href": second}},
        },
    )
    requests_mock.get(
        second,
        json={
            "data": [
                {"name": "mine.qvd", "resourceAttributes": {"secureQri": STORED_QVD}}
            ]
        },
    )

    assert api.list_datasets() == [
        ListedDataset(space_id="space-1", name="src.qvd", secure_qri=SOURCE_QVD),
        ListedDataset(
            space_id="personal-space-id", name="mine.qvd", secure_qri=STORED_QVD
        ),
    ]

    requests_mock.get(first, complete_qs=True, status_code=500)
    assert api.list_datasets() is None
    assert [item.name for item in api.unread_items] == ["datasets"]


@pytest.mark.parametrize(
    "name, dataset_type, display_name, subtype",
    [
        ("sales.qvd", "qvd", "sales", "QVD"),
        ("Sales.QVD", "qvd", "Sales", "QVD"),
        ("sales.csv", "DELIMETED", "sales.csv", "Qlik Dataset"),
    ],
)
def test_qvd_datasets_get_their_own_subtype_and_name(
    name: str, dataset_type: str, display_name: str, subtype: str
) -> None:
    dataset = _dataset(name, SOURCE_QVD, [])
    dataset.type = dataset_type
    with patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI"):
        source = QlikSenseSource(
            QlikSourceConfig.model_validate(CLOUD_CONFIG),
            PipelineContext(run_id="test-lineage-graph"),
        )
    workunits = list(source._gen_dataset_workunit(dataset))

    properties = next(
        props
        for wu in workunits
        if (props := wu.get_aspect_of_type(DatasetPropertiesClass))
    )
    subtypes = next(
        types for wu in workunits if (types := wu.get_aspect_of_type(SubTypesClass))
    )
    assert properties.name == display_name
    assert properties.qualifiedName == name
    assert subtypes.typeNames == [subtype]
    # The URN keeps the file name, so existing entities keep their URNs.
    assert {wu.get_urn() for wu in workunits} == {_urn(f"space-1.{name}")}


def test_data_model_field_types_come_from_qlik_tags() -> None:
    table = DataModelTable.from_engine(
        {
            "qName": "Curve",
            "qFields": [
                # A date is also numeric; the more specific tag wins.
                {"qName": "Day", "qTags": ["$numeric", "$integer", "$date"]},
                {"qName": "Price", "qTags": ["$numeric"]},
                {"qName": "Zone", "qTags": ["$ascii", "$text"]},
                {"qName": "Note", "qTags": []},
                {"qName": "$Syn 1", "qIsSynthetic": True},
            ],
        }
    )
    assert [(f.name, f.dataType) for f in table.fields] == [
        ("Day", "DATE"),
        ("Price", "DOUBLE"),
        ("Zone", "STRING"),
        ("Note", None),
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_tables_take_their_schema_from_the_data_model(
    mock_api_cls: MagicMock,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = []
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    app = _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])
    app.data_model_tables = [
        DataModelTable.from_engine(
            {
                "qName": "Curve",
                "qFields": [
                    {"qName": "Day", "qTags": ["$numeric", "$date"]},
                    {"qName": "Price", "qTags": ["$numeric"]},
                ],
            }
        )
    ]

    schemas = {
        wu.get_urn(): schema
        for wu in source._gen_app_workunit(app)
        if (schema := wu.get_aspect_of_type(SchemaMetadataClass))
    }

    # The lineage graph names the table but not its fields.
    assert [
        (field.fieldPath, field.nativeDataType)
        for field in schemas[_urn("etl-app.curve")].fields
    ] == [("Day", "DATE"), ("Price", "DOUBLE")]


def test_unnamed_script_table_takes_its_data_model_name() -> None:
    app = _app("etl-app", [])
    app.data_model_tables = [
        DataModelTable.from_engine(
            {"qName": name, "qFields": [{"qName": f} for f in fields]}
        )
        for name, fields in [
            ("Orders", ["id", "amount"]),
            ("Customers", ["id", "name"]),
            # Already claimed by a named script table.
            ("Prices", ["sku", "price"]),
        ]
    ]
    unnamed = ScriptTableLineage(
        label="table_1",
        identifier="etl-app.table_1",
        label_unknown=True,
        column_upstreams={"ID": [], "Amount": []},
    )
    # SELECT * against a table DataHub has no schema for: no columns to match.
    unknown_columns = ScriptTableLineage(
        label="table_2", identifier="etl-app.table_2", label_unknown=True
    )
    named = ScriptTableLineage(
        label="Prices",
        identifier="etl-app.prices",
        column_upstreams={"sku": [], "price": []},
    )

    QlikSenseSource._name_unknown_tables(app, [unnamed, unknown_columns, named])

    assert (unnamed.label, unnamed.identifier) == ("Orders", "etl-app.orders")
    assert (unknown_columns.label, unknown_columns.identifier) == (
        "table_2",
        "etl-app.table_2",
    )
