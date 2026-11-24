import base64
import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pytest
import requests_mock as requests_mock_module

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.app_state import AppCheckpoint
from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    DataModelTable,
    LineageTable,
    LineageUpstreams,
    ListedDataset,
    QlikDataset,
    QlikTable,
    ReloadStatement,
    SchemaField,
    UnchangedApp,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.file_lineage import qri_part_id
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import (
    UNREAD_DATA_CONNECTIONS,
    UNREAD_SPACES,
    ReadErrors,
)
from datahub.ingestion.source.qlik_sense.qlik_sense import (
    QlikSenseSource,
    _refs_of_field_sources,
)
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    MetadataChangeProposalClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import ColumnRef

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
    requests_mock.get(
        f"{BASE_URL}/lineage-graphs/nodes/{quote(REPORT_APP, safe='')}/actions/expand"
        f"?node={quote(f'{REPORT_APP}#Tbl', safe='')}&level=FIELD",
        json={
            "graph": {
                "nodes": {
                    f"{REPORT_APP}#Tbl#Fp": {"label": "Price"},
                    f"{REPORT_APP}#Tbl#Fd": {"label": "Day"},
                },
                "edges": [
                    {
                        "source": f"{STORED_QVD}#Curve#Fp",
                        "target": f"{REPORT_APP}#Tbl#Fp",
                    }
                ],
            }
        },
    )
    requests_mock.get(_expand_url(ETL_APP), status_code=404)

    assert api.get_table_lineage(REPORT_APP) == [
        LineageTable(
            qri=f"{REPORT_APP}#Tbl",
            label="Prices",
            source_qris=[f"{STORED_QVD}#Curve"],
            fields=["Price", "Day"],
            field_source_qris={"Price": [f"{STORED_QVD}#Curve#Fp"]},
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


def _patch_ops(
    workunits: Iterable[MetadataWorkUnit],
) -> Dict[str, List[Tuple[str, str]]]:
    """The (op, path) of every UpstreamLineage patch operation, by URN."""
    ops: Dict[str, List[Tuple[str, str]]] = {}
    for wu in workunits:
        assert not wu.get_aspect_of_type(UpstreamLineageClass)
        if (
            isinstance(wu.metadata, MetadataChangeProposalClass)
            and wu.metadata.aspectName == UpstreamLineageClass.ASPECT_NAME
            and wu.metadata.aspect is not None
        ):
            ops.setdefault(wu.get_urn(), []).extend(
                (op["op"], op["path"]) for op in json.loads(wu.metadata.aspect.value)
            )
    return ops


def _removed_upstreams(ops: List[Tuple[str, str]]) -> List[str]:
    return [
        path[len("/upstreams/") :].replace("~1", "/")
        for op, path in ops
        if op == "remove" and path.startswith("/upstreams/")
    ]


def _source(
    mock_api_cls: MagicMock,
    datasets: Optional[List[ListedDataset]],
    keep_unread_items: bool = False,
    graph: Optional[MagicMock] = None,
    script_read_failed: bool = False,
    items: Optional[List[Union[App, QlikDataset]]] = None,
    config: Optional[Dict[str, Any]] = None,
    upstreams: Optional[LineageUpstreams] = None,
    upstream_graph_error: Optional[Exception] = None,
) -> QlikSenseSource:
    space = MagicMock(id="space-1")
    space.name = "Finance"
    api = mock_api_cls.return_value
    api.get_spaces.return_value = [space]
    if not keep_unread_items:
        api.unread_items = []
    api.failed_user_lookups = {}
    api.change_check_failures = []
    api.lineage_read_failures = []
    api.field_lineage_failures = []
    api.opened_without_data = []
    api.include_read_failures = []
    api.reload_statement_read_failures = []
    api.get_lineage_upstreams.return_value = upstreams or LineageUpstreams()
    api.get_database_labels.return_value = {}
    api.get_lineage_upstreams.side_effect = upstream_graph_error
    api.engine_refusals = None
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
    if items is not None:
        api.get_items.return_value = items
    for item in api.get_items.return_value:
        if isinstance(item, App):
            item.script_read_failed = script_read_failed
    # Each app's last reload loads, by path, every dataset its graph names.
    loads = [
        ReloadStatement(discriminator=f"lib://hidden:datafiles/{label};")
        for label in (upstreams or LineageUpstreams()).resource_labels.values()
    ]
    api.get_reload_statements.side_effect = lambda app_id: list(loads)
    return QlikSenseSource(
        QlikSourceConfig.model_validate({**CLOUD_CONFIG, **(config or {})}),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )


def _run(
    mock_api_cls: MagicMock,
    datasets: Optional[List[ListedDataset]],
    keep_unread_items: bool = False,
    graph: Optional[MagicMock] = None,
    script_read_failed: bool = False,
    items: Optional[List[Union[App, QlikDataset]]] = None,
    upstreams: Optional[LineageUpstreams] = None,
    upstream_graph_error: Optional[Exception] = None,
) -> Tuple[QlikSenseSource, Dict[str, List[str]], Dict[str, List[str]]]:
    source = _source(
        mock_api_cls,
        datasets,
        keep_unread_items,
        graph,
        script_read_failed,
        items,
        upstreams=upstreams,
        upstream_graph_error=upstream_graph_error,
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


@pytest.mark.parametrize("loader_first", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_links_tables_of_another_app_it_loads(
    mock_api_cls: MagicMock, loader_first: bool
) -> None:
    etl = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[f"{SOURCE_QVD}#Curve"],
            )
        ],
    )
    # Such as a BINARY load, which reads the other app's tables directly.
    report = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Prices",
                source_qris=[
                    f"{ETL_APP}#Curve",
                    # An app the run does not ingest has no URN to link.
                    "qri:app:sense://other-app#Tbl",
                ],
            )
        ],
    )

    source, written, _ = _run(
        mock_api_cls,
        DATASETS,
        items=[report, etl] if loader_first else [etl, report],
    )

    assert written[_urn("report-app.prices")] == [_urn("etl-app.curve")]
    assert not source.reporter.warnings


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_is_only_added_to_when_the_app_it_loads_may_be_unread(
    mock_api_cls: MagicMock,
) -> None:
    # `other-app` may be in the space that could not be listed, so the last
    # run's edge to its table must stay.
    mock_api_cls.return_value.unread_items = [
        UnreadItem(name="items of space space-2", cause="timeout")
    ]
    report = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Prices",
                source_qris=["qri:app:sense://other-app#Tbl"],
            ),
            LineageTable(
                qri=f"{REPORT_APP}#Tbl2",
                label="Curves",
                source_qris=["qri:app:sense://other-app#Tbl"],
            ),
        ],
    )

    source, written, _ = _run(
        mock_api_cls, DATASETS, keep_unread_items=True, items=[report]
    )

    assert _urn("report-app.prices") not in written
    # One entry for the app, naming the source and every table it holds back.
    [context] = [
        list(w.context)
        for w in source.reporter.warnings
        if w.title == "Lineage of Qlik app tables only added to"
    ]
    assert context == [
        "app=report-app, reason=the tables of source app other-app are not known "
        "this run, tables=Curves, Prices"
    ]


@pytest.mark.parametrize("loader_first", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_gets_column_lineage_from_another_app_it_loads(
    mock_api_cls: MagicMock, loader_first: bool
) -> None:
    curve_qri = f"{ETL_APP}#Curve"
    etl = _app(
        "etl-app",
        [LineageTable(qri=curve_qri, label="Curve", fields=["Price", "Close"])],
    )
    report = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Prices",
                source_qris=[curve_qri],
                fields=["Price", "Px"],
                field_source_qris={
                    "Price": [_field_qri(curve_qri, "Price")],
                    # Renamed, so only named once the other app is read.
                    "Px": [_field_qri(curve_qri, "Close")],
                },
            )
        ],
    )
    source = _source(
        mock_api_cls, DATASETS, items=[report, etl] if loader_first else [etl, report]
    )

    [lineage] = [
        aspect
        for wu in source.get_workunits_internal()
        if wu.get_urn() == _urn("report-app.prices")
        and (aspect := wu.get_aspect_of_type(UpstreamLineageClass))
    ]
    prices, curve = _urn("report-app.prices"), _urn("etl-app.curve")
    assert {
        (cll.downstreams or [""])[0]: cll.upstreams
        for cll in lineage.fineGrainedLineages or []
    } == {
        make_schema_field_urn(prices, "Price"): [make_schema_field_urn(curve, "Price")],
        make_schema_field_urn(prices, "Px"): [make_schema_field_urn(curve, "Close")],
    }


@pytest.mark.parametrize("loader_first", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_is_only_added_to_when_the_app_it_loads_is_partly_read(
    mock_api_cls: MagicMock, loader_first: bool
) -> None:
    etl = _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])
    etl.tables_read_failed = True
    report = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Prices",
                source_qris=[f"{ETL_APP}#Curve"],
            )
        ],
    )

    source, written, added = _run(
        mock_api_cls,
        DATASETS,
        items=[report, etl] if loader_first else [etl, report],
    )

    # The last run's edges to the other app's tables this run could not read
    # must stay.
    prices = _urn("report-app.prices")
    assert prices not in written
    assert added[prices] == [_urn("etl-app.curve")]
    assert any(
        "app=report-app" in str(w.context)
        for w in source.reporter.warnings
        if w.title == "Lineage of Qlik app tables only added to"
    )


DATA_MODEL_APP = "qri:app:sense://dm-app"


@pytest.mark.parametrize(
    "orders_label, extra_nodes, unmatched",
    [
        # A neighbouring resource's table of the same label is not the app's.
        ("Orders", {f"{SOURCE_QVD}#Other": {"label": "Orders"}}, []),
        # Labels are read as the field lineage's table labels are.
        ('"Orders"', {}, []),
        ("Orders", {f"{DATA_MODEL_APP}#Cust": {"label": "Customers"}}, ["Customers"]),
        # Two nodes of one label can't be told apart.
        ("Orders", {f"{DATA_MODEL_APP}#Dup": {"label": "Orders"}}, ["Orders"]),
    ],
)
def test_data_model_tables_get_their_lineage_graph_node(
    requests_mock: requests_mock_module.Mocker,
    orders_label: str,
    extra_nodes: Dict[str, Dict[str, str]],
    unmatched: List[str],
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    requests_mock.get(
        _expand_url(DATA_MODEL_APP),
        json={
            "graph": {
                "nodes": {
                    f"{DATA_MODEL_APP}#Ord": {"label": orders_label},
                    **extra_nodes,
                }
            }
        },
    )
    requests_mock.get(
        re.compile(r".*level=FIELD$"),
        json={"graph": {"nodes": {f"{DATA_MODEL_APP}#Ord#F": {"label": "id"}}}},
    )
    requests_mock.post(
        re.compile(r".*/overview$"),
        json={
            "resources": [
                {
                    "lineage": [
                        {"tableLabel": "Orders", "tableQRI": f"{SOURCE_QVD}#Orders"}
                    ]
                }
            ]
        },
    )
    table = _data_model_table()
    errors = ReadErrors()

    api._add_qri_of_tables([table], "dm-app", errors)

    assert errors.messages == []
    assert table.tableQri == f"{SOURCE_QVD}#Orders"
    assert (table.graphQri is None) == (f"{DATA_MODEL_APP}#Dup" in extra_nodes)
    assert errors.unmatched_graph_tables == unmatched


def _data_model_table() -> QlikTable:
    return QlikTable.model_validate(
        {
            "tableName": "Orders",
            "boxType": "load-file",
            "tableAlias": "Orders",
            "dataconnectorName": "Files",
            "connectionInfo": {
                "id": "conn-1",
                "sourceConnectorID": "File_AmazonS3",
                "space": "space-1",
            },
            "fields": [{"name": "order_id"}],
        }
    )


def _data_model_app(
    unmatched_graph_tables: Optional[List[str]] = None,
    lineage_graph_missing: bool = False,
    table_qri: str = f"{SOURCE_QVD}#Orders",
) -> App:
    data_model = _app("dm-app", [])
    table = _data_model_table()
    table.tableQri = table_qri
    if not lineage_graph_missing:
        table.graphQri = f"{DATA_MODEL_APP}#Ord"
    data_model.tables = [table]
    data_model.unmatched_graph_tables = unmatched_graph_tables or []
    data_model.lineage_graph_missing = lineage_graph_missing
    return data_model


def _loader(source_qri: str) -> App:
    return _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl", label="Prices", source_qris=[source_qri]
            )
        ],
    )


def _table_urn(workunits: List[MetadataWorkUnit], name: str) -> str:
    [urn] = {
        wu.get_urn()
        for wu in workunits
        if (properties := wu.get_aspect_of_type(DatasetPropertiesClass))
        and properties.name == name
    }
    return urn


def _warning_contexts(source: QlikSenseSource, title: str) -> List[str]:
    return [str(w.context) for w in source.reporter.warnings if w.title == title]


@pytest.mark.parametrize("loader_first", [True, False])
@pytest.mark.parametrize(
    "unmatched, graph_missing, loads, written_whole",
    [
        ([], False, "Ord", True),
        # Another table of the app is unmatched; the one it loads resolves.
        (["Customers"], False, "Ord", True),
        (["Customers"], False, "Cust", False),
        ([], True, "Ord", False),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_links_data_model_tables_of_another_app_it_loads(
    mock_api_cls: MagicMock,
    loader_first: bool,
    unmatched: List[str],
    graph_missing: bool,
    loads: str,
    written_whole: bool,
) -> None:
    data_model = _data_model_app(unmatched, graph_missing)
    report = _loader(f"{DATA_MODEL_APP}#{loads}")
    source = _source(
        mock_api_cls,
        DATASETS,
        items=[report, data_model] if loader_first else [data_model, report],
    )

    workunits = list(source.get_workunits_internal())
    written, added = _lineage(workunits)

    prices = _urn("report-app.prices")
    if written_whole:
        assert written[prices] == [_table_urn(workunits, "Orders")]
    else:
        # The table it loads has no URN, so the last run's edges must stay.
        assert prices not in written
        assert prices not in added
        assert _warning_contexts(source, "Lineage of Qlik app tables only added to")
    unmatched_warnings = _warning_contexts(
        source, "Qlik app tables not matched to their lineage graph"
    )
    if unmatched:
        [context] = unmatched_warnings
        assert "dm-app" in context and "Customers" in context
    else:
        assert not unmatched_warnings


@pytest.mark.parametrize("loader_first", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_loaded_by_another_app_keeps_its_own_urn(
    mock_api_cls: MagicMock, loader_first: bool
) -> None:
    # The data model app BINARY-loads etl-app, so its table's tableQri is the
    # etl-app table's graph node, which a second loader also reads from.
    data_model = _data_model_app(table_qri=f"{ETL_APP}#Curve")
    etl = _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])

    _, written, _ = _run(
        mock_api_cls,
        DATASETS,
        items=[
            _loader(f"{ETL_APP}#Curve"),
            *([etl, data_model] if loader_first else [data_model, etl]),
        ],
    )

    assert written[_urn("report-app.prices")] == [_urn("etl-app.curve")]


@pytest.mark.parametrize(
    "stores_from, stale_removed",
    [("Ord", True), ("Cust", False)],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_lineage_of_an_app_with_unmatched_graph_tables(
    mock_api_cls: MagicMock, stores_from: str, stale_removed: bool
) -> None:
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            # A table of an app read this run that no longer stores here.
            UpstreamClass(dataset=_urn("report-app.prices"), type="COPY"),
        ]
    )
    stored = _urn("space-1.stored.qvd")
    source = _source(
        mock_api_cls,
        DATASETS,
        graph=graph,
        items=[
            _data_model_app(["Customers"]),
            _loader(f"{STORED_QVD}#Curve"),
            _dataset("stored.qvd", STORED_QVD, [f"{DATA_MODEL_APP}#{stores_from}"]),
        ],
    )

    workunits = list(source.get_workunits_internal())
    ops = _patch_ops(wu for wu in workunits if wu.get_urn() == stored)

    assert _lineage(workunits)[1].get(stored, []) == (
        [_table_urn(workunits, "Orders")] if stores_from == "Ord" else []
    )
    only_added = _warning_contexts(source, "Lineage of Qlik dataset only added to")
    if stale_removed:
        assert _removed_upstreams(ops[stored]) == [_urn("report-app.prices")]
        assert not only_added
    else:
        assert not _removed_upstreams(ops.get(stored, []))
        [context] = only_added
        assert stored in context


@pytest.mark.parametrize(
    "unmatched, graph_missing", [([], False), (["Customers"], False), ([], True)]
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_with_unresolved_graph_tables_is_recorded_so(
    mock_api_cls: MagicMock, unmatched: List[str], graph_missing: bool
) -> None:
    data_model = _data_model_app(unmatched, graph_missing)
    source = _source(mock_api_cls, DATASETS)

    with patch.object(source.app_state, "record") as record:
        list(source._gen_recorded_app_workunit(data_model))

    # Its tables were read, so a later run that keeps the app still owns their
    # edges, but only resolves the graph tables it recorded.
    checkpoint = record.call_args.args[1]
    assert not checkpoint.tables_unknown
    assert checkpoint.graph_tables_unresolved is bool(unmatched or graph_missing)
    assert checkpoint.table_urns_by_qri


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_read_app_records_its_field_names(mock_api_cls: MagicMock) -> None:
    etl = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve", label="Curve", fields=["Price", "Close"]
            )
        ],
    )
    source = _source(mock_api_cls, DATASETS)

    with patch.object(source.app_state, "record") as record:
        list(source._gen_recorded_app_workunit(etl))

    assert record.call_args.args[1].field_names == ["Price", "Close"]


@pytest.mark.parametrize("loader_first", [True, False])
@pytest.mark.parametrize("kept_by", ["unchanged", "partly_read"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_links_tables_of_a_kept_app_it_loads(
    mock_api_cls: MagicMock, loader_first: bool, kept_by: str
) -> None:
    curve = _urn("etl-app.curve")
    source = _source(mock_api_cls, DATASETS)
    app_state = MagicMock()
    source.app_state = app_state
    app_state.previous.side_effect = lambda app_id: (
        AppCheckpoint(urns=[curve], table_urns_by_qri={f"{ETL_APP}#Curve": curve})
        if app_id == "etl-app"
        else None
    )
    source.stale_entity_removal_handler = MagicMock()
    source._fingerprint_salt = "salt"

    def keep() -> None:
        if kept_by == "unchanged":
            source._keep_unchanged_app(UnchangedApp(id="etl-app", fingerprint="f"))
        else:
            etl = _app("etl-app", [])
            etl.read_errors = ["unable to read a sheet"]
            source._keep_partly_read_app(etl)
        assert app_state.record.call_args.args[0] == "etl-app"

    workunits: List[MetadataWorkUnit] = []
    if not loader_first:
        keep()
    workunits.extend(source._gen_app_workunit(_loader(f"{ETL_APP}#Curve")))
    if loader_first:
        keep()
    workunits.extend(source._gen_pending_table_lineage())

    assert _lineage(workunits)[0][_urn("report-app.prices")] == [curve]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_read_data_model_app_records_its_field_names(mock_api_cls: MagicMock) -> None:
    data_model = _app("dm-app", [])
    data_model.tables = [_data_model_table()]
    source = _source(mock_api_cls, DATASETS)

    with patch.object(source.app_state, "record") as record:
        list(source._gen_recorded_app_workunit(data_model))

    assert record.call_args.args[1].field_names == ["order_id"]


@pytest.mark.parametrize("loader_first", [True, False])
@pytest.mark.parametrize("kept_by", ["unchanged", "partly_read"])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_gets_renamed_column_lineage_from_a_kept_app_it_loads(
    mock_api_cls: MagicMock, loader_first: bool, kept_by: str
) -> None:
    curve_qri = f"{ETL_APP}#Curve"
    curve = _urn("etl-app.curve")
    source = _source(mock_api_cls, DATASETS)
    app_state = MagicMock()
    source.app_state = app_state
    app_state.previous.side_effect = lambda app_id: (
        AppCheckpoint(
            urns=[curve],
            table_urns_by_qri={curve_qri: curve},
            field_names=["Price", "Close"],
        )
        if app_id == "etl-app"
        else None
    )
    source.stale_entity_removal_handler = MagicMock()
    source._fingerprint_salt = "salt"
    report = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Prices",
                source_qris=[curve_qri],
                fields=["Px"],
                field_source_qris={"Px": [_field_qri(curve_qri, "Close")]},
            )
        ],
    )

    def keep() -> None:
        if kept_by == "unchanged":
            source._keep_unchanged_app(UnchangedApp(id="etl-app", fingerprint="f"))
        else:
            etl = _app("etl-app", [])
            etl.read_errors = ["unable to read a sheet"]
            source._keep_partly_read_app(etl)
        recorded = app_state.record.call_args.args[1]
        assert recorded.field_names == ["Price", "Close"]

    workunits: List[MetadataWorkUnit] = []
    if not loader_first:
        keep()
    workunits.extend(source._gen_app_workunit(report))
    if loader_first:
        keep()
    workunits.extend(source._gen_pending_table_lineage())

    prices = _urn("report-app.prices")
    assert _lineage(workunits)[0][prices] == [curve]
    [lineage] = [
        aspect
        for wu in workunits
        if wu.get_urn() == prices
        and (aspect := wu.get_aspect_of_type(UpstreamLineageClass))
    ]
    assert [
        (cll.downstreams, cll.upstreams) for cll in lineage.fineGrainedLineages or []
    ] == [
        (
            [make_schema_field_urn(prices, "Px")],
            [make_schema_field_urn(curve, "Close")],
        )
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_fingerprint_is_written_after_deferred_table_lineage(
    mock_api_cls: MagicMock,
) -> None:
    report = _loader(f"{ETL_APP}#Curve")
    report.fingerprint = "f"
    source = _source(mock_api_cls, DATASETS)
    source.app_state = MagicMock()
    source.app_state.previous.return_value = None
    source._fingerprint_salt = "salt"

    # etl-app is read after the loader, so the loader's lineage waits for it.
    workunits = [
        *source._gen_recorded_app_workunit(report),
        *source._gen_recorded_app_workunit(
            _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])
        ),
        *source._gen_pending_table_lineage(),
    ]

    ids = [wu.id for wu in workunits]
    [lineage_index] = [
        i
        for i, wu in enumerate(workunits)
        if wu.get_urn() == _urn("report-app.prices")
        and wu.get_aspect_of_type(UpstreamLineageClass)
    ]
    [fingerprint_index] = [
        i
        for i, wu_id in enumerate(ids)
        if wu_id.endswith("-fingerprint")
        and workunits[i].get_urn() == source._gen_app_key("report-app").as_urn()
    ]
    assert lineage_index < fingerprint_index


@pytest.mark.parametrize(
    "unlisted, written_whole",
    [
        # Qlik traces QVDs, so one without a QRI may be the source.
        pytest.param("other.qvd", False, id="QVD without a QRI"),
        pytest.param("other", False, id="QVD by type without a QRI"),
        # A file Qlik doesn't trace is never in the graph.
        pytest.param("notes.txt", True, id="untraced file"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_table_lineage_is_only_added_to_when_a_qvd_has_no_qri(
    mock_api_cls: MagicMock, unlisted: str, written_whole: bool
) -> None:
    # The source QVD isn't matched, and another dataset was listed without a
    # secure QRI.
    datasets = [
        ListedDataset(
            space_id="space-1",
            name=unlisted,
            type="QVD" if unlisted == "other" else "",
        ),
        *DATASETS[1:],
    ]
    _, written, _ = _run(mock_api_cls, datasets)

    # When it may be the source, the table's earlier edge is kept rather than
    # replaced away.
    assert (_urn("etl-app.curve") in written) is written_whole


UNLISTED_QVD = "qri:qdf:space://Unlisted#PricesQvd"
WRITER_TABLE = "qri:app:sense://writer-app#Out"
GRAPH_FILE = "qri:file:adls://Conn#Path"
DB_TABLE = "qri:db:mssql://Hash#Orders"


def _reader_and_writer(
    source_qris: Optional[List[str]] = None,
) -> List[Union[App, QlikDataset]]:
    reader = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Prices",
                source_qris=source_qris
                or [f"{UNLISTED_QVD}#Prices", f"{HIDDEN_QVD}#Curve"],
            )
        ],
    )
    # Read after the app that loads what it stores.
    return [reader, _app("writer-app", [LineageTable(qri=WRITER_TABLE, label="Out")])]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unlisted_dataset_is_named_from_the_upstream_graph(
    mock_api_cls: MagicMock,
) -> None:
    upstreams = LineageUpstreams(
        resource_labels={UNLISTED_QVD: "prices.qvd", HIDDEN_QVD: "hidden.qvd"},
        stored_from={
            f"{UNLISTED_QVD}#Prices": [
                WRITER_TABLE,
                "qri:app:sense://unread-app#Out",
            ]
        },
    )
    source = _source(
        mock_api_cls, DATASETS, items=_reader_and_writer(), upstreams=upstreams
    )
    workunits = list(source.get_workunits_internal())
    written, added = _lineage(workunits)

    # A QVD in a space the API key can't see joins the app storing it to the
    # app loading it; one listed in a space the recipe leaves out is not named.
    prices = _urn(UNLISTED_QVD)
    assert written[_urn("report-app.prices")] == [prices]
    assert added[prices] == [_urn("writer-app.out")]
    properties = next(
        p
        for wu in workunits
        if wu.get_urn() == prices
        and (p := wu.get_aspect_of_type(DatasetPropertiesClass))
    )
    assert properties.name == "prices"
    # The path comes from the app's reload, not from Qlik's label.
    assert properties.qualifiedName == "lib://hidden:datafiles/prices.qvd"
    subtypes = next(
        t
        for wu in workunits
        if wu.get_urn() == prices and (t := wu.get_aspect_of_type(SubTypesClass))
    )
    assert subtypes.typeNames == ["QVD"]
    assert not source.reporter.failures


@pytest.mark.parametrize(
    "excluded_name, named",
    [
        # The unlisted QVD may be the one the listing gave no QRI.
        pytest.param("other.qvd", False, id="QVD without a QRI"),
        # Qlik doesn't trace such files, so the graph never names one.
        pytest.param("notes.txt", True, id="untraced file without a QRI"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_of_an_excluded_space_without_a_qri_blocks_graph_naming(
    mock_api_cls: MagicMock, excluded_name: str, named: bool
) -> None:
    upstreams = LineageUpstreams(resource_labels={UNLISTED_QVD: "prices.qvd"})
    datasets = [*DATASETS, ListedDataset(space_id="space-2", name=excluded_name)]
    with patch.object(
        QlikSenseSource,
        "_keep_graph_named_urns",
        autospec=True,
        side_effect=QlikSenseSource._keep_graph_named_urns,
    ) as keep:
        _, written, _ = _run(
            mock_api_cls, datasets, items=_reader_and_writer(), upstreams=upstreams
        )

    if named:
        assert _urn(UNLISTED_QVD) in written[_urn("report-app.prices")]
        keep.assert_not_called()
    else:
        # Not knowing the source, the last run's edges and graph-named
        # datasets stay rather than being replaced away.
        assert _urn("report-app.prices") not in written
        assert [c.args[1] for c in keep.call_args_list] == ["report-app"]


@pytest.mark.parametrize(
    "source_qris, stored_qri",
    [
        pytest.param(
            [UNLISTED_QVD, HIDDEN_QVD], UNLISTED_QVD, id="named_at_resource_level"
        ),
        # The writer's stored table is named apart from the table read.
        pytest.param(
            [f"{UNLISTED_QVD}#Prices", HIDDEN_QVD],
            f"{UNLISTED_QVD}#PricesOut",
            id="stored_under_another_table_name",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_named_source_gets_every_writer_of_its_resource(
    mock_api_cls: MagicMock, source_qris: List[str], stored_qri: str
) -> None:
    upstreams = LineageUpstreams(
        resource_labels={UNLISTED_QVD: "prices.qvd", HIDDEN_QVD: "hidden.qvd"},
        stored_from={
            stored_qri: [WRITER_TABLE],
            f"{UNLISTED_QVD}2#Prices": [f"{WRITER_TABLE}2"],
        },
    )
    items = _reader_and_writer(source_qris=source_qris)
    writer = items[1]
    assert isinstance(writer, App)
    writer.lineage_tables.append(LineageTable(qri=f"{WRITER_TABLE}2", label="Out2"))
    _, written, added = _run(mock_api_cls, DATASETS, items=items, upstreams=upstreams)

    # The one listed in a space the recipe leaves out is still not named.
    assert written[_urn("report-app.prices")] == [_urn(UNLISTED_QVD)]
    assert added[_urn(UNLISTED_QVD)] == [_urn("writer-app.out")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_renamed_field_of_a_graph_named_source_still_replaces_lineage(
    mock_api_cls: MagicMock,
) -> None:
    items = _reader_and_writer()
    reader = items[0]
    assert isinstance(reader, App)
    # Loaded under a new name from a field the run never reads.
    reader.lineage_tables[0].field_source_qris = {
        "Px": [f"{UNLISTED_QVD}#Prices#unread-field-id"]
    }
    _, written, _ = _run(
        mock_api_cls,
        DATASETS,
        items=items,
        upstreams=LineageUpstreams(resource_labels={UNLISTED_QVD: "prices.qvd"}),
    )

    assert written[_urn("report-app.prices")] == [_urn(UNLISTED_QVD)]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_upstream_graph_only_adds_to_table_lineage(
    mock_api_cls: MagicMock,
) -> None:
    source, written, added = _run(
        mock_api_cls,
        DATASETS,
        items=_reader_and_writer(),
        upstream_graph_error=ValueError("no graph"),
    )

    assert _urn("report-app.prices") not in written
    assert _urn(UNLISTED_QVD) not in added
    assert "Unable to read Qlik upstream lineage graph" in [
        w.title for w in source.reporter.warnings
    ]
    assert not source.reporter.failures


@pytest.mark.parametrize(
    "source_qris",
    [
        # Qlik identifies database tables by an opaque hash.
        pytest.param([DB_TABLE], id="database node"),
        pytest.param([f"{HIDDEN_QVD}#Curve"], id="QVD in an excluded space"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_upstream_graph_leaves_sources_it_cannot_name_complete(
    mock_api_cls: MagicMock, source_qris: List[str]
) -> None:
    items = _reader_and_writer()
    reader = items[0]
    assert isinstance(reader, App)
    # Its other table loads an unlisted QVD, so the graph read is attempted.
    reader.lineage_tables.append(
        LineageTable(
            qri=f"{REPORT_APP}#Ord",
            label="Orders",
            source_qris=source_qris,
            # Labelled, as the reload SQL lineage reads them from the graph.
            database_source_labels={
                qri: "dbo.curve" for qri in source_qris if qri == DB_TABLE
            },
        )
    )
    _, written, _ = _run(
        mock_api_cls,
        DATASETS,
        items=items,
        upstream_graph_error=ValueError("no graph"),
    )

    assert _urn("report-app.prices") not in written
    assert written[_urn("report-app.orders")] == []


@pytest.mark.parametrize(
    "source_qris, reads",
    [
        pytest.param([f"{SOURCE_QVD}#Curve", DB_TABLE], 0, id="listed or opaque"),
        pytest.param([f"{HIDDEN_QVD}#Curve"], 0, id="listed in an excluded space"),
        pytest.param(
            [f"{UNLISTED_QVD}#Prices", f"{UNLISTED_QVD}2#Prices"], 1, id="unlisted"
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_upstream_graph_is_read_once_and_only_for_sources_it_may_name(
    mock_api_cls: MagicMock, source_qris: List[str], reads: int
) -> None:
    _run(mock_api_cls, DATASETS, items=_reader_and_writer(source_qris=source_qris))

    assert mock_api_cls.return_value.get_lineage_upstreams.call_count == reads


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_source_only_at_field_level_is_not_named_from_the_graph(
    mock_api_cls: MagicMock,
) -> None:
    items = _reader_and_writer(source_qris=[f"{SOURCE_QVD}#Curve"])
    reader = items[0]
    assert isinstance(reader, App)
    reader.lineage_tables[0].field_source_qris = {
        "Price": [_field_qri(f"{UNLISTED_QVD}#Prices", "Price")]
    }
    workunits = list(
        _source(
            mock_api_cls,
            DATASETS,
            items=items,
            upstreams=LineageUpstreams(
                resource_labels={UNLISTED_QVD: "prices.qvd"},
                stored_from={f"{UNLISTED_QVD}#Prices": [WRITER_TABLE]},
            ),
        ).get_workunits_internal()
    )

    # It would get its writers' edges but none to the table that reads it.
    assert not _workunits_of(workunits, _urn(UNLISTED_QVD))
    lineage = next(
        lineage
        for wu in _workunits_of(workunits, _urn("report-app.prices"))
        if (lineage := wu.get_aspect_of_type(UpstreamLineageClass))
    )
    assert [u.dataset for u in lineage.upstreams] == [_urn("space-1.src.qvd")]
    assert not lineage.fineGrainedLineages


def test_upstream_graph_is_read(requests_mock: requests_mock_module.Mocker) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    node = f"{BASE_URL}/lineage-graphs/nodes/{quote(REPORT_APP, safe='')}"
    requests_mock.get(
        f"{node}?level=RESOURCE&up=1",
        json={
            "graph": {
                "nodes": {
                    UNLISTED_QVD: {"label": "prices.qvd"},
                    REPORT_APP: {"label": "Report"},
                },
                "edges": [{"source": UNLISTED_QVD, "target": REPORT_APP}],
            }
        },
    )
    requests_mock.get(
        f"{node}?level=TABLE&up=2",
        json={
            "graph": {
                "nodes": {},
                "edges": [
                    {
                        "source": WRITER_TABLE,
                        "target": f"{UNLISTED_QVD}#Prices",
                        "relation": "store",
                    },
                    {
                        "source": f"{WRITER_TABLE}2",
                        "target": f"{UNLISTED_QVD}#Prices",
                        "relation": "STORE",
                    },
                    {"source": f"{WRITER_TABLE}3", "target": f"{UNLISTED_QVD}#Prices"},
                    {
                        "source": f"{UNLISTED_QVD}#Prices",
                        "target": f"{REPORT_APP}#Tbl",
                        "relation": "load",
                    },
                ],
            }
        },
    )

    assert api.get_lineage_upstreams(REPORT_APP) == LineageUpstreams(
        resource_labels={UNLISTED_QVD: "prices.qvd", REPORT_APP: "Report"},
        stored_from={f"{UNLISTED_QVD}#Prices": [WRITER_TABLE, f"{WRITER_TABLE}2"]},
        database_labels={},
    )


def _file_reader(script: str, file_qri: str, variables_read_failed: bool) -> App:
    reader = _app(
        "report-app",
        [LineageTable(qri=f"{REPORT_APP}#Tbl", label="Prices", source_qris=[file_qri])],
    )
    reader.script = script
    reader.variables_read_failed = variables_read_failed
    return reader


def _file_upstreams(file_qri: str) -> LineageUpstreams:
    return LineageUpstreams(resource_labels={file_qri.rpartition("#")[0]: "prices.csv"})


def _workunits_of(
    workunits: List[MetadataWorkUnit], urn: str
) -> List[MetadataWorkUnit]:
    return [wu for wu in workunits if wu.get_urn() == urn]


@pytest.mark.parametrize(
    "listed, connections_unread, graph_named",
    [
        pytest.param(True, False, True, id="listed"),
        # The file may be a listed dataset with its own URN.
        pytest.param(False, False, False, id="not listed"),
        # The file may be on a connection whose storage the list would give.
        pytest.param(True, True, False, id="connections unread"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_file_the_script_may_load_is_named_from_the_graph_only_when_listed(
    mock_api_cls: MagicMock, listed: bool, connections_unread: bool, graph_named: bool
) -> None:
    api = mock_api_cls.return_value
    api.get_data_connections.return_value = {}
    api.unread_items = (
        [UnreadItem(name=UNREAD_DATA_CONNECTIONS, cause="timed out")]
        if connections_unread
        else []
    )
    file_qri = f"{GRAPH_FILE}#Tbl"
    # The path is in a variable the run could not read, so the script may load
    # the file.
    reader = _file_reader(
        "Prices: LOAD * FROM [$(vPath)] (txt);",
        file_qri,
        variables_read_failed=not connections_unread,
    )
    source = _source(
        mock_api_cls,
        DATASETS if listed else None,
        keep_unread_items=True,
        items=[reader],
        upstreams=_file_upstreams(file_qri),
    )
    written, added = _lineage(source.get_workunits_internal())

    table = _urn("report-app.prices")
    assert table not in written
    assert (_urn(GRAPH_FILE) in added.get(table, [])) is graph_named


@pytest.mark.parametrize(
    "file_connection_to_storage, script, graph_named",
    [
        pytest.param(
            {
                "Prod": {"storage_uri": "abfss://lake@account.dfs.core.windows.net/p"},
                "Dev": {"storage_uri": "abfss://lake@account.dfs.core.windows.net/d"},
            },
            "IF vEnv = 'prod' THEN\nPrices: LOAD * FROM [lib://Prod/x.csv] (txt);\n"
            "ELSE\nPrices: LOAD * FROM [lib://Dev/x.csv] (txt);\nEND IF;\n",
            False,
            id="on mapped storage, either branch",
        ),
        pytest.param(
            {},
            "Prices: LOAD * FROM [lib://Lake/x.csv] (txt);\n",
            True,
            # In a shared space, the app's own space qualifies the name.
            id="on a connection named without its space in a personal space",
        ),
    ],
)
@pytest.mark.parametrize("variables_unread", [False, True])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unresolved_file_is_named_from_the_graph_only_off_known_storage(
    mock_api_cls: MagicMock,
    variables_unread: bool,
    file_connection_to_storage: Dict[str, Any],
    script: str,
    graph_named: bool,
) -> None:
    mock_api_cls.return_value.get_data_connections.return_value = {}
    mock_api_cls.return_value.spaces = {"space-1": "Finance"}
    file_qri = (
        f"qri:file:adls://{qri_part_id('lake-other')}#{qri_part_id('x.csv')}"
        f"#{qri_part_id('item')}"
    )
    reader = _file_reader(script, file_qri, variables_read_failed=variables_unread)
    if not file_connection_to_storage:
        reader.spaceId = Constant.PERSONAL_SPACE_ID
    source = _source(
        mock_api_cls,
        DATASETS,
        items=[reader],
        config={"file_connection_to_storage": file_connection_to_storage},
        upstreams=_file_upstreams(file_qri),
    )
    workunits = list(source.get_workunits_internal())
    written, added = _lineage(workunits)

    # A file on known storage has its storage URN, so it is left out rather
    # than named apart from it.
    file_urn = _urn(file_qri.rpartition("#")[0])
    table = _urn("report-app.prices")
    assert [*written.get(table, []), *added.get(table, [])] == (
        [file_urn] if graph_named else []
    )
    subtypes = [
        t
        for wu in _workunits_of(workunits, file_urn)
        if (t := wu.get_aspect_of_type(SubTypesClass))
    ]
    assert [t.typeNames for t in subtypes] == (
        [[DatasetSubTypes.QLIK_DATASET]] if graph_named else []
    )


ORACLE_PRICES = make_dataset_urn("oracle", "erp.prices")


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_lineage_removes_only_edges_of_known_app_tables(
    mock_api_cls: MagicMock,
) -> None:
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            # A table of an app read this run that no longer stores here.
            UpstreamClass(dataset=_urn("report-app.prices"), type="COPY"),
            # A table this run knows nothing about, such as one in a space the
            # recipe leaves out, and lineage added in DataHub by hand.
            UpstreamClass(dataset=_urn("other-app.curve"), type="COPY"),
            UpstreamClass(dataset=ORACLE_PRICES, type="TRANSFORMED"),
        ]
    )
    _, written, added = _run(mock_api_cls, DATASETS, graph=graph)
    stored = _urn("space-1.stored.qvd")

    assert stored not in written
    assert added[stored] == [_urn("etl-app.curve")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_lineage_patch_removes_stale_edges(
    mock_api_cls: MagicMock,
) -> None:
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=_urn("etl-app.curve"), type="COPY"),
            UpstreamClass(dataset=_urn("other-app.curve"), type="COPY"),
            UpstreamClass(dataset=ORACLE_PRICES, type="TRANSFORMED"),
        ]
    )
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )
    mock_api_cls.return_value.unread_items = []
    mock_api_cls.return_value.list_datasets.return_value = DATASETS
    source._register_app_tables(
        _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])
    )
    # Qlik's lineage graph for the QVD is read and no app stores into it any more.
    source._dataset_store_sources = {_urn("space-1.stored.qvd"): []}

    ops = _patch_ops(source._gen_dataset_store_lineage())

    assert _removed_upstreams(ops[_urn("space-1.stored.qvd")]) == [
        _urn("etl-app.curve")
    ]


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
        lambda self, app, parsed=None: [script_table] if app.id == "etl-app" else [],
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
        lambda self, app, parsed=None: [script_table] if app.id == "etl-app" else [],
    ):
        source, written, added = _run(mock_api_cls, None)

    # Both tables load datasets, so their lineage from the last run is kept
    # rather than replaced by lineage without the dataset edges, while the SQL
    # lineage read this run is still added.
    assert _urn("etl-app.curve") not in written
    assert _urn("report-app.prices") not in written
    assert added[_urn("etl-app.curve")] == [POSTGRES_ORDERS]
    assert "Lineage of Qlik app tables only added to" in [
        w.title for w in source.reporter.warnings
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_denied_load_script_only_adds_to_table_lineage(
    mock_api_cls: MagicMock,
) -> None:
    # The SQL lineage a run that could read the script wrote stays in place.
    source, written, added = _run(mock_api_cls, DATASETS, script_read_failed=True)

    assert _urn("etl-app.curve") not in written
    assert added[_urn("etl-app.curve")] == [_urn("space-1.src.qvd")]
    assert any(
        "load script could not be read" in str(w.context)
        for w in source.reporter.warnings
        if w.title == "Lineage of Qlik app tables only added to"
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_lineage_adds_tables_of_an_app_whose_tables_are_unknown(
    mock_api_cls: MagicMock,
) -> None:
    other = _urn("other-app.curve")
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=other, type="COPY")]
    )
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )
    mock_api_cls.return_value.unread_items = []
    mock_api_cls.return_value.list_datasets.return_value = DATASETS
    # The load script read timed out, but the graph still names the table.
    app = _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])
    app.tables_read_failed = True
    source._register_app_tables(app)
    source._dataset_store_sources = {_urn("space-1.stored.qvd"): [f"{ETL_APP}#Curve"]}

    ops = _patch_ops(source._gen_dataset_store_lineage())[_urn("space-1.stored.qvd")]

    assert ("add", f"/upstreams/{_urn('etl-app.curve').replace('/', '~1')}") in ops
    assert not _removed_upstreams(ops)


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_lineage_removes_edges_of_dropped_app_tables(
    mock_api_cls: MagicMock,
) -> None:
    dropped = _urn("etl-app.old_curve")
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=dropped, type="COPY")]
    )
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )
    mock_api_cls.return_value.unread_items = []
    mock_api_cls.return_value.list_datasets.return_value = DATASETS
    # The last run recorded a table the app has since dropped or renamed.
    with patch.object(
        source,
        "_usable_checkpoint",
        return_value=AppCheckpoint(
            urns=[dropped], table_urns_by_qri={f"{ETL_APP}#OldCurve": dropped}
        ),
    ):
        source._register_app_tables(
            _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Curve", label="Curve")])
        )
    source._dataset_store_sources = {_urn("space-1.stored.qvd"): []}

    ops = _patch_ops(source._gen_dataset_store_lineage())

    assert _removed_upstreams(ops[_urn("space-1.stored.qvd")]) == [dropped]


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
    source, written, added = _run(
        mock_api_cls, DATASETS, keep_unread_items=True, graph=graph
    )

    assert _urn("space-1.stored.qvd") not in written
    assert added[_urn("space-1.stored.qvd")] == [_urn("etl-app.curve")]
    graph.get_aspect.assert_not_called()
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


@pytest.mark.parametrize(
    "had_tables, graph_tables_unresolved, checkpoint_failed, read_complete",
    [
        (True, False, False, False),
        (False, False, False, True),
        (False, False, True, False),
        # Its last read saw graph tables it couldn't match, so it had tables.
        (False, True, False, False),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_missing_lineage_graph(
    mock_api_cls: MagicMock,
    had_tables: bool,
    graph_tables_unresolved: bool,
    checkpoint_failed: bool,
    read_complete: bool,
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
        graph_tables_unresolved=graph_tables_unresolved,
    )
    if checkpoint_failed:
        # Without the last read, the app's tables may still exist.
        source.app_state.previous.side_effect = RuntimeError("state unavailable")
    app = _app("etl-app", [])
    app.lineage_graph_missing = True

    source._check_missing_lineage_graph(app)

    # A 404 for an app whose last read had tables is not trusted to mean the
    # tables are gone, as Qlik also answers 404 before it recomputes lineage.
    assert app.read_complete is read_complete
    assert app.tables_read_failed is not read_complete
    # Not read completely means a read error, so the last read is kept.
    assert bool(app.read_errors) is not read_complete
    assert list(source.reporter.apps_without_lineage_graph) == (
        ["etl-app"] if read_complete else []
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_missing_lineage_graph_after_a_read_with_unknown_tables_keeps_it(
    mock_api_cls: MagicMock,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source.app_state = MagicMock()
    source.app_state.previous.return_value = AppCheckpoint(
        urns=["urn:li:container:x"],
        table_urns_by_qri={f"{ETL_APP}#Curve": _urn("etl-app.curve")},
        tables_unknown=True,
    )
    app = _app("etl-app", [])
    app.lineage_graph_missing = True

    source._check_missing_lineage_graph(app)

    # Its last read had tables, so the app keeps it instead of being ingested
    # without them.
    assert not app.read_complete
    assert app.tables_read_failed


@pytest.mark.parametrize("tables_unknown", [False, True])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_app_carries_whether_its_tables_are_unknown(
    mock_api_cls: MagicMock, tables_unknown: bool
) -> None:
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
        urns=[curve],
        table_urns_by_qri={f"{ETL_APP}#Curve": curve},
        tables_unknown=tables_unknown,
    )

    source._keep_unchanged_app(UnchangedApp(id="etl-app", fingerprint="f"))

    assert source._app_tables_known("etl-app") is not tables_unknown
    assert source.app_state.record.call_args.args[1].tables_unknown is tables_unknown


@pytest.mark.parametrize("graph_tables_unresolved", [False, True])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_kept_app_carries_whether_its_graph_tables_are_unresolved(
    mock_api_cls: MagicMock, graph_tables_unresolved: bool
) -> None:
    source = _source(mock_api_cls, DATASETS)
    source.app_state = MagicMock()
    source.stale_entity_removal_handler = MagicMock()
    source._fingerprint_salt = "salt"
    orders = _urn("dm-app.orders")
    source.app_state.previous.return_value = AppCheckpoint(
        urns=[orders],
        table_urns_by_qri={f"{DATA_MODEL_APP}#Ord": orders},
        graph_tables_unresolved=graph_tables_unresolved,
    )

    source._keep_unchanged_app(UnchangedApp(id="dm-app", fingerprint="f"))

    assert source._resolve_source(f"{DATA_MODEL_APP}#Ord", "x", None) == (orders, None)
    # A graph table its last read had no URN for may still be one of its tables.
    urn, unknown = source._resolve_source(f"{DATA_MODEL_APP}#Cust", "x", None)
    assert urn is None
    assert (unknown is not None) is graph_tables_unresolved
    recorded = source.app_state.record.call_args.args[1]
    assert recorded.graph_tables_unresolved is graph_tables_unresolved


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_missing_lineage_graph_without_a_last_read_only_adds_lineage(
    mock_api_cls: MagicMock,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source.app_state = MagicMock()
    source.app_state.previous.return_value = None
    app = _app("etl-app", [])
    app.lineage_graph_missing = True

    source._check_missing_lineage_graph(app)

    # Nothing says whether the app has tables, but the app itself was read.
    assert app.read_complete
    assert app.tables_read_failed
    assert len(source.reporter.warnings) == 1


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
                {
                    "name": "notes.txt",
                    "spaceId": "space-1",
                    "resourceAttributes": {"type": "TEXT"},
                },
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
        ListedDataset(space_id="space-1", name="notes.txt", type="TEXT"),
        ListedDataset(
            space_id="personal-space-id", name="mine.qvd", secure_qri=STORED_QVD
        ),
    ]

    requests_mock.get(first, complete_qs=True, status_code=500)
    assert api.list_datasets() is None
    # Only lineage needs the list, so the run's entities still count as read.
    assert not api.unread_items
    assert api.dataset_listing_error


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
    # The lineage graph also traces Orders; the unnamed load must take its name
    # so both are ingested as one table.
    app = _app("etl-app", [LineageTable(qri=f"{ETL_APP}#Orders", label="Orders")])
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


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_column_lineage_uses_the_data_model_spelling(mock_api_cls: MagicMock) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._data_model_fields = {
        "etl-app.orders": [
            SchemaField(name="OrderId"),
            SchemaField(name="Code"),
            SchemaField(name="CODE"),
        ]
    }
    table = ScriptTableLineage(
        label="Orders",
        identifier="etl-app.orders",
        column_upstreams={"orderid": ["u1"], "code": ["u2"], "other": ["u3"]},
    )

    source._use_data_model_field_names([table])

    # "code" matches two fields that differ only by case, so it is left as is.
    assert table.column_upstreams == {
        "OrderId": ["u1"],
        "code": ["u2"],
        "other": ["u3"],
    }


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_files_qlik_catalogued_on_a_connection_are_sources(
    mock_api_cls: MagicMock,
) -> None:
    adls_file = "qri:file:adls://Conn#File"
    mock_api_cls.return_value.list_datasets.return_value = [
        ListedDataset(space_id="space-1", name="prices.qvd", secure_qri=adls_file)
    ]
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}

    resolved = source._resolve_lineage_sources(
        [f"{adls_file}#Tbl", "qri:db:databricks://Opaque#Table#Tbl"],
        _urn("etl-app.curve"),
        own_app_id="etl-app",
    )

    # A database table Qlik did not catalogue is left to the load script parser.
    assert resolved.urns == [_urn("space-1.prices.qvd")]
    assert resolved.complete


def _field_qri(table_qri: str, name: str) -> str:
    digest = hashlib.sha256(name.encode()).digest()
    return f"{table_qri}#{base64.urlsafe_b64encode(digest).decode().rstrip('=')}"


@pytest.mark.parametrize("include_column_lineage", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_field_level_gives_column_lineage_and_fields(
    mock_api_cls: MagicMock, include_column_lineage: bool
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {**CLOUD_CONFIG, "include_column_lineage": include_column_lineage}
        ),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}
    source._learn_field_names(["amount"])
    qvd_table = f"{SOURCE_QVD}#Curve"
    # The script drops the table after storing it, so the data model has no
    # fields for it, and the graph is where its fields come from.
    app = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[qvd_table],
                fields=["Day", "Total", "Note"],
                field_source_qris={
                    "Day": [_field_qri(qvd_table, "Day")],
                    # Renamed from a field name the run knows.
                    "Total": [_field_qri(qvd_table, "amount")],
                },
            )
        ],
    )

    workunits = list(source._gen_app_workunit(app))

    curve = _urn("etl-app.curve")
    [schema] = [
        aspect
        for wu in workunits
        if wu.get_urn() == curve
        and (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]
    assert [field.fieldPath for field in schema.fields] == ["Day", "Total", "Note"]
    [lineage] = [
        upstreams
        for wu in workunits
        if (upstreams := wu.get_aspect_of_type(UpstreamLineageClass))
    ]
    src = _urn("space-1.src.qvd")
    assert [u.dataset for u in lineage.upstreams] == [src]
    assert {
        (cll.downstreams or [""])[0]: cll.upstreams
        for cll in lineage.fineGrainedLineages or []
    } == (
        {
            make_schema_field_urn(curve, "Day"): [make_schema_field_urn(src, "Day")],
            make_schema_field_urn(curve, "Total"): [
                make_schema_field_urn(src, "amount")
            ],
        }
        if include_column_lineage
        else {}
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_script_app_tables_resolve_as_sources(mock_api_cls: MagicMock) -> None:
    mock_api_cls.return_value.unread_items = []
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    script_app = "qri:app:dataprep://script-app"
    app = _app("script-app", [LineageTable(qri=f"{script_app}#Out", label="Out")])
    source._register_app_tables(app)

    resolved = source._resolve_lineage_sources(
        [f"{script_app}#Out"], _urn("space-1.out.qvd"), own_app_id=None
    )

    assert resolved.urns == [_urn("script-app.out")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_dataset_store_column_lineage_removes_only_stale_app_table_edges(
    mock_api_cls: MagicMock,
) -> None:
    stored, curve = _urn("space-1.stored.qvd"), _urn("etl-app.curve")
    manual = FineGrainedLineageClass(
        upstreamType="FIELD_SET",
        upstreams=[make_schema_field_urn(ORACLE_PRICES, "price")],
        downstreamType="FIELD",
        downstreams=[make_schema_field_urn(stored, "Price")],
    )
    stale = FineGrainedLineageClass(
        upstreamType="FIELD_SET",
        upstreams=[make_schema_field_urn(curve, "OldPrice")],
        downstreamType="FIELD",
        downstreams=[make_schema_field_urn(stored, "Price")],
    )
    graph = MagicMock()
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=curve, type="COPY"),
            UpstreamClass(dataset=ORACLE_PRICES, type="TRANSFORMED"),
        ],
        # A column edge written by another tool, with an upstream that is
        # not a schema field.
        fineGrainedLineages=[
            manual,
            stale,
            FineGrainedLineageClass(
                upstreamType="DATASET",
                upstreams=[ORACLE_PRICES],
                downstreamType="FIELD",
                downstreams=[make_schema_field_urn(stored, "Price")],
            ),
            # One with several downstream fields, from an owned table.
            FineGrainedLineageClass(
                upstreamType="FIELD_SET",
                upstreams=[make_schema_field_urn(curve, "Gone")],
                downstreamType="FIELD_SET",
                downstreams=[
                    make_schema_field_urn(stored, "A"),
                    make_schema_field_urn(stored, "B"),
                ],
            ),
        ],
    )
    mock_api_cls.return_value.unread_items = []
    mock_api_cls.return_value.list_datasets.return_value = DATASETS
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph", graph=graph),
    )
    source._allowed_space_ids = {"space-1"}
    curve_table = f"{ETL_APP}#Curve"
    source._register_app_tables(
        _app("etl-app", [LineageTable(qri=curve_table, label="Curve")])
    )
    source._dataset_store_sources = {stored: [curve_table]}
    source._dataset_store_field_sources = {
        stored: {"Price": [_field_qri(curve_table, "Price")]}
    }

    ops = _patch_ops(source._gen_dataset_store_lineage())[stored]
    fine_grained = [
        (op, path) for op, path in ops if path.startswith("/fineGrainedLineages/")
    ]

    # The stale field edge from the app table is removed and the stored one
    # added; the edge added by hand and the other tool's edge are untouched.
    assert [op for op, _ in fine_grained] == ["add", "remove"]
    assert fine_grained[1][1].endswith(
        make_schema_field_urn(curve, "OldPrice").replace("/", "~1")
    )


def test_dataset_schema_leaves_out_orphaned_fields() -> None:
    dataset = QlikDataset.model_validate(
        {
            "id": "ds-1",
            "name": "orders.qvd",
            "itemId": "item-1",
            "spaceId": "space-1",
            "secureQri": SOURCE_QVD,
            "type": "qvd",
            "createdTime": "2024-01-09T18:05:36.000Z",
            "lastModifiedTime": "2024-01-09T18:05:36.000Z",
            "operational": {"size": 1, "rowCount": 1},
            "schema": {
                "dataFields": [
                    {"name": "PROD", "dataType": {"type": "STRING"}, "orphan": False},
                    # A field an earlier version of the file had.
                    {"name": "OLD", "dataType": {"type": "STRING"}, "orphan": True},
                ]
            },
        }
    )
    assert [field.name for field in dataset.datasetSchema] == ["PROD"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_untyped_graph_table_takes_types_from_the_dataset_it_stores(
    mock_api_cls: MagicMock,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    curve, curve_table = _urn("etl-app.curve"), f"{ETL_APP}#Curve"
    source._app_table_urns_by_qri = {curve_table: curve}
    source._untyped_tables = {
        curve: (
            "etl-app.curve",
            [
                SchemaField(name="Day"),
                SchemaField(name="Year"),
                SchemaField(name="Id"),
                # A load script column whose case the SQL parser changed.
                SchemaField(name="amount"),
            ],
        )
    }
    stored, other = _urn("space-1.stored.qvd"), _urn("space-1.other.qvd")
    source._dataset_field_types = {
        stored: {
            "Day": "TIMESTAMP",
            "Total": "DOUBLE",
            "Id": "INTEGER",
            "Amount": "DOUBLE",
        },
        other: {"Id": "STRING"},
    }
    source._dataset_store_field_sources = {
        stored: {
            "Day": [_field_qri(curve_table, "Day")],
            # Stored under a new name, so the type may not be the field's.
            "Total": [_field_qri(curve_table, "Year")],
            "Id": [_field_qri(curve_table, "Id")],
            "Amount": [_field_qri(curve_table, "Amount")],
        },
        # Another dataset disagrees on Id's type.
        other: {"Id": [_field_qri(curve_table, "Id")]},
    }

    [schema] = [
        aspect
        for wu in source._gen_stored_table_types()
        if (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]

    assert [(f.fieldPath, f.nativeDataType) for f in schema.fields] == [
        ("Day", "TIMESTAMP"),
        ("Year", ""),
        ("Id", ""),
        ("amount", "DOUBLE"),
    ]


def test_failed_field_lineage_keeps_the_table_graph(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    requests_mock.get(
        _expand_url(REPORT_APP),
        json={
            "graph": {
                "nodes": {f"{REPORT_APP}#Tbl": {"label": "Prices"}},
                "edges": [
                    {"source": f"{STORED_QVD}#Curve", "target": f"{REPORT_APP}#Tbl"}
                ],
            }
        },
    )
    requests_mock.get(
        f"{BASE_URL}/lineage-graphs/nodes/{quote(REPORT_APP, safe='')}/actions/expand"
        f"?node={quote(f'{REPORT_APP}#Tbl', safe='')}&level=FIELD",
        status_code=500,
    )

    [table] = api.get_table_lineage(REPORT_APP) or []

    assert table.source_qris == [f"{STORED_QVD}#Curve"]
    assert table.field_lineage_failed
    assert api.field_lineage_failures


@pytest.mark.parametrize(
    "graph_response, stored_from_qris, read_failed",
    [
        # Qlik has no graph for the dataset: whether apps store into it is unknown.
        ({"status_code": 404}, None, False),
        ({"status_code": 500}, None, True),
        # A graph with no storing tables: no app stores into it any more.
        ({"json": {"graph": {"nodes": {}, "edges": []}}}, [], False),
    ],
)
def test_dataset_store_sources_tell_unknown_from_none(
    requests_mock: requests_mock_module.Mocker,
    graph_response: Dict[str, Any],
    stored_from_qris: Optional[List[str]],
    read_failed: bool,
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    requests_mock.get(
        f"{BASE_URL}/data-sets/ds-1",
        json={
            "id": "ds-1",
            "name": "stored.qvd",
            "spaceId": "space-1",
            "secureQri": STORED_QVD,
            "type": "qvd",
            "createdTime": "2024-01-09T18:05:36.000Z",
            "lastModifiedTime": "2024-01-09T18:05:36.000Z",
            "operational": {"size": 1, "rowCount": 1},
            "schema": {"dataFields": []},
        },
    )
    requests_mock.get(_expand_url(STORED_QVD), **graph_response)

    dataset = api._get_dataset("ds-1", "item-1")

    assert dataset.stored_from_qris == stored_from_qris
    assert bool(api.lineage_read_failures) is read_failed


def test_failed_field_lineage_of_a_dataset_is_flagged(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    requests_mock.get(
        f"{BASE_URL}/data-sets/ds-1",
        json={
            "id": "ds-1",
            "name": "stored.qvd",
            "spaceId": "space-1",
            "secureQri": STORED_QVD,
            "type": "qvd",
            "createdTime": "2024-01-09T18:05:36.000Z",
            "lastModifiedTime": "2024-01-09T18:05:36.000Z",
            "operational": {"size": 1, "rowCount": 1},
            "schema": {"dataFields": []},
        },
    )
    requests_mock.get(
        _expand_url(STORED_QVD),
        json={
            "graph": {
                "nodes": {f"{STORED_QVD}#Out": {"label": "Out"}},
                "edges": [
                    {"source": f"{ETL_APP}#Curve", "target": f"{STORED_QVD}#Out"}
                ],
            }
        },
    )
    requests_mock.get(
        f"{BASE_URL}/lineage-graphs/nodes/{quote(STORED_QVD, safe='')}/actions/expand"
        f"?node={quote(f'{STORED_QVD}#Out', safe='')}&level=FIELD",
        status_code=500,
    )

    dataset = api._get_dataset("ds-1", "item-1")

    assert dataset.stored_from_qris == [f"{ETL_APP}#Curve"]
    assert dataset.stored_field_lineage_failed


@pytest.mark.parametrize(
    "field_lineage_failed, tables_read_failed, has_graph_tables",
    [
        (True, False, True),
        (False, True, False),
        # The graph was read, but the load script or data model then failed.
        (False, True, True),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_partly_known_lineage_is_only_added_to(
    mock_api_cls: MagicMock,
    field_lineage_failed: bool,
    tables_read_failed: bool,
    has_graph_tables: bool,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}
    app = _app(
        "etl-app",
        []
        if not has_graph_tables
        else [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[f"{SOURCE_QVD}#Curve"],
                field_lineage_failed=field_lineage_failed,
            )
        ],
    )
    # A graph that could not be read leaves the script's tables, which an
    # earlier run may have merged with graph tables.
    app.tables_read_failed = tables_read_failed
    app.script = "LIB CONNECT TO 'db';\nCurve: SQL SELECT id FROM sales.curve;"

    with patch.object(
        QlikSenseSource,
        "_build_script_tables",
        lambda self, app, parsed=None: [
            ScriptTableLineage(
                label="Curve",
                identifier="etl-app.curve",
                upstream_urns=[make_dataset_urn("postgres", "db.sales.curve")],
            )
        ],
    ):
        written, added = _lineage(source._gen_app_workunit(app))

    curve = _urn("etl-app.curve")
    assert curve not in written
    assert curve in added


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_table_waits_for_source_field_names_read_later_in_the_run(
    mock_api_cls: MagicMock,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}
    qvd_table = f"{SOURCE_QVD}#Curve"
    # Renamed from a field whose name the run has not read yet, such as one of
    # a QVD read later in the run.
    app = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[qvd_table],
                fields=["Day", "Note"],
                field_source_qris={
                    "Day": [_field_qri(qvd_table, "Day")],
                    "Note": [_field_qri(qvd_table, "comment")],
                },
            )
        ],
    )

    curve = _urn("etl-app.curve")
    # Nothing is written for the table until the end of the run.
    during, _ = _lineage(source._gen_app_workunit(app))
    assert curve not in during

    # The QVD read later names the field, so the lineage is written whole.
    source._learn_field_names(["comment"])
    written, _ = _lineage(source._gen_pending_table_lineage())
    assert written[curve] == [_urn("space-1.src.qvd")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_source_field_never_named_only_adds_to_column_lineage(
    mock_api_cls: MagicMock,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}
    qvd_table = f"{SOURCE_QVD}#Curve"
    app = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[qvd_table],
                fields=["Note"],
                field_source_qris={"Note": [_field_qri(qvd_table, "comment")]},
            )
        ],
    )

    list(source._gen_app_workunit(app))
    written, added = _lineage(source._gen_pending_table_lineage())

    curve = _urn("etl-app.curve")
    assert curve not in written
    assert added[curve] == [_urn("space-1.src.qvd")]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_partly_read_table_waits_for_source_field_names_read_later(
    mock_api_cls: MagicMock,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}
    qvd_table = f"{SOURCE_QVD}#Curve"
    app = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[qvd_table],
                fields=["Note"],
                field_source_qris={"Note": [_field_qri(qvd_table, "comment")]},
            )
        ],
    )
    # A denied load script leaves the table's lineage only added to, but the
    # column edge named by a QVD read later in the run is still added.
    app.script_read_failed = True

    curve = _urn("etl-app.curve")
    assert curve not in _patch_ops(source._gen_app_workunit(app))

    source._learn_field_names(["comment"])
    ops = _patch_ops(source._gen_pending_table_lineage())[curve]
    assert any(
        op == "add"
        and path.startswith("/fineGrainedLineages/")
        and path.endswith(
            make_schema_field_urn(_urn("space-1.src.qvd"), "comment").replace("/", "~1")
        )
        for op, path in ops
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unnamed_source_field_does_not_block_replace_without_column_lineage(
    mock_api_cls: MagicMock,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(
            {**CLOUD_CONFIG, "include_column_lineage": False}
        ),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source._allowed_space_ids = {"space-1"}
    qvd_table = f"{SOURCE_QVD}#Curve"
    app = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Curve",
                label="Curve",
                source_qris=[qvd_table],
                fields=["Note"],
                field_source_qris={"Note": [_field_qri(qvd_table, "comment")]},
            )
        ],
    )

    written, added = _lineage(
        [*source._gen_app_workunit(app), *source._gen_pending_table_lineage()]
    )

    curve = _urn("etl-app.curve")
    assert written[curve] == [_urn("space-1.src.qvd")]
    assert curve not in added


@pytest.mark.parametrize(
    "previous, checkpointing, tables_unknown",
    [
        # A last read whose tables were not all known can't tell either.
        (AppCheckpoint(urns=["urn:li:container:x"]), True, True),
        (
            AppCheckpoint(
                urns=["urn:li:container:x"], table_urns_by_qri={}, tables_unknown=True
            ),
            True,
            True,
        ),
        # Without stateful ingestion there is never a last read, so the app is
        # taken as having no tables rather than patched on every run.
        (None, False, False),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_missing_lineage_graph_with_unknown_last_tables(
    mock_api_cls: MagicMock,
    previous: Optional[AppCheckpoint],
    checkpointing: bool,
    tables_unknown: bool,
) -> None:
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source.app_state = MagicMock()
    source.app_state.previous.return_value = previous
    source.app_state.is_checkpointing_enabled.return_value = checkpointing
    app = _app("etl-app", [])
    app.lineage_graph_missing = True

    source._check_missing_lineage_graph(app)

    assert app.read_complete
    assert app.tables_read_failed is tables_unknown


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_app_with_unknown_tables_records_them_as_unknown(
    mock_api_cls: MagicMock,
) -> None:
    api = mock_api_cls.return_value
    api.unread_items = []
    api.list_datasets.return_value = DATASETS
    api.get_user_name.return_value = None
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    source.app_state = MagicMock()
    app = _app("etl-app", [])
    # Read in full, but whether it has tables could not be told this run.
    app.tables_read_failed = True

    list(source._gen_recorded_app_workunit(app))

    # Recorded as unknown, so a later 404 isn't read as "the app has no tables".
    assert source.app_state.record.call_args.args[1].tables_unknown


ORDERS_DB_TABLE = "qri:db:sqlserver://Hash#Orders"
RELOAD_CONFIG = {
    "data_connection_to_platform_instance": {
        "Sales": {"platform": "mssql", "database": "shop"},
        "Finance:Archive": {"platform": "mssql", "database": "archive"},
    }
}
SHOP_ORDERS = make_dataset_urn("mssql", "shop.dbo.orders")


def _reload_app(
    statements: Optional[List[ReloadStatement]],
    source_qri: str = ORDERS_DB_TABLE,
    read_failed: bool = False,
) -> App:
    app = _app(
        "etl-app",
        [
            LineageTable(
                qri=f"{ETL_APP}#Orders",
                label="Orders",
                source_qris=[source_qri],
                database_source_labels={}
                if read_failed
                else {source_qri: "dbo.[ORDERS]"},
                fields=["ORDER_ID", "Amount"],
            )
        ],
    )
    app.reload_statements = statements
    app.reload_statements_read_failed = read_failed
    return app


def _reload_source(mock_api_cls: MagicMock, app: App) -> QlikSenseSource:
    source = _source(mock_api_cls, DATASETS, items=[app], config=RELOAD_CONFIG)
    mock_api_cls.return_value.get_data_connections.return_value = {}
    return source


def _sql(connection: str, sql: str) -> ReloadStatement:
    return ReloadStatement(discriminator=f"{connection};", statement=f"SQL {sql}")


def _upstream_lineage(
    workunits: Iterable[MetadataWorkUnit], urn: str
) -> Optional[UpstreamLineageClass]:
    return next(
        (
            lineage
            for wu in workunits
            if wu.get_urn() == urn
            for lineage in [wu.get_aspect_of_type(UpstreamLineageClass)]
            if lineage
        ),
        None,
    )


def _schema_graph(urn: str, fields: List[str]) -> MagicMock:
    """DataHub, holding the schema of one dataset."""
    schema = SchemaMetadataClass(
        schemaName="orders",
        platform="urn:li:dataPlatform:mssql",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=name,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="varchar",
            )
            for name in fields
        ],
    )
    graph = MagicMock()
    graph.get_entities.side_effect = lambda entity_name, urns, **kwargs: {
        u: {SchemaMetadataClass.ASPECT_NAME: (schema, None)} for u in urns if u == urn
    }
    graph.get_aspect.side_effect = lambda u, aspect_type, **kwargs: (
        schema if u == urn and aspect_type is SchemaMetadataClass else None
    )
    graph._make_schema_resolver.side_effect = lambda platform, platform_instance, env: (
        SchemaResolver(
            platform=platform, platform_instance=platform_instance, env=env, graph=graph
        )
    )
    return graph


@pytest.mark.parametrize(
    "graph, columns",
    [
        pytest.param(
            None, ["Amount<-amount", "ORDER_ID<-order_id"], id="schema unknown"
        ),
        pytest.param(
            # The source keeps its columns' case, unlike Qlik's fields.
            _schema_graph(SHOP_ORDERS, ["Order_ID", "AMOUNT", "Other"]),
            ["Amount<-AMOUNT", "ORDER_ID<-Order_ID"],
            id="schema in DataHub",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_database_table_resolves_through_reload_sql(
    mock_api_cls: MagicMock, graph: Optional[MagicMock], columns: List[str]
) -> None:
    app = _reload_app(
        [
            ReloadStatement(discriminator="RESIDENT Config;", statement=""),
            _sql(
                "Finance:Sales",
                "/* built from a config file */ SELECT * FROM dbo.ORDERS WHERE 0 = 0"
                if graph
                else "SELECT order_id, AMOUNT FROM dbo.ORDERS WHERE 0 = 0",
            ),
            _sql("Finance:Sales", "SELECT * FROM dbo.customers"),
        ]
    )
    source = _source(
        mock_api_cls, DATASETS, items=[app], config=RELOAD_CONFIG, graph=graph
    )
    mock_api_cls.return_value.get_data_connections.return_value = {}

    lineage = _upstream_lineage(source.get_workunits_internal(), _urn("etl-app.orders"))

    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [SHOP_ORDERS]
    # Fields keep Qlik's names, and columns the source's, so a SELECT * only
    # maps the columns DataHub holds that the table has.
    assert (
        sorted(
            f"{SchemaFieldUrn.from_string(f.downstreams[0]).field_path}"
            f"<-{SchemaFieldUrn.from_string(f.upstreams[0]).field_path}"
            for f in lineage.fineGrainedLineages or []
            if f.downstreams and f.upstreams
        )
        == columns
    )
    assert source.reporter.reload_sql_tables_with_lineage == 1
    assert not source.reporter.reload_sql_tables_not_in_datahub


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_join_maps_only_columns_of_the_graph_source(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app(
        [
            _sql(
                "Finance:Sales",
                "SELECT o.order_id, c.amount FROM dbo.orders o "
                "JOIN dbo.customers c ON o.customer_id = c.id",
            )
        ]
    )
    source = _reload_source(mock_api_cls, app)

    lineage = _upstream_lineage(source.get_workunits_internal(), _urn("etl-app.orders"))

    # The graph names only the orders table, so the customers column, which
    # the table has no edge to, is left out.
    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [SHOP_ORDERS]
    assert [
        SchemaFieldUrn.from_string(f.downstreams[0]).field_path
        for f in lineage.fineGrainedLineages or []
        if f.downstreams
    ] == ["ORDER_ID"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_join_with_a_table_missing_from_datahub_is_not_reported(
    mock_api_cls: MagicMock,
) -> None:
    source = _source(
        mock_api_cls,
        DATASETS,
        items=[
            _reload_app(
                [
                    _sql(
                        "Finance:Sales",
                        "SELECT o.order_id FROM dbo.orders o "
                        "JOIN dbo.customers c ON o.customer_id = c.id",
                    )
                ]
            )
        ],
        config=RELOAD_CONFIG,
        graph=_schema_graph(SHOP_ORDERS, ["order_id", "customer_id"]),
    )
    mock_api_cls.return_value.get_data_connections.return_value = {}

    written, _ = _lineage(source.get_workunits_internal())

    assert written[_urn("etl-app.orders")] == [SHOP_ORDERS]
    # Only the customers table, which the graph doesn't name, lacks a schema.
    assert not source.reporter.reload_sql_tables_not_in_datahub


@pytest.mark.parametrize(
    "unread_sql, linked",
    [
        pytest.param(
            "SELECT id FROM dbo.customers GROUP BY id ORDER BY id", True, id="keyword"
        ),
        pytest.param("SELECT * FROM dbo.customers c, [order] o", False, id="from list"),
        pytest.param(
            "SELECT * FROM (SELECT * FROM x) a JOIN dbo.[order] b ON a.id = b.id",
            False,
            id="join after subquery",
        ),
        pytest.param("EXEC('SELECT * FROM dbo.[order]')", False, id="dynamic SQL"),
        pytest.param("SELECT * FROM dbo.order", False, id="keyword as a table name"),
        pytest.param(
            "SELECT * FROM dbo.customers c, order o", False, id="keyword after a comma"
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c OUTER APPLY dbo.[order] o",
            False,
            id="outer apply",
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c CROSS APPLY dbo.[order] o",
            False,
            id="cross apply",
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c, /* hint */ order o",
            False,
            id="keyword after a block comment",
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c, -- note\n order o",
            False,
            id="keyword after a line comment",
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c CROSS /* c */ APPLY dbo.[order] o",
            False,
            id="comment inside cross apply",
        ),
        pytest.param("INSERT dbo.[order] SELECT 1", False, id="insert without into"),
        pytest.param(
            "MERGE dbo.[order] USING dbo.customers c ON 1 = 1",
            False,
            id="merge without into",
        ),
        pytest.param("DELETE dbo.[order]", False, id="delete without from"),
        pytest.param(
            "DELETE FROM dbo.customers WHERE id = 1; INSERT order SELECT 1",
            False,
            id="second statement inserts without into",
        ),
        pytest.param(
            "INSERT INTO dbo.customers SELECT id FROM x ORDER BY id",
            True,
            id="insert into with order by",
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c JOIN dbo.x y ON c.id = y.id, dbo.[order] o",
            False,
            id="comma after a join condition",
        ),
        pytest.param(
            "SELECT * FROM dbo.customers c JOIN dbo.x y ON COALESCE(c.id, y.id) = 1 "
            "WHERE c.kind = 'order'",
            True,
            id="comma in a join condition",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_reload_sql_only_counts_names_in_table_lists(
    mock_api_cls: MagicMock, unread_sql: str, linked: bool
) -> None:
    app = _reload_app(
        [
            _sql("Finance:Sales", "SELECT * FROM dbo.[order]"),
            _sql("Finance:Unknown", unread_sql),
        ]
    )
    app.lineage_tables[0].database_source_labels = {ORDERS_DB_TABLE: "dbo.[ORDER]"}
    source = _reload_source(mock_api_cls, app)

    written, _ = _lineage(source.get_workunits_internal())

    shop_order = make_dataset_urn("mssql", "shop.dbo.order")
    assert written[_urn("etl-app.orders")] == ([shop_order] if linked else [])
    assert bool(source.reporter.ambiguous_reload_sql_sources) is not linked


@pytest.mark.parametrize(
    "statements, source_qri, unresolved, ambiguous",
    [
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Archive", "SELECT * FROM dbo.orders"),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="two databases",
        ),
        pytest.param(
            # The unread statement may read the label's table in another
            # database.
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Unknown", "SELECT * FROM dbo.orders"),
            ],
            ORDERS_DB_TABLE,
            ["etl-app: Finance:Unknown"],
            True,
            id="also named by unresolved SQL",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Sales", "SELECT * FROM dbo.orders_$(vYear)"),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="also named by SQL with an unset variable",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Archive", "SELECT * FROM orders"),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="also named by SQL without its schema",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Archive", "EXEC('SELECT * FROM dbo.orders')"),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="also named by dynamic SQL",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql(
                    "Finance:Archive",
                    "INSERT INTO dbo.jobs (query) SELECT 'SELECT * FROM dbo.orders'",
                ),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="also named by parsed SQL reading no table",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql(
                    "Finance:Archive",
                    "SELECT id, 'SELECT * FROM dbo.orders' AS query FROM dbo.jobs",
                ),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="also named in a string literal of parsed SQL",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                ReloadStatement(
                    discriminator="Finance:Archive;",
                    statement="INSERT INTO dbo.jobs SELECT * FROM dbo.orders",
                ),
            ],
            ORDERS_DB_TABLE,
            [],
            True,
            id="also named by SQL without the SQL prefix",
        ),
        pytest.param(
            [_sql("Finance:Sales", "SELECT * FROM dbo.orders")],
            "qri:db:databricks://Hash#Orders",
            [],
            False,
            id="other platform",
        ),
        pytest.param(
            [_sql("Finance:Sales", "SELECT * FROM dbo.orders")],
            "qri:db:odbc://Hash#Orders",
            [],
            False,
            id="scheme of no known platform",
        ),
        pytest.param(
            [
                _sql("Finance:Unknown", "SELECT * FROM dbo.orders"),
                _sql("Finance:Unknown", "SELECT * FROM dbo.orders WHERE 0 = 0"),
            ],
            ORDERS_DB_TABLE,
            ["etl-app: Finance:Unknown"],
            False,
            id="unresolved connection",
        ),
        pytest.param(
            [_sql("Finance:Sales", "SELECT * FROM sales.orders")],
            ORDERS_DB_TABLE,
            [],
            False,
            id="other schema",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_database_table_links_only_one_matching_reload_table(
    mock_api_cls: MagicMock,
    statements: List[ReloadStatement],
    source_qri: str,
    unresolved: List[str],
    ambiguous: bool,
) -> None:
    source = _reload_source(mock_api_cls, _reload_app(statements, source_qri))

    written, added = _lineage(source.get_workunits_internal())

    assert written == {_urn("etl-app.orders"): []}
    assert not added
    assert list(source.reporter.unresolved_data_connections) == unresolved
    assert bool(source.reporter.ambiguous_reload_sql_sources) is ambiguous


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_reload_sql_only_adds_to_table_lineage(
    mock_api_cls: MagicMock,
) -> None:
    source = _reload_source(mock_api_cls, _reload_app(None, read_failed=True))

    written, _ = _lineage(source.get_workunits_internal())

    # The edges an earlier run read from the reload SQL must stay.
    assert _urn("etl-app.orders") not in written


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_keeps_the_graph_fields_it_does_not_select(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app([_sql("Finance:Sales", "SELECT order_id FROM dbo.orders")])
    source = _reload_source(mock_api_cls, app)

    [schema] = [
        aspect
        for wu in source.get_workunits_internal()
        if wu.get_urn() == _urn("etl-app.orders")
        for aspect in [wu.get_aspect_of_type(SchemaMetadataClass)]
        if aspect
    ]

    # The app has no data model, so the graph's fields are the table's.
    assert sorted(field.fieldPath for field in schema.fields) == ["Amount", "ORDER_ID"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unlabelled_database_source_only_adds_to_table_lineage(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app([_sql("Finance:Sales", "SELECT * FROM dbo.orders")])
    # The upstream graph gave no label for the database table this run.
    app.lineage_tables[0].database_source_labels = {}
    source = _reload_source(mock_api_cls, app)

    written, _ = _lineage(source.get_workunits_internal())

    assert _urn("etl-app.orders") not in written


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_database_labels_are_not_reported_as_unread_reload_sql(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app([_sql("Finance:Sales", "SELECT * FROM dbo.orders")])
    app.lineage_tables[0].database_source_labels = {}
    source = _reload_source(mock_api_cls, app)
    api = mock_api_cls.return_value
    api.get_database_labels.side_effect = RuntimeError("lineage graph unavailable")

    written, _ = _lineage(source.get_workunits_internal())

    assert _urn("etl-app.orders") not in written
    assert [f.split(":")[0] for f in source.reporter.database_label_read_failures] == [
        "etl-app"
    ]
    assert not api.reload_statement_read_failures


def test_database_sources_are_read_with_the_reload_sql(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    node = quote(REPORT_APP, safe="")
    table = f"{REPORT_APP}#Tbl"
    requests_mock.get(
        f"{BASE_URL}/lineage-graphs/nodes/{node}/actions/expand?level=TABLE",
        json={
            "graph": {
                "nodes": {table: {"label": "Orders"}},
                "edges": [{"source": ORDERS_DB_TABLE, "target": table}],
            }
        },
    )
    # Only the upstream graph labels the database tables.
    requests_mock.get(
        f"{BASE_URL}/lineage-graphs/nodes/{node}?level=TABLE&up=1",
        json={
            "graph": {
                "nodes": {ORDERS_DB_TABLE: {"label": "dbo.ORDERS"}},
                "edges": [{"source": ORDERS_DB_TABLE, "target": table}],
            }
        },
    )
    requests_mock.get(
        f"{BASE_URL}/apps/report-app/data/lineage",
        json=[{"discriminator": "Finance:Sales;", "statement": "SQL SELECT 1"}],
    )

    tables = api.get_table_lineage(REPORT_APP)
    assert tables is not None
    assert tables[0].database_source_labels == {}
    app = _app("report-app", tables)
    app.qri = REPORT_APP
    api._read_reload_statements(app)

    assert api.get_database_labels(REPORT_APP) == {ORDERS_DB_TABLE: "dbo.ORDERS"}
    assert app.reload_statements == [
        ReloadStatement(discriminator="Finance:Sales;", statement="SQL SELECT 1")
    ]

    requests_mock.get(f"{BASE_URL}/apps/report-app/data/lineage", status_code=404)
    api._read_reload_statements(app)

    assert app.reload_statements_read_failed
    assert [f.split(":")[0] for f in api.reload_statement_read_failures] == [
        "report-app"
    ]


@pytest.mark.parametrize(
    "statements",
    [
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                # Names another table, not the label's.
                _sql("Finance:Unknown", "SELECT * FROM dbo.orders_archive"),
            ],
            id="unread SQL naming a longer table",
        ),
        pytest.param(
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Sales", "SELECT * FROM dbo.orders_archive"),
            ],
            id="SQL naming a longer table",
        ),
        pytest.param(
            [
                ReloadStatement(
                    discriminator="Finance:Sales;",
                    statement="SELECT * FROM dbo.orders",
                )
            ],
            id="SELECT without the SQL prefix",
        ),
        pytest.param(
            [
                ReloadStatement(
                    discriminator="Finance:Sales;",
                    statement="WITH o AS (SELECT * FROM dbo.orders) SELECT * FROM o",
                )
            ],
            id="WITH without the SQL prefix",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_label_matches_whole_table_names(
    mock_api_cls: MagicMock, statements: List[ReloadStatement]
) -> None:
    source = _reload_source(mock_api_cls, _reload_app(statements))

    written, _ = _lineage(source.get_workunits_internal())

    assert written[_urn("etl-app.orders")] == [SHOP_ORDERS]
    assert not source.reporter.ambiguous_reload_sql_sources


@pytest.mark.parametrize(
    "statements, source_qri, unmatched",
    [
        pytest.param(
            [], ORDERS_DB_TABLE, ["etl-app: dbo.[ORDERS] (mssql)"], id="no SQL"
        ),
        pytest.param(
            [_sql("Finance:Sales", "SELECT * FROM dbo.orders")],
            "qri:db:odbc://Hash#Orders",
            ["etl-app: dbo.[ORDERS] (odbc)"],
            id="scheme of no known platform",
        ),
        pytest.param(
            # Already reported as an unresolved connection.
            [_sql("Finance:Unknown", "SELECT * FROM dbo.orders")],
            ORDERS_DB_TABLE,
            [],
            id="unresolved connection",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_database_table_matching_no_reload_table_is_reported(
    mock_api_cls: MagicMock,
    statements: List[ReloadStatement],
    source_qri: str,
    unmatched: List[str],
) -> None:
    source = _reload_source(mock_api_cls, _reload_app(statements, source_qri))

    written, _ = _lineage(source.get_workunits_internal())

    assert written == {_urn("etl-app.orders"): []}
    assert list(source.reporter.unmatched_reload_sql_sources) == unmatched


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unmapped_database_scheme_matching_its_platform_is_linked(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app(
        [_sql("Finance:Warehouse", "SELECT * FROM sales.orders")],
        source_qri="qri:db:oracle://Hash#Orders",
    )
    app.lineage_tables[0].database_source_labels = {
        "qri:db:oracle://Hash#Orders": "SALES.ORDERS"
    }
    source = _source(
        mock_api_cls,
        DATASETS,
        items=[app],
        config={
            "data_connection_to_platform_instance": {
                "Warehouse": {"platform": "oracle", "database": "orcl"}
            }
        },
    )
    mock_api_cls.return_value.get_data_connections.return_value = {}

    written, _ = _lineage(source.get_workunits_internal())

    assert written[_urn("etl-app.orders")] == [
        make_dataset_urn("oracle", "orcl.sales.orders")
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_unread_data_connections_only_add_to_reload_sql_lineage(
    mock_api_cls: MagicMock,
) -> None:
    source = _reload_source(
        mock_api_cls,
        _reload_app([_sql("Finance:Orders", "SELECT * FROM dbo.orders")]),
    )
    # The statement's connection may be one the failed listing would resolve.
    mock_api_cls.return_value.unread_items = [
        UnreadItem(name=UNREAD_DATA_CONNECTIONS, cause="timed out")
    ]

    written, _ = _lineage(source.get_workunits_internal())

    assert _urn("etl-app.orders") not in written


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_table_missing_from_datahub_is_reported(
    mock_api_cls: MagicMock,
) -> None:
    # The warehouse source kept the table's case, which the SQL doesn't use.
    graph = _schema_graph(make_dataset_urn("mssql", "Shop.dbo.Orders"), ["Order_ID"])
    source = _source(
        mock_api_cls,
        DATASETS,
        items=[_reload_app([_sql("Finance:Sales", "SELECT * FROM dbo.orders")])],
        config=RELOAD_CONFIG,
        graph=graph,
    )
    mock_api_cls.return_value.get_data_connections.return_value = {}

    written, _ = _lineage(source.get_workunits_internal())

    assert written[_urn("etl-app.orders")] == [SHOP_ORDERS]
    assert list(source.reporter.reload_sql_tables_not_in_datahub) == [
        f"etl-app: {SHOP_ORDERS}"
    ]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_links_database_sources_the_script_does_not_trace(
    mock_api_cls: MagicMock,
) -> None:
    sources = {
        "qri:db:sqlserver://Hash#Orders2023": "dbo.orders_2023",
        "qri:db:sqlserver://Hash#Orders2024": "dbo.orders_2024",
    }
    app = _reload_app(
        [
            _sql("Finance:Sales", "SELECT * FROM dbo.orders_2023"),
            _sql("Finance:Sales", "SELECT * FROM dbo.orders_2024"),
        ]
    )
    table = app.lineage_tables[0]
    table.source_qris = list(sources)
    table.database_source_labels = sources
    source = _reload_source(mock_api_cls, app)
    traced = make_dataset_urn("mssql", "shop.dbo.orders_2023")
    # The script traces the first source; the second is a CONCATENATE from SQL
    # it builds at run time.
    script_tables = {
        table.qri: ScriptTableLineage(
            label="Orders", identifier="etl-app.orders", upstream_urns=[traced]
        )
    }

    written, _ = _lineage(source._gen_lineage_tables_workunit(app, script_tables))

    assert written[_urn("etl-app.orders")] == [
        traced,
        make_dataset_urn("mssql", "shop.dbo.orders_2024"),
    ]


@pytest.mark.parametrize(
    "script_urns, statements, linked, ambiguous",
    [
        pytest.param(
            [SHOP_ORDERS],
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Archive", "SELECT * FROM dbo.orders"),
            ],
            [make_dataset_urn("mssql", "archive.dbo.orders")],
            False,
            id="script traces one, reload SQL reads the other",
        ),
        pytest.param(
            # The script spells the table it traces in another case.
            [make_dataset_urn("mssql", "shop.dbo.ORDERS")],
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Archive", "SELECT * FROM dbo.orders"),
            ],
            [make_dataset_urn("mssql", "archive.dbo.orders")],
            False,
            id="script traces one in another case",
        ),
        pytest.param(
            [],
            [_sql("Finance:Sales", "SELECT * FROM dbo.orders")],
            [],
            True,
            id="one table read for two sources",
        ),
        pytest.param(
            [],
            [
                _sql("Finance:Sales", "SELECT * FROM dbo.orders"),
                _sql("Finance:Archive", "SELECT * FROM dbo.orders"),
            ],
            [],
            True,
            id="which source is which is unknown",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_database_sources_sharing_a_label_need_one_untraced_match(
    mock_api_cls: MagicMock,
    script_urns: List[str],
    statements: List[ReloadStatement],
    linked: List[str],
    ambiguous: bool,
) -> None:
    sources = {
        "qri:db:sqlserver://ShopHash#Orders": "dbo.orders",
        "qri:db:sqlserver://ArchiveHash#Orders": "dbo.orders",
    }
    app = _reload_app(statements)
    table = app.lineage_tables[0]
    table.source_qris = list(sources)
    table.database_source_labels = sources
    table.field_source_qris = {
        "ORDER_ID": ["qri:db:sqlserver://ShopHash#Orders#order_id"]
    }
    source = _reload_source(mock_api_cls, app)
    script_tables = {
        table.qri: ScriptTableLineage(
            label="Orders", identifier="etl-app.orders", upstream_urns=script_urns
        )
    }

    workunits = list(source._gen_lineage_tables_workunit(app, script_tables))
    written, _ = _lineage(workunits)

    assert written.get(_urn("etl-app.orders"), []) == script_urns + linked
    assert bool(source.reporter.ambiguous_reload_sql_sources) is ambiguous
    # Which source the linked table is isn't known, so neither is the field
    # the graph says each loads.
    lineage = _upstream_lineage(workunits, _urn("etl-app.orders"))
    column_sources = {
        SchemaFieldUrn.from_string(upstream).parent
        for f in (lineage.fineGrainedLineages or [] if lineage else [])
        for upstream in f.upstreams or []
    }
    assert not column_sources & set(linked)


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param("UPDATE dbo.orders SET amount = 0", id="update"),
        pytest.param(
            "UPDATE o SET amount = 0 FROM dbo.orders o JOIN dbo.rates r ON o.id = r.id",
            id="update from",
        ),
        pytest.param(
            "MERGE INTO dbo.orders t USING dbo.staging s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.amount = s.amount",
            id="merge",
        ),
        pytest.param("INSERT INTO dbo.orders SELECT * FROM dbo.staging", id="insert"),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_tables_written_are_not_load_sources(
    mock_api_cls: MagicMock, statement: str
) -> None:
    source = _reload_source(
        mock_api_cls, _reload_app([_sql("Finance:Sales", statement)])
    )

    written, added = _lineage(source.get_workunits_internal())

    assert written == {_urn("etl-app.orders"): []}
    assert not added
    # The written table may be the label's, so a match elsewhere isn't certain.
    assert not source.reporter.unmatched_reload_sql_sources


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_insert_reads_its_select_tables(mock_api_cls: MagicMock) -> None:
    app = _reload_app(
        [_sql("Finance:Sales", "INSERT INTO dbo.audit SELECT * FROM dbo.orders")]
    )
    source = _reload_source(mock_api_cls, app)

    written, _ = _lineage(source.get_workunits_internal())

    assert written[_urn("etl-app.orders")] == [SHOP_ORDERS]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_columns_merge_with_script_columns_in_any_case(
    mock_api_cls: MagicMock,
) -> None:
    sources = {
        "qri:db:sqlserver://Hash#Orders2023": "dbo.orders_2023",
        "qri:db:sqlserver://Hash#Orders2024": "dbo.orders_2024",
    }
    app = _reload_app([_sql("Finance:Sales", "SELECT order_id FROM dbo.orders_2024")])
    table = app.lineage_tables[0]
    table.source_qris = list(sources)
    table.database_source_labels = sources
    source = _reload_source(mock_api_cls, app)
    traced = make_dataset_urn("mssql", "shop.dbo.orders_2023")
    # The parser folded the script's column to lower case; the graph's field
    # that the reload SQL maps is ORDER_ID.
    script_tables = {
        table.qri: ScriptTableLineage(
            label="Orders",
            identifier="etl-app.orders",
            upstream_urns=[traced],
            column_upstreams={"order_id": [make_schema_field_urn(traced, "order_id")]},
        )
    }

    workunits = list(source._gen_lineage_tables_workunit(app, script_tables))

    [schema] = [
        aspect
        for wu in workunits
        if wu.get_urn() == _urn("etl-app.orders")
        for aspect in [wu.get_aspect_of_type(SchemaMetadataClass)]
        if aspect
    ]
    assert [field.fieldPath for field in schema.fields] == ["order_id"]
    lineage = _upstream_lineage(workunits, _urn("etl-app.orders"))
    assert lineage is not None
    assert [
        (SchemaFieldUrn.from_string(downstream).field_path, sorted(f.upstreams or []))
        for f in lineage.fineGrainedLineages or []
        for downstream in f.downstreams or []
    ] == [
        (
            "order_id",
            sorted(
                make_schema_field_urn(urn, "order_id")
                for urn in [traced, make_dataset_urn("mssql", "shop.dbo.orders_2024")]
            ),
        )
    ]


def _reload_columns(lineage: Optional[UpstreamLineageClass]) -> List[str]:
    assert lineage is not None
    return sorted(
        f"{SchemaFieldUrn.from_string(f.downstreams[0]).field_path}"
        f"<-{SchemaFieldUrn.from_string(f.upstreams[0]).field_path}"
        for f in lineage.fineGrainedLineages or []
        if f.downstreams and f.upstreams
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_columns_need_one_statement_reading_the_table(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app(
        [
            _sql("Finance:Sales", "SELECT order_id, amount FROM dbo.orders"),
            # May load another Qlik table, whose fields aren't this table's.
            _sql("Finance:Sales", "SELECT amount AS order_id FROM dbo.orders"),
        ]
    )
    source = _reload_source(mock_api_cls, app)

    lineage = _upstream_lineage(source.get_workunits_internal(), _urn("etl-app.orders"))

    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [SHOP_ORDERS]
    assert _reload_columns(lineage) == []


@pytest.mark.parametrize(
    "field_sources, columns",
    [
        pytest.param(
            {"ORDER_ID": "order_id", "Amount": "amount"},
            ["Amount<-amount", "ORDER_ID<-order_id"],
            id="graph agrees",
        ),
        pytest.param(
            # Qlik hashes the names the database returned, in their case.
            {"ORDER_ID": "ORDER_ID", "Amount": "Amount"},
            ["Amount<-amount", "ORDER_ID<-order_id"],
            id="graph names the columns in another case",
        ),
        pytest.param(
            # A LOAD over the SQL swapped the two fields' names.
            {"ORDER_ID": "amount", "Amount": "order_id"},
            [],
            id="fields renamed",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_columns_follow_the_graph_field_sources(
    mock_api_cls: MagicMock, field_sources: Dict[str, str], columns: List[str]
) -> None:
    app = _reload_app(
        [_sql("Finance:Sales", "SELECT order_id, amount FROM dbo.orders")]
    )
    app.lineage_tables[0].field_source_qris = {
        field: [_field_qri(ORDERS_DB_TABLE, column)]
        for field, column in field_sources.items()
    }
    source = _reload_source(mock_api_cls, app)

    lineage = _upstream_lineage(source.get_workunits_internal(), _urn("etl-app.orders"))

    assert _reload_columns(lineage) == columns


def test_upstream_graph_holds_the_database_labels(
    requests_mock: requests_mock_module.Mocker,
) -> None:
    requests_mock.get(f"{BASE_URL}/api-keys", json={})
    api = QlikAPI(QlikSourceConfig.model_validate(CLOUD_CONFIG))
    node = f"{BASE_URL}/lineage-graphs/nodes/{quote(REPORT_APP, safe='')}"
    requests_mock.get(
        f"{node}?level=RESOURCE&up=1",
        json={"graph": {"nodes": {UNLISTED_QVD: {"label": "prices.qvd"}}}},
    )
    requests_mock.get(
        f"{node}?level=TABLE&up=2",
        json={"graph": {"nodes": {ORDERS_DB_TABLE: {"label": "dbo.ORDERS"}}}},
    )
    one_level = requests_mock.get(f"{node}?level=TABLE&up=1", json={"graph": {}})

    assert api.get_lineage_upstreams(REPORT_APP).database_labels == {
        ORDERS_DB_TABLE: "dbo.ORDERS"
    }
    assert not one_level.called


@pytest.mark.parametrize("graph_read", [True, False])
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reload_sql_labels_come_from_the_upstream_graph_already_read(
    mock_api_cls: MagicMock, graph_read: bool
) -> None:
    app = _app(
        "report-app",
        [
            LineageTable(
                qri=f"{REPORT_APP}#Tbl",
                label="Orders",
                source_qris=[f"{UNLISTED_QVD}#Prices", ORDERS_DB_TABLE],
            )
        ],
    )
    app.qri = REPORT_APP
    app.reload_statements = []
    source = _source(mock_api_cls, DATASETS, items=[app])
    api = mock_api_cls.return_value
    api.get_database_labels.return_value = {ORDERS_DB_TABLE: "dbo.ORDERS"}
    if graph_read:
        source._lineage_upstreams[app.id] = LineageUpstreams(
            database_labels={ORDERS_DB_TABLE: "dbo.ORDERS"}
        )

    source._read_database_source_labels(app)

    assert app.lineage_tables[0].database_source_labels == {
        ORDERS_DB_TABLE: "dbo.ORDERS"
    }
    # The one-level graph is only read when the upstream graph wasn't.
    assert api.get_database_labels.called is not graph_read


def _load(path: str) -> ReloadStatement:
    return ReloadStatement(discriminator=f"{{{path}}};")


def _store(path: str) -> ReloadStatement:
    return ReloadStatement(discriminator=f"STORE - {path};")


def _unseen_spaces(source: QlikSenseSource) -> List[str]:
    return [
        entry
        for warning in source.reporter.warnings
        if warning.title == "Qlik spaces the API key can't see"
        for entry in warning.context or []
    ]


@pytest.mark.parametrize(
    "script_path, path, browse",
    [
        pytest.param(
            "lib://Hidden:DataFiles/Sub/Other.qvd",
            "lib://Hidden:DataFiles/Sub/prices.qvd",
            ["Hidden", "DataFiles", "Sub"],
            id="same folder",
        ),
        pytest.param(
            # Only the space is spelled by a script.
            "lib://Hidden:Archive/Other.qvd",
            "lib://Hidden:datafiles/sub/prices.qvd",
            ["Hidden", "datafiles", "sub"],
            id="same space",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_named_dataset_path_takes_the_spelling_of_the_scripts(
    mock_api_cls: MagicMock, script_path: str, path: str, browse: List[str]
) -> None:
    # Qlik records the load in lowercase; a load script spells the same space,
    # connection and folder, so the dataset is placed beside the files named
    # from scripts rather than under a lowercase copy of their folders.
    upstreams = LineageUpstreams(resource_labels={UNLISTED_QVD: "Sub/prices.qvd"})
    reader, writer = _reader_and_writer()
    assert isinstance(reader, App)
    reader.script = f"LOAD * FROM [{script_path}] (qvd);"
    source = _source(
        mock_api_cls, DATASETS, items=[reader, writer], upstreams=upstreams
    )
    mock_api_cls.return_value.get_reload_statements.side_effect = lambda app_id: (
        [_load("lib://hidden:datafiles/sub/prices.qvd")]
        if app_id == "report-app"
        else []
    )

    workunits = list(source.get_workunits_internal())

    prices = _urn(UNLISTED_QVD)
    [properties] = [
        p
        for wu in workunits
        if wu.get_urn() == prices
        and (p := wu.get_aspect_of_type(DatasetPropertiesClass))
    ]
    [browse_path] = [
        b
        for wu in workunits
        if wu.get_urn() == prices and (b := wu.get_aspect_of_type(BrowsePathsV2Class))
    ]
    assert properties.qualifiedName == path
    assert [e.id for e in browse_path.path] == browse


@pytest.mark.parametrize(
    "statements, path, browse",
    [
        pytest.param(
            {
                "writer-app": [
                    ReloadStatement(discriminator="RESIDENT Out;"),
                    _store("lib://[Hidden (prod)]:DataFiles/Prices.QVD"),
                    _store("lib://[Hidden (prod)]:DataFiles/Other.qvd"),
                ],
                # Qlik records loads in lowercase.
                "report-app": [_load("lib://hidden (prod):datafiles/prices.qvd")],
            },
            "lib://[Hidden (prod)]:DataFiles/Prices.QVD",
            ["Hidden (prod)", "DataFiles"],
            id="stored and loaded",
        ),
        pytest.param(
            {"report-app": [_load("lib://hidden:datafiles/sub/prices.qvd (qvd)")]},
            "lib://hidden:datafiles/sub/prices.qvd",
            ["hidden", "datafiles", "sub"],
            id="loaded only",
        ),
        pytest.param(
            {
                "report-app": [_load("lib://hidden:datafiles/prices.qvd")],
                # The graph doesn't name this app as the writer.
                "etl-app": [_store("lib://Hidden:DataFiles/Prices.qvd")],
            },
            "lib://Hidden:DataFiles/Prices.qvd",
            ["Hidden", "DataFiles"],
            id="stored by an app the graph doesn't link",
        ),
        pytest.param(
            {
                "writer-app": [_store("lib://Hidden:DataFiles/prices.qvd")],
                "report-app": [_load("lib://archive:datafiles/prices.qvd")],
            },
            None,
            None,
            id="two files of that name",
        ),
        pytest.param(
            {
                "report-app": [
                    _load("lib://hidden:datafiles/archive/prices.qvd"),
                    _load(
                        "lib://hidden:datafiles/sub/prices.qvd "
                        "(ooxml, embedded labels, filters(Remove(Row, Pos(Top, 1))))"
                    ),
                ]
            },
            "lib://hidden:datafiles/sub/prices.qvd",
            ["hidden", "datafiles", "sub"],
            id="the label's folders decide between files of that name",
        ),
        pytest.param(
            {
                "report-app": [
                    _load("lib://hidden:datafiles/prices.qvd"),
                    _load("lib://hidden:datafiles/archive/prices.qvd"),
                ]
            },
            "lib://hidden:datafiles/prices.qvd",
            ["hidden", "datafiles"],
            id="a namesake in a nested folder doesn't match a root label",
        ),
        pytest.param(
            {
                "report-app": [
                    _load("lib://hidden:datafiles/sub/prices.qvd"),
                    _load("lib://hidden:datafiles/archive/sub/prices.qvd"),
                ]
            },
            "lib://hidden:datafiles/sub/prices.qvd",
            ["hidden", "datafiles", "sub"],
            id="the label's folders start at the connection",
        ),
        pytest.param(
            # Qlik drops the brackets around a space name holding a colon.
            {"report-app": [_load("lib://team: sales:datafiles/prices.qvd")]},
            None,
            None,
            id="space and connection can't be told apart",
        ),
        pytest.param(
            {
                "writer-app": [_store("lib://[Team: Sales]:DataFiles/Prices.QVD")],
                "report-app": [_load("lib://team: sales:datafiles/prices.qvd")],
            },
            "lib://[Team: Sales]:DataFiles/Prices.QVD",
            ["Team: Sales", "DataFiles"],
            id="a store tells space and connection apart",
        ),
        pytest.param({}, None, None, id="no file"),
        pytest.param(RuntimeError("Forbidden"), None, None, id="unread"),
        pytest.param(
            {
                "report-app": [_load("lib://hidden:datafiles/prices.qvd")],
                # May hold another file of that name.
                "writer-app": RuntimeError("Forbidden"),
            },
            None,
            None,
            id="one file, but a reload unread",
        ),
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_named_dataset_takes_its_path_from_reload_files(
    mock_api_cls: MagicMock,
    statements: Union[Dict[str, Union[List[ReloadStatement], Exception]], Exception],
    path: Optional[str],
    browse: Optional[List[str]],
) -> None:
    # The graph labels a file with the folders under its connection.
    label = "Sub/Prices.qvd" if path and "/sub/" in path else "prices.qvd"
    upstreams = LineageUpstreams(
        resource_labels={UNLISTED_QVD: label},
        stored_from={
            f"{UNLISTED_QVD}#Prices": [WRITER_TABLE, "qri:app:sense://unread-app#Out"]
        },
    )
    etl_app = _app("etl-app", [])
    if isinstance(statements, dict):
        etl_statements = statements.get("etl-app")
        if isinstance(etl_statements, list):
            etl_app.reload_statements = etl_statements
    source = _source(
        mock_api_cls,
        DATASETS,
        items=[*_reader_and_writer(), etl_app],
        upstreams=upstreams,
    )
    api = mock_api_cls.return_value
    api.spaces = {"space-1": "Finance"}
    unread = isinstance(statements, Exception) or any(
        isinstance(app_statements, Exception) for app_statements in statements.values()
    )

    def get_reload_statements(app_id: str) -> List[ReloadStatement]:
        app_statements = (
            statements
            if isinstance(statements, Exception)
            else statements.get(app_id, [])
        )
        if isinstance(app_statements, Exception):
            raise app_statements
        return app_statements

    api.get_reload_statements.side_effect = get_reload_statements

    workunits = list(source.get_workunits_internal())
    written, added = _lineage(workunits)

    prices = _urn(UNLISTED_QVD)
    properties = next(
        (
            p
            for wu in workunits
            if wu.get_urn() == prices
            and (p := wu.get_aspect_of_type(DatasetPropertiesClass))
        ),
        None,
    )
    browse_path = next(
        (
            b
            for wu in workunits
            if wu.get_urn() == prices
            and (b := wu.get_aspect_of_type(BrowsePathsV2Class))
        ),
        None,
    )
    # The reading table links to the URN, which every reading app shares,
    # whether or not the dataset itself is certain enough to ingest.
    assert written[_urn("report-app.prices")] == [prices]
    if path is None:
        # Without a certain path, nothing makes the dataset exist.
        assert properties is None and browse_path is None
        assert prices not in added
        assert bool(source.reporter.graph_named_datasets_without_path) is not unread
    else:
        assert properties is not None and browse_path is not None
        assert browse is not None
        # The dataset's space is one the key can't see.
        assert _unseen_spaces(source) == [f"{browse[0]}: datasets"]
        assert properties.name == label.split("/")[-1][: -len(".qvd")]
        assert properties.qualifiedName == path
        assert properties.customProperties.get("path") == path
        assert [e.id for e in browse_path.path] == browse
        assert added[prices] == [_urn("writer-app.out")]
    # Only the reading app and the writer apps this run ingests are asked.
    assert sorted(c.args[0] for c in api.get_reload_statements.call_args_list) == [
        "report-app",
        "writer-app",
    ]
    assert bool(source.reporter.apps_without_reload_paths) is unread
    assert (
        any(
            w.title == "Unable to read the file paths of Qlik apps' last reload"
            for w in source.reporter.warnings
        )
        is unread
    )


@pytest.mark.parametrize(
    "discriminator",
    [
        # A file of that name in another folder than the label's.
        "{lib://hidden:datafiles/archive/prices.qvd};",
        "{lib://hidden:datafiles/archive/sub/prices.qvd};",
        "{lib://hidden:datafiles/sub/prices.qvd} unknown form;",
    ],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_graph_named_dataset_without_a_file_at_the_label_has_no_path(
    mock_api_cls: MagicMock, discriminator: str
) -> None:
    upstreams = LineageUpstreams(resource_labels={UNLISTED_QVD: "Sub/Prices.qvd"})
    source = _source(
        mock_api_cls, DATASETS, items=_reader_and_writer(), upstreams=upstreams
    )
    mock_api_cls.return_value.get_reload_statements.side_effect = lambda app_id: (
        [ReloadStatement(discriminator=discriminator)] if app_id == "report-app" else []
    )

    workunits = list(source.get_workunits_internal())

    assert not [
        wu
        for wu in workunits
        if wu.get_urn() == _urn(UNLISTED_QVD)
        and wu.get_aspect_of_type(DatasetPropertiesClass)
    ]
    assert source.reporter.graph_named_datasets_without_path
    # A path in a form the connector doesn't read is reported, not dropped
    # without notice.
    assert bool(source.reporter.unrecognised_reload_files) is (
        "unknown form" in discriminator
    )


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_no_unseen_spaces_when_the_spaces_could_not_be_listed(
    mock_api_cls: MagicMock,
) -> None:
    app = _reload_app([_sql("Hidden Space:Db", "SELECT * FROM dbo.orders")])
    source = _reload_source(mock_api_cls, app)
    api = mock_api_cls.return_value
    api.spaces = {}
    api.unread_items = [UnreadItem(name=UNREAD_SPACES, cause="timed out")]
    api.include_read_failures = ["lib://[Shared Scripts]:DataFiles/setup.qvs: denied"]

    list(source.get_workunits_internal())

    # Every space would look unseen without the listing.
    assert _unseen_spaces(source) == []


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_reloads_are_read_before_files_are_placed(mock_api_cls: MagicMock) -> None:
    # A writer app's STOREs vote on how the space, connection and folders of
    # every file are spelled, so they must be read before any file is placed.
    upstreams = LineageUpstreams(
        resource_labels={UNLISTED_QVD: "prices.qvd"},
        stored_from={f"{UNLISTED_QVD}#Prices": [WRITER_TABLE]},
    )
    source = _source(
        mock_api_cls, DATASETS, items=_reader_and_writer(), upstreams=upstreams
    )
    read_before_placing: List[str] = []

    def unmapped_files(self: QlikSenseSource) -> Iterable[MetadataWorkUnit]:
        read_before_placing.extend(sorted(self._reload_files))
        return []

    with patch.object(QlikSenseSource, "_gen_unmapped_file_entities", unmapped_files):
        list(source.get_workunits_internal())

    assert read_before_placing == ["report-app", "writer-app"]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_spaces_the_key_cant_see_are_named(mock_api_cls: MagicMock) -> None:
    app = _reload_app(
        [
            _sql("Hidden Space:Db", "SELECT * FROM dbo.orders"),
            # A connection in a space the key sees is not about access.
            _sql("Finance:Unknown", "SELECT * FROM dbo.orders"),
            _sql("Finance:[My:Db]", "SELECT * FROM dbo.orders"),
            # Either `Team` or `Team: Sales` may be the space.
            _sql("Team: Sales:Db", "SELECT * FROM dbo.orders"),
        ]
    )
    source = _reload_source(mock_api_cls, app)
    api = mock_api_cls.return_value
    api.spaces = {"space-1": "Finance"}
    api.include_read_failures = [
        "lib://[Shared Scripts]:DataFiles/setup.qvs (app etl-app in space "
        "Finance): Access denied",
        "lib://finance:DataFiles/vars.qvs (app etl-app in space Finance): "
        "no script text returned",
        # The space's name holds the `: ` that also ends the entry's path.
        "lib://[Team: Ops]:DataFiles/env.qvs (app etl-app in space Finance): "
        "Access denied",
    ]

    list(source.get_workunits_internal())

    assert _unseen_spaces(source) == [
        "Hidden Space: data connections, Shared Scripts: included scripts, "
        "Team: Ops: included scripts"
    ]


def test_field_source_never_picks_between_columns_differing_in_case() -> None:
    refs = [
        ColumnRef(table=SHOP_ORDERS, column="order_id"),
        ColumnRef(table=SHOP_ORDERS, column="Order_Id"),
    ]
    field_id = _field_qri("t", "ORDER_ID").rpartition("#")[2]

    assert _refs_of_field_sources(refs, "ORDER_ID", {field_id}) == []
    assert _refs_of_field_sources(refs[:1], "ORDER_ID", {field_id}) == refs[:1]
