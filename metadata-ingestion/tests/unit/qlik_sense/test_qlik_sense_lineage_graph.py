import base64
import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pytest
import requests_mock as requests_mock_module

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
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
    SchemaField,
    UnchangedApp,
    UnreadItem,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    FineGrainedLineageClass,
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
    api.field_lineage_failures = []
    api.opened_without_data = []
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
    "had_tables, checkpoint_failed, read_complete",
    [(True, False, False), (False, False, True), (False, True, False)],
)
@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_missing_lineage_graph(
    mock_api_cls: MagicMock,
    had_tables: bool,
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
    assert list(source.reporter.apps_without_lineage_graph) == (
        ["etl-app"] if read_complete else []
    )


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
    assert source.app_state.record.call_args.args[1].table_urns_by_qri is None
