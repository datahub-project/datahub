from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import requests_mock as requests_mock_module

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.qlik_sense.config import QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    LineageTable,
    ListedDataset,
    QlikDataset,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_sense import QlikSenseSource
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.metadata.schema_classes import UpstreamLineageClass

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
                "nodes": {f"{REPORT_APP}#Tbl": {"label": "Prices"}},
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
    assert api.get_table_lineage(ETL_APP) == []


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


def _run(
    mock_api_cls: MagicMock, datasets: Optional[List[ListedDataset]]
) -> Tuple[QlikSenseSource, Dict[str, List[str]]]:
    space = MagicMock(id="space-1")
    space.name = "Finance"
    api = mock_api_cls.return_value
    api.get_spaces.return_value = [space]
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
    source = QlikSenseSource(
        QlikSourceConfig.model_validate(CLOUD_CONFIG),
        PipelineContext(run_id="test-lineage-graph"),
    )
    upstreams: Dict[str, List[str]] = {}
    for wu in source.get_workunits_internal():
        lineage = wu.get_aspect_of_type(UpstreamLineageClass)
        if lineage:
            upstreams[wu.get_urn()] = [u.dataset for u in lineage.upstreams]
    return source, upstreams


DATASETS = [
    ListedDataset(space_id="space-1", name="src.qvd", secure_qri=SOURCE_QVD),
    ListedDataset(space_id="space-1", name="stored.qvd", secure_qri=STORED_QVD),
    # In a space the recipe leaves out, so it is never ingested.
    ListedDataset(space_id="space-2", name="hidden.qvd", secure_qri=HIDDEN_QVD),
]


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_qvd_lineage_links_apps_through_datasets(mock_api_cls: MagicMock) -> None:
    source, upstreams = _run(mock_api_cls, DATASETS)

    curve = _urn(f"{ETL_APP}#Curve")
    assert upstreams == {
        curve: [_urn("space-1.src.qvd")],
        _urn(f"{REPORT_APP}#Tbl"): [_urn("space-1.stored.qvd")],
        _urn("space-1.stored.qvd"): [curve],
    }
    assert not source.reporter.failures


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_script_table_in_lineage_graph_keeps_its_urn(mock_api_cls: MagicMock) -> None:
    # Script tables were ingested as `{app_id}.{label}` before the lineage graph
    # was read; moving them to the graph QRI would soft-delete the old entity.
    script_table = ScriptTableLineage(
        label="Curve", identifier="etl-app.curve", upstream_urns=[POSTGRES_ORDERS]
    )
    with patch.object(
        QlikSenseSource,
        "_build_script_tables",
        lambda self, app: [script_table] if app.id == "etl-app" else [],
    ):
        _, upstreams = _run(mock_api_cls, DATASETS)

    curve = _urn("etl-app.curve")
    assert upstreams[curve] == [_urn("space-1.src.qvd"), POSTGRES_ORDERS]
    assert upstreams[_urn("space-1.stored.qvd")] == [curve]
    assert _urn(f"{ETL_APP}#Curve") not in upstreams


@patch("datahub.ingestion.source.qlik_sense.qlik_sense.QlikAPI")
def test_failed_dataset_listing_keeps_table_lineage(mock_api_cls: MagicMock) -> None:
    source, upstreams = _run(mock_api_cls, None)

    # Both tables load datasets, so their lineage from the last run is kept
    # rather than replaced by lineage without the dataset edges.
    assert _urn(f"{ETL_APP}#Curve") not in upstreams
    assert _urn(f"{REPORT_APP}#Tbl") not in upstreams
    assert [w.title for w in source.reporter.warnings] == [
        "Lineage of Qlik app table not updated"
    ]
