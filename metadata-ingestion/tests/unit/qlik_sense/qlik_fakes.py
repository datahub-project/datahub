from typing import Dict, List, Optional

import requests

ON_PREM = {
    "server": "qlik.example.com",
    "client_cert": "/secrets/client.pem",
    "client_key": "/secrets/client_key.pem",
    "root_cert": "/secrets/root.pem",
    # The Engine fakes don't model the data model read; it has its own test.
    "read_data_model": False,
}
ONPREM_CONFIG = {"on_prem": ON_PREM}

STREAM = {
    "id": "stream-1",
    "name": "Everyone",
    "createdDate": "2024-01-09T18:05:36Z",
    "modifiedDate": "2024-01-15T11:07:00.333Z",
    "owner": {"id": "user-1", "name": "Ada", "userId": "ada"},
}

APP = {
    "id": "app-1",
    "name": "Sales",
    "description": "",
    "createdDate": "2024-01-09T18:05:36.545Z",
    "modifiedDate": "2024-01-15T11:07:00.333Z",
    "stream": {"id": "stream-1", "name": "Everyone"},
    "owner": {"id": "user-1", "name": "Ada", "userId": "ada"},
}

SHEET_META = {
    "title": "Overview",
    "description": "",
    "owner": {"id": "user-1", "name": "Ada", "userId": "ada"},
    "createdDate": "2024-01-15T11:01:49.704Z",
    "modifiedDate": "2024-01-29T12:23:46.868Z",
}


LOAD_SCRIPT = """
LIB CONNECT TO 'Sales SQL';
Orders:
SQL SELECT order_id, amount FROM dbo.orders;
"""

DATA_CONNECTION = {
    "name": "Sales SQL",
    "connectionstring": (
        "OLEDB CONNECT TO [Provider=MSOLEDBSQL;Data Source=db.example.com;"
        "Initial Catalog=sales_db]"
    ),
    "type": "OLEDB",
}


class FakeResponse:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = requests.exceptions.HTTPError(f"{self.status_code} Client Error")
            error.response = self  # type: ignore[assignment]
            raise error


def fake_qrs_get(
    url: str, params: Optional[Dict[str, str]] = None, **_: object
) -> FakeResponse:
    assert params is not None
    assert len(params["xrfkey"]) == 16
    if url.endswith("/about"):
        return FakeResponse({"buildVersion": "14.129.3"})
    if url.endswith("/stream/full"):
        return FakeResponse([STREAM])
    if url.endswith("/app/full"):
        assert params["filter"] == "stream.id eq stream-1"
        return FakeResponse([APP])
    if url.endswith("/dataconnection/full"):
        return FakeResponse([DATA_CONNECTION])
    raise AssertionError(url)


def fake_engine_calls() -> List[Dict[str, object]]:
    return [
        {"qReturn": {"qHandle": 1}},
        {"qList": [{"qInfo": {"qId": "sheet-1", "qType": "sheet"}}]},
        {"qReturn": {"qHandle": 2, "qType": "GenericObject"}},
        {
            "qLayout": {
                "qMeta": SHEET_META,
                "qChildList": {"qItems": []},
            }
        },
    ]
