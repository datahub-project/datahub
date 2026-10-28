from typing import Dict, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.fabric.onelake.client import OneLakeClient


def _page(body: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = body
    return response


def test_paginate_stops_on_a_repeated_continuation_token() -> None:
    client = OneLakeClient.__new__(OneLakeClient)
    pages = [
        _page({"value": [{"id": "a"}], "continuationToken": "t1"}),
        _page({"value": [{"id": "b"}], "continuationToken": "t1"}),
    ]
    params_seen: List[Dict[str, str]] = []

    def get(endpoint: str, params: Optional[Dict[str, str]] = None) -> MagicMock:
        params_seen.append(dict(params or {}))
        return pages.pop(0)

    client.get = get  # type: ignore[method-assign]

    items = list(client._paginate("workspaces"))

    assert items == [{"id": "a"}, {"id": "b"}]
    assert params_seen == [{}, {"continuationToken": "t1"}]
