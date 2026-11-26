"""The two table listers degrade to empty on some statuses; a caller that asks
must be told, because empty is otherwise indistinguishable from 'no tables'."""

import logging
from typing import List, Tuple
from unittest.mock import patch

import pytest
import requests

from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from tests.unit.fabric_common.conftest import fabric_response, specced_auth_helper


def test_warehouse_404_is_reported_to_the_caller(
    onelake_client: OneLakeClient,
) -> None:
    client = onelake_client
    seen: List[str] = []

    with patch.object(client._session, "request", return_value=fabric_response(404)):
        tables = list(client.list_warehouse_tables("ws", "wh", on_degraded=seen.append))

    assert tables == []
    assert len(seen) == 1 and "404" in seen[0]


def test_schemas_enabled_lakehouse_403_is_reported_to_the_caller(
    onelake_client: OneLakeClient,
) -> None:
    client = onelake_client
    seen: List[str] = []

    # GET lakehouse properties says schemas are enabled, then the OneLake
    # Delta API (called through session.get) refuses.
    with (
        patch.object(
            client._session,
            "request",
            return_value=fabric_response(
                200, b'{"properties": {"defaultSchema": "dbo"}}'
            ),
        ),
        patch.object(
            client._session,
            "get",
            return_value=fabric_response(
                403, b'{"error": "placeholder-token in body"}'
            ),
        ),
    ):
        tables = list(client.list_lakehouse_tables("ws", "lh", on_degraded=seen.append))

    assert tables == []
    assert len(seen) == 1 and "403" in seen[0]
    # The response body can echo request details; the message carries the
    # status and operation only.
    assert "placeholder-token" not in seen[0]


def test_ingestion_callers_still_get_a_silent_empty_result(
    onelake_client: OneLakeClient,
) -> None:
    client = onelake_client

    with patch.object(client._session, "request", return_value=fabric_response(404)):
        assert list(client.list_warehouse_tables("ws", "wh")) == []


_SENTINEL = "planted-sentinel-token"


def _lakehouse_delta_api_failing(
    status: int, failing_call: int
) -> Tuple[OneLakeClient, List[requests.Response]]:
    """A schemas-enabled lakehouse whose Delta API call number `failing_call`
    (0 = list schemas, 1 = list tables) returns `status` with the sentinel."""
    client = OneLakeClient(auth_helper=specced_auth_helper())
    responses = [
        fabric_response(200, b'{"schemas": [{"name": "dbo"}]}'),
        fabric_response(200, b'{"tables": []}'),
    ]
    responses[failing_call] = fabric_response(
        status, ('{"error": "' + _SENTINEL + '"}').encode()
    )
    return client, responses


def test_delta_api_error_bodies_never_reach_the_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    # 403 on schemas (the swallowed branch), 500 on schemas and on tables
    # (the raised branches).
    for status, failing_call, raises in (
        (403, 0, False),
        (500, 0, True),
        (500, 1, True),
    ):
        client, responses = _lakehouse_delta_api_failing(status, failing_call)
        with (
            patch.object(
                client._session,
                "request",
                return_value=fabric_response(
                    200, b'{"properties": {"defaultSchema": "dbo"}}'
                ),
            ),
            patch.object(client._session, "get", side_effect=responses),
        ):
            if raises:
                with pytest.raises(requests.HTTPError):
                    list(client.list_lakehouse_tables("ws", "lh"))
            else:
                assert list(client.list_lakehouse_tables("ws", "lh")) == []
    assert caplog.records
    assert _SENTINEL not in caplog.text
