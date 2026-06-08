"""Recorded-replay integration test: the closest-to-real end-to-end test.

A mock SAP Datasphere is stood up from REAL captured API responses (a SAP-
delivered demo space from a trial tenant) and the connector is driven
through it via a real in-process HTTP server (pytest_httpserver). Unlike the
sibling ``test_sap_datasphere_mock_service.py`` — which uses synthetic CSN
payloads — every response body here is a static fixture derived from a live
tenant capture (catalog spaces/assets/connections, per-view CSN, EDMX XML, and
local-table CSN). The output is asserted against a golden built from those real
payload shapes.

The pipeline is configured with NO pre-set ``token`` / ``refresh_token`` —
only ``client_id`` + ``client_secret`` + ``xsuaa_url`` — so the connector is
forced through its full cold-start ``client_credentials`` OAuth grant (
``_fetch_client_credentials_token``) before any data call. This exercises the
previously-untested cold-start grant: the test asserts the token endpoint was
hit with ``grant_type=client_credentials`` and that the fetched bearer is
actually attached to subsequent data requests.
"""

import json
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs

import pytest
import time_machine
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WerkzeugRequest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers
from tests.integration.sap_datasphere.mock_datasphere_server import (
    LOCAL_TABLE_NAMES,
    RECORDED_TOKEN,
    VIEW_NAMES,
    register_handlers,
)

FROZEN_TIME = "2024-01-15 12:00:00+00:00"

SPACE = "DEMO_SPACE"


pytestmark = pytest.mark.integration_batch_0


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_datasphere_recorded_replay_cold_start_oauth(
    pytestconfig: pytest.Config,
    tmp_path: Path,
    httpserver: HTTPServer,
) -> None:
    base_url = httpserver.url_for("").rstrip("/")

    # The mock wiring (token endpoint + all data endpoints) lives in the
    # shared mock_datasphere_server module so the standalone dev server and
    # this test register exactly the same handlers (DRY). The test layers its
    # extra assertions on top via the on_token / on_request spying hooks:
    #
    #   * on_token captures every /oauth/token POST body so we can assert the
    #     cold-start client_credentials grant fired with the right creds.
    #   * on_request captures the Authorization header on catalog-spaces calls
    #     to prove the freshly-fetched bearer is actually used on data calls.
    token_calls: List[Dict[str, List[str]]] = []
    seen_auth_headers: List[str] = []

    def _on_token(request: WerkzeugRequest) -> None:
        token_calls.append(parse_qs(request.get_data(as_text=True)))

    def _on_request(request: WerkzeugRequest) -> None:
        seen_auth_headers.append(request.headers.get("Authorization", ""))

    register_handlers(
        httpserver,
        base_url,
        on_token=_on_token,
        on_request=_on_request,
    )

    output_file = tmp_path / "sap_datasphere_recorded_replay_mces.json"
    golden_file = (
        Path(__file__).parent / "sap_datasphere_recorded_replay_mces_golden.json"
    )

    pipeline_config = {
        "run_id": "sap-datasphere-recorded-replay",
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": base_url,
                # NO token / refresh_token -> forces the cold-start
                # client_credentials grant via _fetch_client_credentials_token.
                "client_id": "recorded-cid",
                "client_secret": "recorded-secret",
                "xsuaa_url": base_url,
                "platform_instance": "recorded_tenant",
                "include_lineage": True,
                "include_view_definitions": True,
                "include_local_tables": True,
                "stateful_ingestion": {"enabled": False},
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(output_file)},
        },
    }

    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.raise_from_status()

    # --- Assertion: cold-start client_credentials grant actually fired. ---
    assert token_calls, "XSUAA /oauth/token was never called (cold-start OAuth)"
    first = token_calls[0]
    assert first.get("grant_type") == ["client_credentials"], (
        f"First token call should be a client_credentials grant, got: {first}"
    )
    assert first.get("client_id") == ["recorded-cid"]
    assert first.get("client_secret") == ["recorded-secret"]

    # --- Assertion: the fetched bearer is attached to data requests. ---
    assert seen_auth_headers, "Spaces endpoint was never called"
    assert any(h == f"Bearer {RECORDED_TOKEN}" for h in seen_auth_headers), (
        "No data request carried the freshly-fetched bearer token; "
        f"saw Authorization headers: {seen_auth_headers}"
    )

    # --- Belt-and-suspenders entity assertions on top of the golden. ---
    events = json.loads(output_file.read_text())
    assert events, "Pipeline produced no events"

    dataset_urns = {
        e["entityUrn"]
        for e in events
        if "urn:li:dataset:" in (e.get("entityUrn") or "")
    }
    # All managed (no @remote.source in the captured CSN) -> sap-datasphere.
    for urn in dataset_urns:
        assert "urn:li:dataPlatform:sap-datasphere" in urn, (
            f"Recorded demo content is managed; expected sap-datasphere URN, got {urn}"
        )

    # URNs lowercase the technical name; match case-insensitively.
    lower_urns = {urn.lower() for urn in dataset_urns}
    for name in VIEW_NAMES + LOCAL_TABLE_NAMES:
        assert any(name.lower() in urn for urn in lower_urns), (
            f"Expected a sap-datasphere dataset for {name}; urns: {sorted(dataset_urns)}"
        )

    upstream_events = [e for e in events if e.get("aspectName") == "upstreamLineage"]
    assert upstream_events, (
        "No upstreamLineage emitted — view->local-table lineage path didn't fire"
    )

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )

    httpserver.check_assertions()
