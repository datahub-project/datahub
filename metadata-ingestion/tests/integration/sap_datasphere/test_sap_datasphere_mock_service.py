import json
from pathlib import Path
from typing import Iterator
from urllib.parse import parse_qs

import pytest
import time_machine
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WerkzeugRequest, Response as WerkzeugResponse

from datahub.ingestion.run.pipeline import Pipeline

FROZEN_TIME = "2024-01-15 12:00:00+00:00"
FIXTURES_DIR = Path(__file__).parent / "fixtures"
# Host baked into the fixture's assetRelationalMetadataUrl fields; rewritten to
# the mock server's URL at runtime.
FIXTURE_TENANT_HOST = "https://test.eu10.hcs.cloud.sap"


pytestmark = pytest.mark.integration_batch_0


def _fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


def _assets_payload_rewritten(base_url: str) -> str:
    return _fixture_text("assets_lineage_test.json").replace(
        FIXTURE_TENANT_HOST, base_url
    )


def _csn_for_technical_name(technical_name: str) -> dict:
    """CSN with columns[] so the column-lineage extractor has work; shaped like the per-object-type endpoint response."""
    if technical_name == "BASE_TABLE":
        return {"definitions": {"BASE_TABLE": {"kind": "entity"}}}
    if technical_name == "MID_VIEW":
        return {
            "definitions": {
                "MID_VIEW": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_TABLE"]},
                            "columns": [
                                {"ref": ["ID"]},
                                {"ref": ["NAME"]},
                                {
                                    "func": "SUM",
                                    "args": [{"ref": ["AMOUNT"]}],
                                    "as": "total_amount",
                                },
                            ],
                        }
                    },
                }
            }
        }
    if technical_name == "FED_SNOWFLAKE_CUST":
        return {
            "definitions": {
                "FED_SNOWFLAKE_CUST": {
                    "kind": "entity",
                    "@remote.source": "SNOWFLAKE_PROD",
                    "@DataWarehouse.external.schema": "PUBLIC",
                }
            }
        }
    if technical_name == "AM_REVENUE":
        return {
            "definitions": {
                "AM_REVENUE": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_TABLE"], "as": "F"},
                            "columns": [{"ref": ["REVENUE"]}],
                            "distinct": False,
                        }
                    },
                }
            },
            "businessLayerDefinitions": {
                "AM_REVENUE": {
                    "sourceModel": {
                        "factSources": {
                            "F": {
                                "text": "Revenue facts",
                                "dataEntity": {"key": "LINEAGE_TEST.BASE_TABLE"},
                            }
                        },
                        "dimensionSources": {
                            "_REGION_DIM": {
                                "text": "Region dimension",
                                "dataEntity": {"key": "LINEAGE_TEST.MID_VIEW"},
                            }
                        },
                    },
                    "measures": {"REVENUE": {"isAuxiliary": False}},
                    "attributes": {"REGION": {}},
                    "variables": {"P_DATE": {}},
                }
            },
        }
    return {"definitions": {}}


@pytest.fixture
def sap_mock_service(httpserver: HTTPServer) -> Iterator[str]:
    base_url = httpserver.url_for("").rstrip("/")

    httpserver.expect_request(
        "/api/v1/datasphere/consumption/catalog/spaces",
        method="GET",
    ).respond_with_data(
        _fixture_text("spaces_lineage.json"),
        content_type="application/json",
    )

    # Metadata URLs are rewritten to point at the mock server.
    httpserver.expect_request(
        "/api/v1/datasphere/consumption/catalog/spaces('LINEAGE_TEST')/assets",
        method="GET",
    ).respond_with_data(
        _assets_payload_rewritten(base_url),
        content_type="application/json",
    )

    httpserver.expect_request(
        "/api/v1/datasphere/spaces/LINEAGE_TEST/connections",
        method="GET",
    ).respond_with_data(
        _fixture_text("connections_lineage.json"),
        content_type="application/json",
    )

    httpserver.expect_request(
        "/edmx/LINEAGE_TEST/BASE_TABLE/$metadata",
        method="GET",
    ).respond_with_data(
        _fixture_text("base_table.xml"),
        content_type="application/xml",
    )

    httpserver.expect_request(
        "/edmx/LINEAGE_TEST/MID_VIEW/$metadata",
        method="GET",
    ).respond_with_data(
        _fixture_text("mid_view.xml"),
        content_type="application/xml",
    )

    httpserver.expect_request(
        "/edmx/LINEAGE_TEST/AM_REVENUE/$metadata",
        method="GET",
    ).respond_with_data(
        _fixture_text("am_revenue.xml"),
        content_type="application/xml",
    )

    # Non-analytical assets route to /views/.
    for technical_name in (
        "BASE_TABLE",
        "MID_VIEW",
        "FED_SNOWFLAKE_CUST",
        "FED_UNSUPPORTED",
    ):
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/LINEAGE_TEST/views/{technical_name}",
            method="GET",
        ).respond_with_data(
            json.dumps(_csn_for_technical_name(technical_name)),
            status=200,
            content_type="application/json",
        )

    # AM_REVENUE (supportsAnalyticalQueries=true) routes to /analyticmodels/.
    httpserver.expect_request(
        "/dwaas-core/api/v1/spaces/LINEAGE_TEST/analyticmodels/AM_REVENUE",
        method="GET",
    ).respond_with_data(
        json.dumps(_csn_for_technical_name("AM_REVENUE")),
        status=200,
        content_type="application/json",
    )

    yield base_url
    httpserver.check_assertions()


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_datasphere_against_mock_service_emits_column_lineage(
    pytestconfig: pytest.Config,
    tmp_path: Path,
    sap_mock_service: str,
) -> None:
    """Drive the pipeline through a real HTTP wire, not just the requests-mock level."""
    output_file = tmp_path / "sap_datasphere_mock_service_mces.json"

    pipeline_config = {
        "run_id": "sap-datasphere-mock-service-test",
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": sap_mock_service,
                "token": "test-token",
                "env": "PROD",
                "platform_instance": "test_tenant",
                "include_lineage": True,
                # Exercise parallel asset processing through real HTTP.
                "max_workers_assets": 4,
                # Managed assets always resolve to `sap-datasphere` with the
                # top-level platform_instance; this map only routes federated
                # (non-managed) connections.
                "connection_to_platform_map": {},
                "platform_type_defaults": {
                    "SNOWFLAKE": {
                        "platform": "snowflake",
                        "platform_instance": "prod_snowflake",
                        "convert_urns_to_lowercase": True,
                    },
                },
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

    events = json.loads(output_file.read_text())

    # Structural assertions (the golden lives in the federated golden test).
    assert events, "Pipeline produced no events"

    dataset_events = [
        e for e in events if "urn:li:dataset:" in (e.get("entityUrn") or "")
    ]
    assert dataset_events, "No dataset events emitted"

    # Datasets land under sap-datasphere (managed — incl. FED_UNSUPPORTED, whose
    # mock CSN omits @remote.source) or snowflake (FED_SNOWFLAKE_CUST).
    for e in dataset_events:
        urn = e["entityUrn"]
        assert (
            "urn:li:dataPlatform:sap-datasphere" in urn
            or "urn:li:dataPlatform:snowflake" in urn
        ), f"Dataset URN should be under sap-datasphere/snowflake platform, got: {urn}"

    upstream_events = [e for e in events if e.get("aspectName") == "upstreamLineage"]
    assert upstream_events, "No upstreamLineage aspects emitted"

    fine_grained_events = [
        e
        for e in upstream_events
        if e.get("aspect", {}).get("json", {}).get("fineGrainedLineages")
    ]
    assert fine_grained_events, (
        "No fineGrainedLineages — column lineage path didn't fire"
    )

    all_fg = [
        fg
        for e in fine_grained_events
        for fg in e["aspect"]["json"]["fineGrainedLineages"]
    ]
    assert any(fg.get("transformOperation") == "AGGREGATE" for fg in all_fg), (
        "Expected at least one AGGREGATE transformOperation"
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_datasphere_against_mock_service_handles_oauth_refresh_on_401(
    tmp_path: Path,
    httpserver: HTTPServer,
) -> None:
    """The client refreshes the OAuth bearer and retries when spaces returns 401 once."""
    base_url = httpserver.url_for("").rstrip("/")

    xsuaa_calls = []

    def _xsuaa_handler(request: WerkzeugRequest) -> WerkzeugResponse:
        body = parse_qs(request.get_data(as_text=True))
        xsuaa_calls.append(body)
        return WerkzeugResponse(
            json.dumps(
                {
                    "access_token": f"fresh-token-{len(xsuaa_calls)}",
                    "expires_in": 3600,
                }
            ),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_request(
        "/oauth/token",
        method="POST",
    ).respond_with_handler(_xsuaa_handler)

    call_count = {"n": 0}

    def _spaces_handler(request: WerkzeugRequest) -> WerkzeugResponse:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return WerkzeugResponse("Unauthorized", status=401)
        return WerkzeugResponse(
            _fixture_text("spaces_lineage.json"),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_request(
        "/api/v1/datasphere/consumption/catalog/spaces",
        method="GET",
    ).respond_with_handler(_spaces_handler)

    # Empty assets list keeps the test focused on the 401 retry path.
    httpserver.expect_request(
        "/api/v1/datasphere/consumption/catalog/spaces('LINEAGE_TEST')/assets",
        method="GET",
    ).respond_with_data(
        json.dumps({"value": []}),
        content_type="application/json",
    )

    httpserver.expect_request(
        "/api/v1/datasphere/spaces/LINEAGE_TEST/connections",
        method="GET",
    ).respond_with_data(
        _fixture_text("connections_lineage.json"),
        content_type="application/json",
    )

    output_file = tmp_path / "out.json"
    pipeline_config = {
        "run_id": "sap-oauth-refresh-test",
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": base_url,
                "refresh_token": "rt-xyz",
                "client_id": "cid",
                "xsuaa_url": base_url,
                "env": "PROD",
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }

    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.raise_from_status()

    assert len(xsuaa_calls) >= 2, (
        f"Expected >=2 XSUAA token calls (initial + refresh on 401), "
        f"got {len(xsuaa_calls)}"
    )
    first_body = xsuaa_calls[0]
    assert first_body.get("grant_type") == ["refresh_token"], (
        f"First XSUAA call should be refresh_token grant, got: {first_body}"
    )
    assert first_body.get("refresh_token") == ["rt-xyz"]
    assert first_body.get("client_id") == ["cid"]
    assert call_count["n"] == 2, (
        f"Expected spaces endpoint called twice (401 retry), got {call_count['n']}"
    )

    httpserver.check_assertions()
