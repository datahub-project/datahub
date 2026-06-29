"""Standalone runnable mock SAP Datasphere built from recorded API captures.

This module serves the same REAL captured tenant responses that the
recorded-replay integration test uses (catalog spaces/assets/connections,
per-view CSN, EDMX XML, local-table list + per-table CSN) plus a permissive
``/oauth/token`` endpoint, so the full connector path — including the OAuth
cold-start — can be exercised without a real SAP tenant.

Two ways to use it:

* As a library, from the recorded-replay test: call ``register_handlers`` to
  wire every endpoint onto a ``pytest_httpserver.HTTPServer`` (the test layers
  its own spying assertions on top via the ``on_token`` / ``on_request`` hooks).

* As a standalone dev server, from the command line::

      python tests/integration/sap_datasphere/mock_datasphere_server.py --sink http://localhost:8080
      datahub ingest -c /tmp/sap_datasphere_mock_recipe.yml

  It boots a real in-process HTTP server, writes a ready-to-run ingestion
  recipe, and blocks until Ctrl-C. A developer can then point ``datahub
  ingest`` (CLI) or the DataHub UI ingestion source at it.
"""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Optional

if TYPE_CHECKING:
    from pytest_httpserver import HTTPServer
    from werkzeug.wrappers import Request as WerkzeugRequest

RECORDED_DIR = Path(__file__).parent / "fixtures" / "recorded"

# The placeholder host baked into the committed assets fixture's
# assetRelationalMetadataUrl fields — rewritten to the mock server URL at
# runtime. The real tenant host was scrubbed to this placeholder when
# the fixture was derived from the capture.
RECORDED_TENANT_HOST = "https://RECORDED_TENANT"

SPACE = "DEMO_SPACE"
RECORDED_TOKEN = "recorded-replay-token"

# The real SAP-delivered demo content captured from the tenant.
VIEW_NAMES = [
    "SAP.TIME.VIEW_DIMENSION_DAY",
    "SAP.TIME.VIEW_DIMENSION_MONTH",
    "SAP.TIME.VIEW_DIMENSION_QUARTER",
    "SAP.TIME.VIEW_DIMENSION_YEAR",
]
LOCAL_TABLE_NAMES = [
    "SAP.TIME.M_TIME_DIMENSION",
    "SAP.TIME.M_TIME_DIMENSION_TDAY",
    "SAP.TIME.M_TIME_DIMENSION_TMONTH",
    "SAP.TIME.M_TIME_DIMENSION_TQUARTER",
]
# Analytic Models (supportsAnalyticalQueries -> /analyticmodels/). Their CSN
# carries businessLayerDefinitions (fact/dimension sources) for star-schema lineage.
ANALYTIC_MODEL_NAMES = [
    "Test_Analytic_Model",
]


def _fixture_text(rel: str) -> str:
    return (RECORDED_DIR / rel).read_text()


def _assets_payload_rewritten(base_url: str) -> str:
    """Point the captured assetRelationalMetadataUrl fields at the mock server."""
    return _fixture_text("catalog_assets.json").replace(RECORDED_TENANT_HOST, base_url)


def register_handlers(
    httpserver: "HTTPServer",
    base_url: str,
    on_token: Optional[Callable[["WerkzeugRequest"], None]] = None,
    on_request: Optional[Callable[["WerkzeugRequest"], None]] = None,
) -> None:
    """Register every recorded-replay handler onto ``httpserver``.

    Wires the permissive OAuth token endpoint plus all data endpoints
    (catalog spaces / assets / connections, per-view EDMX + CSN, local-table
    list + per-table CSN) from the recorded fixtures.

    Optional spying hooks (used by the test, no-ops for the standalone server):

    * ``on_token`` — called with the werkzeug request for every
      ``/oauth/token`` POST (e.g. to capture the grant_type / client creds).
    * ``on_request`` — called with the werkzeug request for every
      catalog-spaces GET (e.g. to capture the Authorization header and prove
      the freshly-fetched bearer is actually used on data calls).
    """
    # Imported lazily so this module is importable (e.g. for the test) even if
    # the optional werkzeug dep isn't present; register_handlers needs it.
    from werkzeug.wrappers import (
        Request as WerkzeugRequest,
        Response as WerkzeugResponse,
    )

    # --- XSUAA token endpoint: permissive for ANY grant so the server works
    # for every auth mode (client_credentials cold-start, refresh_token, ...).
    # A standalone dev server must not assert grant types — that assertion
    # lives in the test, layered on via on_token. ---
    def _token_handler(request: WerkzeugRequest) -> WerkzeugResponse:
        if on_token is not None:
            on_token(request)
        return WerkzeugResponse(
            json.dumps(
                {
                    "access_token": RECORDED_TOKEN,
                    "expires_in": 3600,
                    "token_type": "bearer",
                }
            ),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_request("/oauth/token", method="POST").respond_with_handler(
        _token_handler
    )

    # --- Catalog spaces. ---
    def _spaces_handler(request: WerkzeugRequest) -> WerkzeugResponse:
        if on_request is not None:
            on_request(request)
        return WerkzeugResponse(
            _fixture_text("catalog_spaces.json"),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_request(
        "/api/v1/datasphere/consumption/catalog/spaces",
        method="GET",
    ).respond_with_handler(_spaces_handler)

    # --- Catalog assets (URLs rewritten to the mock server). ---
    httpserver.expect_request(
        f"/api/v1/datasphere/consumption/catalog/spaces('{SPACE}')/assets",
        method="GET",
    ).respond_with_data(
        _assets_payload_rewritten(base_url),
        content_type="application/json",
    )

    # --- Connections list (bare JSON array). ---
    httpserver.expect_request(
        f"/api/v1/datasphere/spaces/{SPACE}/connections",
        method="GET",
    ).respond_with_data(
        _fixture_text("connections.json"),
        content_type="application/json",
    )

    # --- EDMX per view (assetRelationalMetadataUrl path). ---
    for name in VIEW_NAMES:
        httpserver.expect_request(
            f"/api/v1/dwc/consumption/relational/{SPACE}/{name}/$metadata",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"edmx/{name}.xml"),
            content_type="application/xml",
        )

    # --- Per-view CSN (all four views are non-analytical -> /views/). ---
    for name in VIEW_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/views/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"views/{name}.json"),
            content_type="application/json",
        )

    # --- Per-analytic-model CSN (supportsAnalyticalQueries -> /analyticmodels/). ---
    for name in ANALYTIC_MODEL_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/analyticmodels/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"analyticmodels/{name}.json"),
            content_type="application/json",
        )

    # --- Local tables list. ---
    httpserver.expect_request(
        f"/dwaas-core/api/v1/spaces/{SPACE}/localtables",
        method="GET",
    ).respond_with_data(
        _fixture_text("localtables.json"),
        content_type="application/json",
    )

    # --- Per-local-table CSN. ---
    for name in LOCAL_TABLE_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/localtables/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"localtables/{name}.json"),
            content_type="application/json",
        )


def build_recipe(base_url: str, sink_url: Optional[str] = None) -> str:
    """Return a ready-to-run DataHub ingestion recipe YAML string.

    The source points ``base_url`` / ``client_id`` / ``client_secret`` /
    ``xsuaa_url`` all at the mock server (forcing a client_credentials
    cold-start through the permissive token endpoint). When ``sink_url`` is
    given the sink is a ``datahub-rest`` to it; otherwise a ``file`` sink with
    a comment showing how to switch to datahub-rest.
    """
    if sink_url is not None:
        sink_block = (
            "sink:\n"
            "  type: datahub-rest\n"
            "  config:\n"
            f"    server: {sink_url}\n"
            "    # token: <your-datahub-pat>  # if your DataHub requires auth\n"
        )
    else:
        sink_block = (
            "sink:\n"
            "  type: file\n"
            "  config:\n"
            "    filename: /tmp/sap_datasphere_mock_out.json\n"
            "  # To push into a running DataHub instead, swap the sink for:\n"
            "  #   type: datahub-rest\n"
            "  #   config:\n"
            "  #     server: http://localhost:8080\n"
        )

    return (
        "# Auto-generated recipe for the standalone mock SAP Datasphere server.\n"
        "# Serves real recorded tenant responses + a permissive\n"
        "# /oauth/token, so the full connector path (incl. OAuth) runs without\n"
        "# a real SAP tenant.\n"
        "source:\n"
        "  type: sap-datasphere\n"
        "  config:\n"
        f"    base_url: {base_url}\n"
        "    # No token / refresh_token -> forces the cold-start\n"
        "    # client_credentials OAuth grant against the mock token endpoint.\n"
        "    client_id: recorded-cid\n"
        "    client_secret: recorded-secret\n"
        f"    xsuaa_url: {base_url}\n"
        "    platform_instance: recorded_tenant\n"
        "    include_lineage: true\n"
        "    include_view_definitions: true\n"
        "    include_local_tables: true\n"
        "    stateful_ingestion:\n"
        "      enabled: false\n"
        f"{sink_block}"
    )


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    import time

    parser = argparse.ArgumentParser(
        description=(
            "Standalone mock SAP Datasphere server. Serves real recorded "
            "tenant responses + a permissive /oauth/token so 'datahub "
            "ingest' (CLI or UI) can run against it without a real SAP tenant."
        )
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help=(
            "Host to bind (default: 127.0.0.1). Use 0.0.0.0 so the DataHub UI "
            "ingestion executor (in a container) can reach it via "
            "host.docker.internal or the host's LAN IP."
        ),
    )
    parser.add_argument(
        "--port", type=int, default=18000, help="Port to bind (default: 18000)."
    )
    parser.add_argument(
        "--sink",
        default=None,
        help=(
            "Optional datahub-rest server URL (e.g. http://localhost:8080). "
            "If omitted, the generated recipe uses a file sink."
        ),
    )
    parser.add_argument(
        "--recipe-out",
        default="/tmp/sap_datasphere_mock_recipe.yml",
        help="Where to write the generated recipe (default: %(default)s).",
    )
    args = parser.parse_args(argv)

    try:
        from pytest_httpserver import HTTPServer
    except ImportError:
        print(
            "ERROR: pytest_httpserver is not installed (it's a test dependency).\n"
            "Install it with:\n"
            "    pip install pytest-httpserver\n"
            "or install the test extra for this package, e.g.:\n"
            "    pip install -e '.[dev]'   # or '.[integration-tests]'",
        )
        return 1

    server = HTTPServer(host=args.host, port=args.port)
    server.start()
    base_url = f"http://{args.host}:{args.port}"
    register_handlers(server, base_url)

    recipe = build_recipe(base_url, sink_url=args.sink)
    recipe_path = Path(args.recipe_out)
    recipe_path.write_text(recipe)

    banner = (
        f"Mock SAP Datasphere serving at {base_url}\n"
        f"Recipe written to {recipe_path}\n"
        f"Run:  datahub ingest -c {recipe_path}\n"
        "Ctrl-C to stop."
    )
    print(banner)
    print("\n--- recipe ---")
    print(recipe)
    print("--- end recipe ---\n", flush=True)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping mock SAP Datasphere server...")
        server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
