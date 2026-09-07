import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Optional

if TYPE_CHECKING:
    from pytest_httpserver import HTTPServer
    from werkzeug.wrappers import Request as WerkzeugRequest

RECORDED_DIR = Path(__file__).parent / "fixtures" / "recorded"

# Placeholder host baked into the assets fixture's assetRelationalMetadataUrl
# fields; rewritten to the mock server URL at runtime.
RECORDED_TENANT_HOST = "https://RECORDED_TENANT"

SPACE = "DEMO_SPACE"
RECORDED_TOKEN = "recorded-replay-token"

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
# Analytic models (supportsAnalyticalQueries) route to /analyticmodels/ and
# carry businessLayerDefinitions for star-schema lineage.
ANALYTIC_MODEL_NAMES = [
    "Test_Analytic_Model",
]
# Source objects an analytic model projects from. These are not catalog assets;
# the connector probes them via the /views/ endpoint (by the projection's
# source-object name) to recover the analytic model's typeless column types.
SOURCE_OBJECT_VIEW_NAMES = [
    "FINANCE_DATASALES_A",
]


def _fixture_text(rel: str) -> str:
    return (RECORDED_DIR / rel).read_text()


def _assets_payload_rewritten(base_url: str) -> str:
    return _fixture_text("catalog_assets.json").replace(RECORDED_TENANT_HOST, base_url)


def register_handlers(
    httpserver: "HTTPServer",
    base_url: str,
    on_token: Optional[Callable[["WerkzeugRequest"], None]] = None,
    on_request: Optional[Callable[["WerkzeugRequest"], None]] = None,
) -> None:
    # Imported lazily so the module imports even without the optional werkzeug dep.
    from werkzeug.wrappers import (
        Request as WerkzeugRequest,
        Response as WerkzeugResponse,
    )

    # Permissive for any grant so the server works for every auth mode; the
    # test layers grant-type assertions on top via on_token.
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

    httpserver.expect_request(
        f"/api/v1/datasphere/consumption/catalog/spaces('{SPACE}')/assets",
        method="GET",
    ).respond_with_data(
        _assets_payload_rewritten(base_url),
        content_type="application/json",
    )

    httpserver.expect_request(
        f"/api/v1/datasphere/spaces/{SPACE}/connections",
        method="GET",
    ).respond_with_data(
        _fixture_text("connections.json"),
        content_type="application/json",
    )

    for name in VIEW_NAMES:
        httpserver.expect_request(
            f"/api/v1/dwc/consumption/relational/{SPACE}/{name}/$metadata",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"edmx/{name}.xml"),
            content_type="application/xml",
        )

    # Non-analytical views route to /views/.
    for name in VIEW_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/views/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"views/{name}.json"),
            content_type="application/json",
        )

    # Source objects projected by an analytic model, probed via /views/ to
    # resolve the model's typeless (measure) columns.
    for name in SOURCE_OBJECT_VIEW_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/views/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"views/{name}.json"),
            content_type="application/json",
        )

    # Analytic models route to /analyticmodels/.
    for name in ANALYTIC_MODEL_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/analyticmodels/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"analyticmodels/{name}.json"),
            content_type="application/json",
        )

    httpserver.expect_request(
        f"/dwaas-core/api/v1/spaces/{SPACE}/localtables",
        method="GET",
    ).respond_with_data(
        _fixture_text("localtables.json"),
        content_type="application/json",
    )

    for name in LOCAL_TABLE_NAMES:
        httpserver.expect_request(
            f"/dwaas-core/api/v1/spaces/{SPACE}/localtables/{name}",
            method="GET",
        ).respond_with_data(
            _fixture_text(f"localtables/{name}.json"),
            content_type="application/json",
        )

    # Fallback 404 for source-object probes whose name is not a real catalog
    # object — query aliases and association navigation names. When resolving an
    # analytic model's typeless columns, the connector probes each projected
    # source object (and, on a 404, retries as an analytic model); association
    # refs get probed the same way. A real tenant returns 404 for a non-object
    # name and the connector falls back to the measure heuristic, so mirror that
    # rather than leaving the probe unhandled. Registered last: the specific
    # handlers above win for real objects; this only catches the rest.
    for kind in ("views", "analyticmodels"):
        httpserver.expect_request(
            re.compile(rf"^/dwaas-core/api/v1/spaces/{re.escape(SPACE)}/{kind}/.+$"),
            method="GET",
        ).respond_with_data("", status=404)


def build_recipe(base_url: str, sink_url: Optional[str] = None) -> str:
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
