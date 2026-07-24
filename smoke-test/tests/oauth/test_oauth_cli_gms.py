"""End-to-end OAuth smoke test: SDK/CLI authenticating to GMS with a real OAuth
token fetched from an OSS IdP (Keycloak).

Run from inside the compose network (see docker-compose.oauth.yml / README.md):

    docker compose -f <quickstart> -f docker-compose.oauth.yml exec tester sh -c \\
      "pip install -e /repo/metadata-ingestion && pytest /smoke/test_oauth_cli_gms.py -v"

Required env (set on the `tester` service):
  DATAHUB_GMS_URL          - internal GMS URL, e.g. http://datahub-gms:8080
  KEYCLOAK_TOKEN_ENDPOINT  - Keycloak token endpoint for the `datahub` realm
"""

import importlib.util
import json
import logging
import os
import uuid

import pytest
import requests
from requests.exceptions import HTTPError

from datahub.cli.config_utils import load_client_config
from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.run.pipeline import Pipeline

logger = logging.getLogger(__name__)

GMS = os.environ.get("DATAHUB_GMS_URL")
TOKEN_ENDPOINT = os.environ.get("KEYCLOAK_TOKEN_ENDPOINT")

# Env-based auth (DATAHUB_AUTH_TYPE) shipped after the auth layer itself; skip
# those tests gracefully on an older SDK instead of failing collection. The CI
# harness overlays the triggering ref's SDK source onto the released install
# (see run-oauth-ci.sh), so nothing skips there — and the CI script fails the
# run if anything does.
requires_env_auth = pytest.mark.skipif(
    importlib.util.find_spec("datahub.ingestion.auth.env") is None,
    reason="installed acryl-datahub predates env-based auth (datahub.ingestion.auth.env)",
)

# Opt-in: skipped unless a GMS configured for external OAuth AND a Keycloak realm
# are available (see README). Keeps it out of the default CI smoke run entirely —
# no Keycloak/GMS-OAuth setup means zero added CI runtime.
pytestmark = pytest.mark.skipif(
    not (GMS and TOKEN_ENDPOINT),
    reason="OAuth smoke test requires DATAHUB_GMS_URL + KEYCLOAK_TOKEN_ENDPOINT (see README)",
)

# Requires the authenticated actor; a token GMS rejects (wrong audience) fails here.
_ME_QUERY = "{ me { corpUser { urn } } }"


def _graph(client_id: str, client_secret: str) -> DataHubGraph:
    config = DatahubClientConfig(
        server=GMS,
        auth={
            "type": "oidc_client_credentials",
            "config": {
                "token_endpoint": TOKEN_ENDPOINT,
                "client_id": client_id,
                "client_secret": client_secret,
            },
        },
    )
    # On an SDK without the auth layer (< 1.6.0.9) pydantic silently drops the
    # extra `auth` field, the graph sends no credentials, and the positive test
    # 401s while the negative test passes vacuously. Fail loudly instead.
    assert getattr(config, "auth", None) is not None, (
        "DatahubClientConfig dropped the 'auth' config — the installed "
        "acryl-datahub is too old for this test (needs >= 1.6.0.9)."
    )
    return DataHubGraph(config)


def test_oauth_authenticated_call_succeeds():
    # datahub-executor's token carries aud=datahub-gms, which GMS trusts.
    graph = _graph("datahub-executor", "datahub-executor-secret")
    result = graph.execute_graphql(_ME_QUERY)
    assert result["me"]["corpUser"]["urn"]


def test_wrong_audience_is_rejected():
    # datahub-wrong-aud's token carries aud=not-datahub -> GMS must reject it.
    # The 401 surfaces as a wrapped OperationalError (or raw HTTPError).
    graph = _graph("datahub-wrong-aud", "datahub-wrong-aud-secret")
    with pytest.raises((OperationalError, HTTPError)):
        graph.execute_graphql(_ME_QUERY)


def test_unauthenticated_request_is_rejected():
    # Guards the guard: if METADATA_SERVICE_AUTH_ENABLED regresses to false, GMS
    # accepts ANY request and every other test here passes vacuously. An
    # anonymous GraphQL call must be a 401.
    response = requests.post(
        f"{GMS}/api/graphql",
        json={"query": _ME_QUERY},
        timeout=30,
    )
    assert response.status_code == 401, (
        f"expected 401 for unauthenticated request, got {response.status_code} — "
        "is METADATA_SERVICE_AUTH_ENABLED=true on GMS?"
    )


def _set_env_auth(monkeypatch) -> None:
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", TOKEN_ENDPOINT or "")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_ID", "datahub-executor")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_SECRET", "datahub-executor-secret")


@requires_env_auth
def test_env_auth_resolves_client_config(monkeypatch):
    # DATAHUB_AUTH_TYPE alone (no DATAHUB_GMS_TOKEN) must yield an auth-carrying
    # client config — the path the CLI, default sink, and executor recipe
    # subprocesses resolve through.
    _set_env_auth(monkeypatch)
    config = load_client_config()
    assert config.token is None
    assert config.auth is not None and config.auth.type == "oidc_client_credentials"
    result = DataHubGraph(config).execute_graphql(_ME_QUERY)
    assert result["me"]["corpUser"]["urn"]


def _write_events(tmp_path, urn: str) -> str:
    events_path = tmp_path / "events.json"
    events_path.write_text(
        json.dumps(
            [
                {
                    "entityType": "dataset",
                    "entityUrn": urn,
                    "changeType": "UPSERT",
                    "aspectName": "status",
                    "aspect": {"json": {"removed": False}},
                }
            ]
        )
    )
    return str(events_path)


def _run_file_pipeline(tmp_path, sink_config, urn: str) -> None:
    """Run a file-source ingest of one status aspect for `urn`; raise on failure.

    report_to=None disables the default run-summary reporting: it writes
    dataHubIngestionSourceInfo, which a dedicated GMS aspect validator
    authorizes regardless of REST_API_AUTHORIZATION_ENABLED — and granting
    the machine principal privileges is a deployment concern, not this
    authentication canary's subject.
    """
    recipe: dict = {
        "source": {"type": "file", "config": {"path": _write_events(tmp_path, urn)}},
    }
    if sink_config is not None:
        recipe["sink"] = {"type": "datahub-rest", "config": sink_config}
    pipeline = Pipeline.create(recipe, report_to=None)
    pipeline.run()
    pipeline.raise_from_status()


def _unique_test_urn() -> str:
    return (
        "urn:li:dataset:(urn:li:dataPlatform:oauthsmoke,"
        f"oauth_smoke.{uuid.uuid4().hex[:8]},PROD)"
    )


@pytest.fixture
def cleanup_urn():
    # Idempotency: every ingest test writes a unique URN and hard-deletes it,
    # authenticating the delete with the declarative auth config.
    urn = _unique_test_urn()
    yield urn
    try:
        _graph("datahub-executor", "datahub-executor-secret").delete_entity(
            urn, hard=True
        )
    except Exception as e:
        logger.warning(f"cleanup of {urn} failed (non-fatal): {e}")


@requires_env_auth
def test_ingest_default_sink_uses_env_auth(monkeypatch, tmp_path, cleanup_urn):
    # A recipe with NO sink block resolves the default datahub-rest sink from
    # the environment — the shape executor-run recipes use. Also exercises the
    # ctx.graph derivation from the OAuth sink (from_emitter).
    _set_env_auth(monkeypatch)
    _run_file_pipeline(tmp_path, sink_config=None, urn=cleanup_urn)


@requires_env_auth
def test_ingest_explicit_credentialless_sink_inherits_env_auth(
    monkeypatch, tmp_path, cleanup_urn
):
    # An explicit `sink: datahub-rest` block WITHOUT credentials (the common
    # UI-managed recipe shape) must inherit env OAuth instead of silently
    # emitting unauthenticated.
    _set_env_auth(monkeypatch)
    _run_file_pipeline(tmp_path, sink_config={"server": GMS}, urn=cleanup_urn)


def test_connector_graph_client_from_recipe_datahub_api(tmp_path, cleanup_urn):
    # The "plain" DataHub client every connector sees (pipeline ctx.graph),
    # configured DECLARATIVELY in the recipe (datahub_api.auth + sink auth) —
    # no env vars involved. After the ingest, an authenticated connector-style
    # read through ctx.graph must see the entity. Sink mode is sync so the
    # write is immediately readable (no eventual-consistency flake).
    auth_block = {
        "type": "oidc_client_credentials",
        "config": {
            "token_endpoint": TOKEN_ENDPOINT,
            "client_id": "datahub-executor",
            "client_secret": "datahub-executor-secret",
        },
    }
    recipe = {
        "datahub_api": {"server": GMS, "auth": auth_block},
        "source": {
            "type": "file",
            "config": {"path": _write_events(tmp_path, cleanup_urn)},
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": GMS, "auth": auth_block, "mode": "SYNC"},
        },
    }
    # report_to=None: see _run_file_pipeline — run-summary reporting needs
    # privileges this canary's machine principal deliberately doesn't have.
    pipeline = Pipeline.create(recipe, report_to=None)
    pipeline.run()
    pipeline.raise_from_status()

    graph = pipeline.ctx.graph
    assert graph is not None
    assert graph.exists(cleanup_urn), (
        "connector-facing ctx.graph could not read back the entity it ingested"
    )


@requires_env_auth
def test_ingest_explicit_static_token_wins_over_env_auth(
    monkeypatch, tmp_path, cleanup_urn
):
    # Precedence: explicit credentials in the sink block beat the environment.
    # A bogus static token must fail with 401s even though valid env OAuth is
    # available — env auth must never silently override explicit recipe config.
    _set_env_auth(monkeypatch)
    with pytest.raises(Exception) as exc_info:
        _run_file_pipeline(
            tmp_path,
            sink_config={"server": GMS, "token": "bogus-static-token"},
            urn=cleanup_urn,
        )
    assert "401" in str(exc_info.value) or "Unauthorized" in str(exc_info.value), (
        f"expected an authentication failure, got: {exc_info.value!r}"
    )


@requires_env_auth
def test_version_check_config_fetch_uses_env_auth(monkeypatch):
    # The CLI version check fetches /config through DataHubGraph
    # (upgrade.fetch_server_config_via_graph), minting a real token from the
    # env-configured provider on the way. GMS serves /config unauthenticated
    # (it's in authentication.excludedPaths), so this does NOT prove GMS
    # accepted the token — it gates that the version-check path itself
    # (env auth -> load_client_config -> DataHubGraph -> token mint -> /config)
    # works when auth= replaces a static token, instead of crashing or hanging.
    upgrade = pytest.importorskip("datahub.upgrade.upgrade")
    if not hasattr(upgrade, "fetch_server_config_via_graph"):
        pytest.skip(
            "installed acryl-datahub predates upgrade.fetch_server_config_via_graph"
        )

    _set_env_auth(monkeypatch)
    server_config = upgrade.fetch_server_config_via_graph()
    assert server_config.service_version
