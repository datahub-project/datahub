"""End-to-end OAuth smoke test: SDK/CLI authenticating to GMS with a real OAuth
token fetched from an OSS IdP (Keycloak).

Run from inside the compose network (see docker-compose.oauth.yml / README.md):

    docker compose -f <quickstart> -f docker-compose.oauth.yml exec tester sh -c \\
      "pip install -e /repo/metadata-ingestion && pytest /smoke/tests/oauth/test_oauth_cli_gms.py -v"

Required env (set on the `tester` service):
  DATAHUB_GMS_URL          - internal GMS URL, e.g. http://datahub-gms:8080
  KEYCLOAK_TOKEN_ENDPOINT  - Keycloak token endpoint for the `datahub` realm
"""

import os

import pytest
from requests.exceptions import HTTPError

from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig

GMS = os.environ.get("DATAHUB_GMS_URL")
TOKEN_ENDPOINT = os.environ.get("KEYCLOAK_TOKEN_ENDPOINT")

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
    return DataHubGraph(
        DatahubClientConfig(
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
    )


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
