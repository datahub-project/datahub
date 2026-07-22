from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.emitter.token_provider import StaticTokenProvider, TokenProviderAuth


def test_emitter_installs_session_auth_and_skips_static_header():
    auth = TokenProviderAuth(StaticTokenProvider("tok"), retry_on_401=False)
    emitter = DataHubRestEmitter(gms_server="http://gms", auth=auth)
    assert emitter._session.auth is auth
    # The static Authorization header must NOT be baked in when auth is used.
    assert "Authorization" not in emitter._session.headers


def test_emitter_static_token_still_works():
    emitter = DataHubRestEmitter(gms_server="http://gms", token="tok")
    assert emitter._session.headers.get("Authorization") == "Bearer tok"
    assert emitter._session.auth is None


def test_emitter_explicit_host_uses_env_oauth(monkeypatch):
    # No token/auth passed + DATAHUB_AUTH_TYPE set -> the emitter resolves env
    # OAuth even for an explicit host (not just the __from_env__ sentinel).
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", "http://idp/token")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_SECRET", "csecret")
    emitter = DataHubRestEmitter(gms_server="http://gms")
    assert isinstance(emitter._session.auth, TokenProviderAuth)
    assert "Authorization" not in emitter._session.headers


def test_emitter_explicit_token_beats_env_oauth(monkeypatch):
    # Caller-supplied token wins: env OAuth must not override explicit creds.
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", "http://idp/token")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_SECRET", "csecret")
    emitter = DataHubRestEmitter(gms_server="http://gms", token="tok")
    assert emitter._session.auth is None
    assert emitter._session.headers.get("Authorization") == "Bearer tok"


def test_emitter_no_env_auth_is_unchanged(monkeypatch):
    # No DATAHUB_AUTH_TYPE -> no auth resolved, existing behavior preserved.
    monkeypatch.delenv("DATAHUB_AUTH_TYPE", raising=False)
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    emitter = DataHubRestEmitter(gms_server="http://gms")
    assert emitter._session.auth is None


def test_emitter_resolve_env_auth_false_suppresses_env_oauth(monkeypatch):
    # A caller that does its own env-auth resolution (e.g. the datahub-rest sink,
    # with its origin guard) passes resolve_env_auth=False; the emitter must NOT
    # re-resolve env OAuth, even with DATAHUB_AUTH_TYPE set.
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", "http://idp/token")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_SECRET", "csecret")
    emitter = DataHubRestEmitter(gms_server="http://gms", resolve_env_auth=False)
    assert emitter._session.auth is None


def test_emitter_explicit_host_ignores_static_gms_token(monkeypatch):
    # Invariant: an explicit-host tokenless emitter must NOT read the static
    # DATAHUB_GMS_TOKEN from env (that env token is only for the __from_env__
    # sentinel). With no DATAHUB_AUTH_TYPE it falls through to system auth and
    # must never bake `Bearer <DATAHUB_GMS_TOKEN>`.
    monkeypatch.delenv("DATAHUB_AUTH_TYPE", raising=False)
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "static-env-token")
    emitter = DataHubRestEmitter(gms_server="http://gms")
    assert emitter._session.auth is None
    assert emitter._session.headers.get("Authorization") != "Bearer static-env-token"
