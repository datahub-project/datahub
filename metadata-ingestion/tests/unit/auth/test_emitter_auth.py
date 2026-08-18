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
