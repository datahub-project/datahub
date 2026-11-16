import os
from typing import Callable
from unittest import mock

import looker_sdk.rtl.requests_transport as looker_requests_transport
import pytest
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import Look, User, WriteQuery
from requests.adapters import HTTPAdapter

from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
    TransportOptionsConfig,
    _DataHubLookerApiSettings,
)

ApiFactory = Callable[[mock.MagicMock], LookerAPI]

_LOOKERSDK_ENV_VARS = (
    "LOOKERSDK_CLIENT_ID",
    "LOOKERSDK_CLIENT_SECRET",
    "LOOKERSDK_BASE_URL",
)


@pytest.fixture
def clean_lookersdk_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in _LOOKERSDK_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def test_credentials_not_written_to_environ(
    clean_lookersdk_env: None, looker_api_config: LookerAPIConfig
) -> None:
    """LookerAPI must not leak credentials into os.environ."""
    with mock.patch("looker_sdk.init40") as mock_init40:
        mock_init40.return_value = mock.MagicMock()
        LookerAPI(config=looker_api_config)

    for var in _LOOKERSDK_ENV_VARS:
        assert var not in os.environ, (
            f"{var} should not be set in os.environ after LookerAPI init"
        )


def test_init40_called_with_in_memory_config_settings(
    looker_api_config: LookerAPIConfig,
) -> None:
    """init40 must receive a config_settings instance carrying the credentials."""
    with mock.patch("looker_sdk.init40") as mock_init40:
        mock_init40.return_value = mock.MagicMock()
        LookerAPI(config=looker_api_config)

    mock_init40.assert_called_once()
    _, kwargs = mock_init40.call_args
    settings = kwargs["config_settings"]

    assert isinstance(settings, _DataHubLookerApiSettings)
    assert settings.base_url == looker_api_config.base_url

    data = settings.read_config()
    assert data["client_id"] == looker_api_config.client_id
    assert data["client_secret"] == looker_api_config.client_secret.get_secret_value()
    assert data["base_url"] == looker_api_config.base_url


def test_settings_read_config_ignores_lookersdk_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """read_config must return in-memory creds even if LOOKERSDK_* env vars are set."""
    monkeypatch.setenv("LOOKERSDK_CLIENT_ID", "should-be-ignored")
    monkeypatch.setenv("LOOKERSDK_CLIENT_SECRET", "should-be-ignored")
    monkeypatch.setenv("LOOKERSDK_BASE_URL", "https://should-be-ignored.example.com")

    settings = _DataHubLookerApiSettings(
        client_id="real-id",
        client_secret="real-secret",
        base_url="https://real.example.com",
    )

    data = settings.read_config()
    assert data["client_id"] == "real-id"
    assert data["client_secret"] == "real-secret"
    assert data["base_url"] == "https://real.example.com"
    assert settings.base_url == "https://real.example.com"


def test_http_adapter_pool_size_matches_max_threads() -> None:
    """HTTPAdapter pool size must be set to max_threads + 10 to avoid connection pool warnings."""
    config = LookerAPIConfig(
        base_url="https://looker.example.com",
        client_id="test-client-id",
        client_secret="test-client-secret",
        max_threads=20,
    )

    mock_session = mock.MagicMock()
    mock_transport = mock.MagicMock(spec=looker_requests_transport.RequestsTransport)
    mock_transport.session = mock_session

    mock_client = mock.MagicMock()
    mock_client.transport = mock_transport

    with (
        mock.patch("looker_sdk.init40", return_value=mock_client),
        mock.patch(
            "datahub.ingestion.source.looker.looker_lib_wrapper.HTTPAdapter",
            wraps=HTTPAdapter,
        ) as mock_adapter_cls,
    ):
        LookerAPI(config=config)

    mock_adapter_cls.assert_called_once_with(
        max_retries=config.max_retries,
        pool_connections=30,
        pool_maxsize=30,
    )
    assert mock_session.mount.call_count == 2


def test_transport_options_headers_defaults_to_empty() -> None:
    """Regression guard: headers was required-without-default, so a recipe
    setting `transport_options: { timeout: 300 }` failed validation with
    "transport_options.headers Field required" and aborted the ingestion run.
    """
    opts = TransportOptionsConfig(timeout=300)
    assert opts.headers == {}
    # TransportOptions is a TypedDict (dict subclass), so key access.
    transport = opts.get_transport_options()
    assert transport["timeout"] == 300
    assert transport["headers"] == {}


class TestSdkErrorHandling:
    """SDK failures on lookups are swallowed so one bad object does not abort a run."""

    def test_user_lookup_failures_return_none(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        client.user.side_effect = [
            SDKError("<title>Looker Not Found (404)</title>"),
            SDKError("forbidden"),
            User(id="3", email="three@example.com"),
        ]
        api = looker_api_factory(client)
        assert api.get_user("1", user_fields="id,email") is None
        assert api.get_user("2", user_fields="id,email") is None
        found = api.get_user("3", user_fields="id,email")
        assert found is not None and found.email == "three@example.com"
        # Results are cached per user id.
        assert api.get_user("3", user_fields="id,email") is found
        assert client.user.call_count == 3
        assert api.compute_stats()["client_stats"].user_calls == 3

    def test_all_users_failure_returns_empty_list(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        client.all_users.side_effect = SDKError("forbidden")
        assert looker_api_factory(client).all_users(user_fields="id") == []

    def test_folder_ancestors_failures_return_empty_list(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        client.folder_ancestors.side_effect = [
            SDKError("<title>Looker Not Found (404)</title>"),
            SDKError("forbidden"),
        ]
        api = looker_api_factory(client)
        assert api.folder_ancestors("missing") == []
        assert api.folder_ancestors("denied") == []
        # Default fields are requested when none are given.
        assert client.folder_ancestors.call_args.args[1] == "id,name,parent_id"


class TestQueryCalls:
    def test_execute_query_decodes_json_rows(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        client.run_inline_query.return_value = '[{"a": 1}]'
        api = looker_api_factory(client)
        query = WriteQuery(model="m", view="v", fields=["v.a"])
        assert api.execute_query(query) == [{"a": 1}]
        assert client.run_inline_query.call_args.kwargs["result_format"] == "json"

    def test_generate_sql_query_requests_sql_with_cache_flag(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        client.run_inline_query.return_value = "SELECT 1"
        api = looker_api_factory(client)
        query = WriteQuery(model="m", view="v", fields=["v.a"])
        assert api.generate_sql_query(query, use_cache=True) == "SELECT 1"
        kwargs = client.run_inline_query.call_args.kwargs
        assert kwargs["result_format"] == "sql"
        assert kwargs["cache"] is True
        assert api.compute_stats()["client_stats"].generate_sql_query_calls == 1

    def test_all_looks_includes_soft_deleted_when_asked(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        client.all_looks.return_value = [Look(id="1")]
        client.search_looks.return_value = [Look(id="2", deleted=True)]
        api = looker_api_factory(client)
        assert [
            look.id for look in api.all_looks(fields=["id"], soft_deleted=False)
        ] == ["1"]
        assert [
            look.id for look in api.all_looks(fields=["id"], soft_deleted=True)
        ] == ["1", "2"]
        assert client.search_looks.call_args.kwargs["deleted"] is True

    def test_list_fields_are_joined_for_the_sdk(
        self, looker_api_factory: ApiFactory
    ) -> None:
        client = mock.MagicMock()
        api = looker_api_factory(client)
        api.lookml_model("sales", fields=["project_name", "explores"])
        assert client.lookml_model.call_args.args == ("sales", "project_name,explores")
        api.get_query("q1", fields=["id", "view"])
        assert client.query.call_args.kwargs["fields"] == "id,view"
        api.lookml_model_explore("sales", "orders")
        assert client.lookml_model_explore.call_args.kwargs["fields"] is None
