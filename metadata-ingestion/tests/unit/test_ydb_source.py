from types import SimpleNamespace
from typing import Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.ydb import YDBConfig, YDBSource


def _base_config():
    return {"host_port": "localhost:2136", "database": "local"}


def test_platform_correctly_set_ydb():
    source = YDBSource(
        ctx=PipelineContext(run_id="ydb-source-test"),
        config=YDBConfig.model_validate(_base_config()),
    )
    assert source.platform == "ydb"


def test_sqlalchemy_url_omits_credentials():
    config = YDBConfig.model_validate(
        {**_base_config(), "username": "alice", "password": "secret"}
    )
    url = config.get_sql_alchemy_url(current_db="local")
    assert url == "yql+ydb://localhost:2136/local"
    # Credentials go through connect_args, never the URL (avoids leaking secrets).
    assert "alice" not in url and "secret" not in url


def test_identifier_is_database_qualified():
    config = YDBConfig.model_validate(_base_config())
    # Directory paths are part of the table name in YDB.
    assert (
        config.get_identifier(schema="local", table="jaffle_shop/customers")
        == "local.jaffle_shop/customers"
    )


def _credentials(config: YDBConfig) -> Optional[dict]:
    return config.options.get("connect_args", {}).get("credentials")


def _source() -> YDBSource:
    return YDBSource(
        ctx=PipelineContext(run_id="ydb-source-test"),
        config=YDBConfig.model_validate(_base_config()),
    )


def test_get_db_name_normalizes_to_rooted_path():
    source = _source()
    # YDB databases are always rooted; "local" and "/local" must yield the same name.
    for raw in ("local", "/local"):
        inspector = SimpleNamespace(
            engine=SimpleNamespace(url=SimpleNamespace(database=raw))
        )
        assert source.get_db_name(inspector) == "/local"  # type: ignore[arg-type]


def test_auth_anonymous_has_no_credentials():
    config = YDBConfig.model_validate(_base_config())
    assert _credentials(config) is None


def test_auth_access_token():
    config = YDBConfig.model_validate({**_base_config(), "access_token": "my-token"})
    assert _credentials(config) == {"token": "my-token"}


def test_auth_static_username_password():
    config = YDBConfig.model_validate(
        {**_base_config(), "username": "alice", "password": "secret"}
    )
    assert _credentials(config) == {"username": "alice", "password": "secret"}


def test_auth_service_account_file():
    config = YDBConfig.model_validate(
        {**_base_config(), "service_account_key_file": "/keys/sa.json"}
    )
    assert _credentials(config) == {"service_account_file": "/keys/sa.json"}


def test_auth_service_account_json_secret():
    # Secret JSON content is parsed into the dict ydb-dbapi expects.
    config = YDBConfig.model_validate(
        {**_base_config(), "service_account_json": '{"id": "abc", "key": "xyz"}'}
    )
    assert _credentials(config) == {"service_account_json": {"id": "abc", "key": "xyz"}}


def test_transport_is_orthogonal_to_auth():
    # TLS protocol/certs can accompany any credential type (here, an access token).
    config = YDBConfig.model_validate(
        {
            **_base_config(),
            "access_token": "my-token",
            "protocol": "grpcs",
            "root_certificates_path": "/certs/ca.pem",
        }
    )
    connect_args = config.options["connect_args"]
    assert connect_args["credentials"] == {"token": "my-token"}
    assert connect_args["protocol"] == "grpcs"
    assert connect_args["root_certificates_path"] == "/certs/ca.pem"
