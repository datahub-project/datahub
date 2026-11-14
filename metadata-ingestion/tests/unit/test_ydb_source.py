import json
import sys
from importlib.machinery import EXTENSION_SUFFIXES, ModuleSpec
from types import ModuleType, SimpleNamespace
from typing import Dict, Optional
from unittest.mock import Mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.sink import Sink
from datahub.ingestion.reporting.datahub_ingestion_run_summary_provider import (
    DatahubIngestionRunSummaryProvider,
)
from datahub.ingestion.run.pipeline_config import PipelineConfig
from datahub.ingestion.source.sql import ydb as ydb_source
from datahub.ingestion.source.sql.ydb import YDBConfig, YDBSource


def _base_config():
    return {"host_port": "localhost:2136", "database": "local"}


@pytest.mark.parametrize("profiling_enabled", [False, True])
def test_metadata_and_profiling_do_not_require_lineage_runtime(
    monkeypatch: pytest.MonkeyPatch, profiling_enabled: bool
) -> None:
    def unexpected_plugin_lookup(name: str) -> None:
        raise AssertionError(f"Unexpected lineage dependency lookup: {name}")

    monkeypatch.setattr(ydb_source, "find_spec", unexpected_plugin_lookup)
    source = YDBSource(
        ctx=PipelineContext(run_id="ydb-source-test"),
        config=YDBConfig.model_validate(
            {**_base_config(), "profiling": {"enabled": profiling_enabled}}
        ),
    )
    assert not source.include_lineage


def test_lineage_requires_the_optional_runtime_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ydb_source, "find_spec", lambda name: None)
    with pytest.raises(ValueError, match="ydb-sqlglot-plugin"):
        YDBConfig.model_validate({**_base_config(), "include_view_lineage": True})


def test_lineage_rejects_native_sqlglot_even_if_flag_is_set_late(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_SQLGLOT_DISABLE_C", "1")
    native_module = ModuleType("sqlglot._ydb_runtime_test")
    native_module.__file__ = f"parser{EXTENSION_SUFFIXES[0]}"
    monkeypatch.setitem(sys.modules, native_module.__name__, native_module)
    monkeypatch.setattr(
        ydb_source, "find_spec", lambda name: ModuleSpec(name, loader=None)
    )
    with pytest.raises(ValueError, match="DATAHUB_SQLGLOT_DISABLE_C"):
        YDBConfig.model_validate({**_base_config(), "include_view_lineage": True})


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
    return config.get_connection_options().get("connect_args", {}).get("credentials")


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


def test_auth_service_account_key_content():
    # Secret JSON content is parsed into the dict ydb-dbapi expects.
    config = YDBConfig.model_validate(
        {**_base_config(), "service_account_key": '{"id": "abc", "key": "xyz"}'}
    )
    assert _credentials(config) == {"service_account_json": {"id": "abc", "key": "xyz"}}


def test_service_account_key_requires_a_json_object() -> None:
    config = YDBConfig.model_validate(
        {**_base_config(), "service_account_key": '["not-a-key-object"]'}
    )
    with pytest.raises(ValueError, match="service_account_key"):
        config.get_connection_options()


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
    connect_args = config.get_connection_options()["connect_args"]
    assert connect_args["credentials"] == {"token": "my-token"}
    assert connect_args["protocol"] == "grpcs"
    assert connect_args["root_certificates_path"] == "/certs/ca.pem"


@pytest.mark.parametrize(
    "credential_fields",
    [
        {"access_token": "DO_NOT_EXPOSE_YDB_CREDENTIAL"},
        {"username": "test-user", "password": "DO_NOT_EXPOSE_YDB_CREDENTIAL"},
        {
            "service_account_key": json.dumps(
                {"private_key": "DO_NOT_EXPOSE_YDB_CREDENTIAL"}
            )
        },
    ],
)
def test_building_connection_options_keeps_secrets_out_of_config(
    credential_fields: Dict[str, str],
) -> None:
    config = YDBConfig.model_validate({**_base_config(), **credential_fields})
    options = config.get_connection_options()
    marker = "DO_NOT_EXPOSE_YDB_CREDENTIAL"
    assert marker in json.dumps(options["connect_args"]["credentials"])
    assert marker not in repr(config)
    assert marker not in config.model_dump_json()
    assert marker not in repr(config.model_dump_redacted())
    assert "credentials" not in config.options.get("connect_args", {})


def test_inline_key_is_masked_in_reported_recipe_before_source_validation() -> None:
    marker = "DO_NOT_EXPOSE_YDB_CREDENTIAL"
    pipeline_config = PipelineConfig.model_validate(
        {
            "source": {
                "type": "ydb",
                "config": {
                    **_base_config(),
                    "service_account_key": json.dumps({"private_key": marker}),
                },
            },
            "sink": {"type": "file", "config": {"filename": "/dev/null"}},
        }
    )
    ctx = PipelineContext(run_id="ydb-security-test", pipeline_config=pipeline_config)
    reporter = DatahubIngestionRunSummaryProvider(
        sink=Mock(spec=Sink), report_recipe=True, ctx=ctx
    )
    recipe = reporter._get_recipe_to_report(ctx)
    assert marker not in recipe
    reported_config = json.loads(recipe)["source"]["config"]
    assert "service_account_key" in reported_config
    assert reported_config["database"] == "local"


def test_connection_options_preserve_explicit_driver_settings() -> None:
    options = {
        "pool_pre_ping": True,
        "connect_args": {
            "credentials": {"token": "explicit-token"},
            "protocol": "grpc",
        },
    }
    config = YDBConfig.model_validate(
        {
            **_base_config(),
            "access_token": "fallback-token",
            "protocol": "grpcs",
            "root_certificates": "test-ca",
            "options": options,
        }
    )
    result = config.get_connection_options()
    assert result["connect_args"]["credentials"] == {"token": "explicit-token"}
    assert result["connect_args"]["protocol"] == "grpc"
    assert result["connect_args"]["root_certificates"] == "test-ca"
    assert result["pool_pre_ping"] is True
    assert "root_certificates" not in config.options["connect_args"]
