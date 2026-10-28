import base64
import hashlib
from datetime import datetime, timezone
from typing import Callable, Dict, Iterator, List, Optional, Union
from unittest.mock import MagicMock, patch

import pytest
from OpenSSL import crypto
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.snowflake.oauth_config import OAuthIdentityProvider
from datahub.ingestion.source.snowflake.oauth_generator import OAuthTokenGenerator
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeSchema,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source

QueryResult = Union[List[dict], Exception]
QueryHandler = Callable[[str], QueryResult]

SESSION_ROWS: Dict[str, List[dict]] = {
    SnowflakeQuery.current_version(): [{"CURRENT_VERSION()": "8.1.0"}],
    SnowflakeQuery.current_role(): [{"CURRENT_ROLE()": "TEST_ROLE"}],
    SnowflakeQuery.current_warehouse(): [{"CURRENT_WAREHOUSE()": "TEST_WH"}],
    SnowflakeQuery.show_tags(): [],
    SnowflakeQuery.current_account(): [{"CURRENT_ACCOUNT()": "ABC123"}],
    SnowflakeQuery.current_region(): [{"CURRENT_REGION()": "PUBLIC.AWS_US_WEST_2"}],
}


class _Cursor(list):
    @property
    def rowcount(self) -> int:
        return len(self)


def _make_source(
    overrides: Optional[Dict[str, QueryResult]] = None, **config_overrides: object
) -> SnowflakeV2Source:
    rows: Dict[str, QueryResult] = {**SESSION_ROWS, **(overrides or {})}

    def execute(query: str) -> _Cursor:
        if query not in rows:
            raise AssertionError(f"Unexpected query: {query}")
        result = rows[query]
        if isinstance(result, Exception):
            raise result
        return _Cursor(result)

    native = MagicMock()
    native.cursor.return_value.execute.side_effect = execute
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            # Password auth adds a deprecation warning to the report, which
            # would muddy the warning counts below; connect() is patched anyway.
            "authentication_type": "EXTERNAL_BROWSER_AUTHENTICATOR",
            "use_queries_v2": False,
            "include_table_lineage": False,
            "include_usage_stats": False,
            "include_operational_stats": False,
            "include_assertion_results": False,
            **config_overrides,
        }
    )
    with patch("snowflake.connector.connect", return_value=native):
        return SnowflakeV2Source(ctx=PipelineContext(run_id="test"), config=config)


def test_session_metadata_detects_standard_edition_from_tag_support() -> None:
    standard = _make_source(
        {SnowflakeQuery.show_tags(): RuntimeError("Unsupported feature 'TAG'.")}
    )
    standard.inspect_session_metadata(standard.connection)
    assert standard.report.edition == SnowflakeEdition.STANDARD
    assert standard.report.saas_version == "8.1.0"
    assert standard.report.role == "TEST_ROLE"
    assert standard.report.default_warehouse == "TEST_WH"

    enterprise = _make_source()
    enterprise.inspect_session_metadata(enterprise.connection)
    assert enterprise.report.edition == SnowflakeEdition.ENTERPRISE

    # Any other failure to list tags must not be mistaken for Standard edition.
    unknown = _make_source({SnowflakeQuery.show_tags(): RuntimeError("timeout")})
    unknown.inspect_session_metadata(unknown.connection)
    assert unknown.report.edition is None


def test_known_edition_in_config_skips_the_probe() -> None:
    source = _make_source(
        {SnowflakeQuery.show_tags(): AssertionError("should not be queried")},
        known_snowflake_edition="Standard",
    )

    source.inspect_session_metadata(source.connection)

    assert source.report.edition == SnowflakeEdition.STANDARD
    assert source.report.failures == []


def test_missing_warehouse_stops_extraction_with_a_failure() -> None:
    source = _make_source(
        {SnowflakeQuery.current_warehouse(): [{"CURRENT_WAREHOUSE()": None}]},
        warehouse="MISSING_WH",
    )

    with (
        patch.object(source, "_snowflake_clear_ocsp_cache"),
        patch(
            "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeSchemaGenerator"
        ) as schema_gen,
    ):
        assert list(source.get_workunits_internal()) == []

    schema_gen.assert_not_called()
    assert len(source.report.failures) == 1


@pytest.mark.parametrize("warn_no_datasets", [False, True])
def test_no_discovered_datasets_is_failure_unless_downgraded(
    warn_no_datasets: bool,
) -> None:
    source = _make_source(warn_no_datasets=warn_no_datasets)

    with (
        patch.object(source, "_snowflake_clear_ocsp_cache"),
        patch(
            "datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeSchemaGenerator"
        ) as schema_gen,
    ):
        schema_gen.return_value.get_workunits_internal.return_value = iter([])
        schema_gen.return_value.databases = []
        assert list(source.get_workunits_internal()) == []

    assert source.discovered_datasets == []
    assert len(source.report.failures) == (0 if warn_no_datasets else 1)
    assert len(source.report.warnings) == (1 if warn_no_datasets else 0)


def test_snowsight_url_builder_uses_account_locator_and_bare_region() -> None:
    source = _make_source()

    builder = source.get_snowsight_url_builder()

    assert builder is not None
    assert builder.snowsight_base_url == "https://app.snowflake.com/us-west-2/abc123/"
    assert source.report.account_locator == "ABC123"
    assert source.report.region == "PUBLIC.AWS_US_WEST_2"

    failing = _make_source({SnowflakeQuery.current_region(): RuntimeError("denied")})
    assert failing.get_snowsight_url_builder() is None
    assert len(failing.report.warnings) == 1


def test_temp_and_allowed_table_checks_use_discovered_datasets() -> None:
    source = _make_source(
        temporary_tables_pattern=[".*\\.tmp_.*"],
        table_pattern={"deny": [".*\\.denied"]},
    )
    source.discovered_datasets = ["test_db.public.orders"]

    assert source._is_temp_table("test_db.public.tmp_orders")
    # Allowed by the recipe but never discovered: treat as a transient table
    # rather than inventing lineage to a dataset that was never emitted.
    assert source._is_temp_table("test_db.public.staging")
    assert not source._is_temp_table("test_db.public.orders")
    assert not source._is_temp_table("test_db.public.denied")

    assert source._is_allowed_table("test_db.public.orders")
    assert not source._is_allowed_table("test_db.public.staging")
    assert not source._is_allowed_table("test_db.public.denied")


def _profiling_table(name: str, table_type: str = "BASE TABLE") -> SnowflakeTable:
    return SnowflakeTable(
        name=name,
        comment=None,
        created=datetime(2023, 1, 1, tzinfo=timezone.utc),
        last_altered=None,
        size_in_bytes=1024,
        rows_count=10,
        column_count=2,
        type=table_type,
    )


def test_profiler_skips_external_tables_and_falls_back_to_first_schema() -> None:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            "password": "pass",
            "profiling": {"enabled": True, "profile_external_tables": False},
        }
    )
    profiler = SnowflakeProfiler(config, SnowflakeV2Report())
    database = SnowflakeDatabase(
        name="TEST_DB",
        created=None,
        comment=None,
        schemas=[
            SnowflakeSchema(
                name="ANALYTICS", created=None, last_altered=None, comment=None
            )
        ],
    )
    db_tables = {
        "ANALYTICS": [
            _profiling_table("ORDERS"),
            _profiling_table("EXT_EVENTS", table_type="EXTERNAL TABLE"),
        ]
    }

    with patch.object(
        profiler, "generate_profile_workunits", return_value=iter([])
    ) as generate:
        list(profiler.get_workunits(database, db_tables))

    requests = generate.call_args.args[0]
    assert [r.pretty_name for r in requests] == ["test_db.analytics.orders"]
    assert requests[0].batch_kwargs["row_count"] == 10
    assert profiler.report.profiling_skipped_other["ANALYTICS"] == 1

    # No PUBLIC schema, so the connection must explicitly switch to one that
    # exists or unqualified profiling queries fail.
    native = MagicMock()
    with patch.object(SnowflakeV2Config, "get_native_connection", return_value=native):
        profiler.callable_for_db_connection("TEST_DB")()
    executed = [c.args[0] for c in native.cursor.return_value.execute.call_args_list]
    assert executed == [
        SnowflakeQuery.use_database("TEST_DB"),
        SnowflakeQuery.use_schema("ANALYTICS"),
    ]


def test_okta_token_request_picks_grant_from_configured_credentials() -> None:
    with patch(
        "datahub.ingestion.source.snowflake.oauth_generator.requests.post"
    ) as post:
        post.return_value.json.return_value = {"access_token": "tok"}

        client_grant = OAuthTokenGenerator(
            client_id="cid",
            authority_url="https://idp.example.test/token",
            provider=OAuthIdentityProvider.OKTA,
        )
        assert client_grant.get_token_with_secret("sec", ["read", "write"]) == {
            "access_token": "tok"
        }
        assert post.call_args.kwargs["auth"] == ("cid", "sec")
        assert post.call_args.kwargs["data"] == {
            "grant_type": "client_credentials",
            "scope": "read,write",
        }

        password_grant = OAuthTokenGenerator(
            client_id="cid",
            authority_url="https://idp.example.test/token",
            provider=OAuthIdentityProvider.OKTA,
            username="svc_user",
            password=SecretStr("svc_pass"),
        )
        password_grant.get_token_with_secret("sec", ["read"])
        assert post.call_args.kwargs["data"] == {
            "grant_type": "password",
            "scope": "read",
            "username": "svc_user",
            "password": "svc_pass",
        }


def test_microsoft_token_prefers_cache_only_when_asked() -> None:
    generator = OAuthTokenGenerator(
        client_id="cid",
        authority_url="https://login.example.test/tenant",
        provider=OAuthIdentityProvider.MICROSOFT,
    )
    with patch(
        "datahub.ingestion.source.snowflake.oauth_generator.msal.ConfidentialClientApplication"
    ) as app_cls:
        app = app_cls.return_value
        app.acquire_token_silent.return_value = {"access_token": "cached"}
        app.acquire_token_for_client.return_value = {"access_token": "fresh"}

        assert generator.get_token_with_secret("sec", ["s"], check_cache=True) == {
            "access_token": "cached"
        }
        app.acquire_token_for_client.assert_not_called()

        app.acquire_token_silent.return_value = None
        assert generator.get_token_with_secret("sec", ["s"], check_cache=True) == {
            "access_token": "fresh"
        }

        app.acquire_token_silent.reset_mock()
        generator.get_token_with_secret("sec", ["s"])
        app.acquire_token_silent.assert_not_called()


def _self_signed_certificate() -> Iterator[bytes]:
    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, 2048)
    cert = crypto.X509()
    cert.get_subject().CN = "datahub-test"
    cert.set_serial_number(1)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(3600)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(key)
    cert.sign(key, "sha256")
    yield crypto.dump_privatekey(crypto.FILETYPE_PEM, key)
    yield crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
    yield crypto.dump_certificate(crypto.FILETYPE_ASN1, cert)


def test_certificate_credential_carries_sha1_thumbprint_of_the_public_cert() -> None:
    private_pem, public_pem, public_der = _self_signed_certificate()
    generator = OAuthTokenGenerator(
        client_id="cid",
        authority_url="https://login.example.test/tenant",
        provider=OAuthIdentityProvider.MICROSOFT,
    )

    with patch(
        "datahub.ingestion.source.snowflake.oauth_generator.msal.ConfidentialClientApplication"
    ) as app_cls:
        app_cls.return_value.acquire_token_for_client.return_value = {"ok": True}
        token = generator.get_token_with_certificate(
            private_key_content=base64.b64encode(private_pem).decode(),
            public_key_content=base64.b64encode(public_pem).decode(),
            scopes=["s"],
        )

    assert token == {"ok": True}
    credential = app_cls.call_args.kwargs["client_credential"]
    # Azure matches the certificate by the hex SHA-1 of its DER form, no colons.
    assert credential["thumbprint"] == hashlib.sha1(public_der).hexdigest().upper()
    assert credential["private_key"] == private_pem
