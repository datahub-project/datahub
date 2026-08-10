"""Unit tests for usage-events SoT resolution used by smoke fixtures."""

from tests.utilities import env_vars, usage_events_sot


def test_usage_events_implementation_explicit_env(monkeypatch):
    monkeypatch.setenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", "postgres")
    monkeypatch.delenv("DATAHUB_LOCAL_COMMON_ENV", raising=False)
    monkeypatch.delenv("PROFILE_NAME", raising=False)
    monkeypatch.delenv("DB_TYPE", raising=False)
    assert env_vars.get_usage_events_implementation() == "postgres"
    assert env_vars.usage_events_stored_in_postgres() is True


def test_usage_events_implementation_from_postgres_profile(monkeypatch):
    monkeypatch.delenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", raising=False)
    monkeypatch.delenv("DATAHUB_LOCAL_COMMON_ENV", raising=False)
    monkeypatch.setenv("PROFILE_NAME", "quickstart-postgres")
    monkeypatch.delenv("DB_TYPE", raising=False)
    assert env_vars.get_usage_events_implementation() == "postgres"


def test_usage_events_implementation_defaults_to_elasticsearch(monkeypatch):
    monkeypatch.delenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", raising=False)
    monkeypatch.delenv("DATAHUB_LOCAL_COMMON_ENV", raising=False)
    monkeypatch.setenv("PROFILE_NAME", "quickstart-consumers")
    monkeypatch.delenv("DB_TYPE", raising=False)
    assert env_vars.get_usage_events_implementation() == "elasticsearch"
    assert env_vars.usage_events_stored_in_postgres() is False


def test_usage_events_implementation_common_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "common.env"
    env_file.write_text(
        "DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres\n", encoding="utf-8"
    )
    monkeypatch.delenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", raising=False)
    monkeypatch.setenv("DATAHUB_LOCAL_COMMON_ENV", str(env_file))
    monkeypatch.setenv("PROFILE_NAME", "quickstart-consumers")
    assert env_vars.get_usage_events_implementation() == "postgres"


def test_resolve_prefers_gms_over_env(monkeypatch):
    monkeypatch.setenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", "elasticsearch")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")

    class _Session:
        def get(self, url, timeout=None):
            assert timeout == usage_events_sot._SYSTEM_INFO_TIMEOUT_SEC

            class _Resp:
                status_code = 200

                @staticmethod
                def json():
                    return {"platformAnalytics.usage-events.implementation": "postgres"}

            return _Resp()

    assert (
        usage_events_sot.resolve_usage_events_implementation(_Session()) == "postgres"
    )


def test_resolve_gms_without_gms_url_method(monkeypatch):
    """Audit helpers pass plain Sessions that lack gms_url(); use DATAHUB_GMS_URL."""
    monkeypatch.setenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", "elasticsearch")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    monkeypatch.delenv("DATAHUB_FRONTEND_URL", raising=False)

    class _PlainSession:
        def get(self, url, timeout=None):
            assert url.startswith("http://localhost:8080/")
            assert timeout == usage_events_sot._SYSTEM_INFO_TIMEOUT_SEC

            class _Resp:
                status_code = 200

                @staticmethod
                def json():
                    return {"platformAnalytics.usage-events.implementation": "postgres"}

            return _Resp()

    assert (
        usage_events_sot.resolve_usage_events_implementation(_PlainSession())
        == "postgres"
    )


def test_resolve_falls_back_to_env_when_gms_unavailable(monkeypatch):
    monkeypatch.setenv("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", "postgres")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")

    class _Session:
        def get(self, url, timeout=None):
            raise ConnectionError("gms down")

    assert (
        usage_events_sot.resolve_usage_events_implementation(_Session()) == "postgres"
    )


def test_canonicalize_login_source_enum_and_camel_case():
    assert (
        usage_events_sot.canonicalize_login_source("passwordLogin") == "PASSWORD_LOGIN"
    )
    assert (
        usage_events_sot.canonicalize_login_source("PASSWORD_LOGIN") == "PASSWORD_LOGIN"
    )
    assert usage_events_sot.canonicalize_login_source("signUpLinkLogin") == (
        "SIGN_UP_LINK_LOGIN"
    )
    assert usage_events_sot.login_sources_equivalent("passwordLogin", "PASSWORD_LOGIN")
    assert not usage_events_sot.login_sources_equivalent(
        "signUpLinkLogin", "PASSWORD_LOGIN"
    )
