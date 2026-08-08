"""Unit tests for usage-events SoT resolution used by smoke fixtures."""

from tests.utilities import env_vars


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
