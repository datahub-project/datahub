import pytest

from datahub.cli import config_utils
from datahub.ingestion.agent.secrets import (
    DatahubEnvResolver,
    EnvVarResolver,
    resolve_config,
    resolve_config_collecting,
)


def test_resolves_env_ref(monkeypatch):
    monkeypatch.setenv("MY_PW", "s3cr3t")
    out = resolve_config({"password": "${MY_PW}"}, [EnvVarResolver()])
    assert out["password"] == "s3cr3t"


def test_literal_passes_through(monkeypatch):
    out = resolve_config({"password": "inline-literal"}, [EnvVarResolver()])
    assert out["password"] == "inline-literal"


def test_nested_and_list_refs(monkeypatch):
    monkeypatch.setenv("H", "host1")
    out = resolve_config(
        {"a": {"host": "${H}"}, "hosts": ["${H}", "plain"]},
        [EnvVarResolver()],
    )
    nested = out["a"]
    assert isinstance(nested, dict)
    assert nested["host"] == "host1"
    assert out["hosts"] == ["host1", "plain"]


def test_unresolved_ref_raises():
    with pytest.raises(ValueError):
        resolve_config({"password": "${NOPE_MISSING}"}, [EnvVarResolver()])


def test_collecting_records_nested_ref(monkeypatch):
    monkeypatch.setenv("NESTED_PW", "nestedsecret")
    out = resolve_config_collecting(
        {"a": {"b": {"pw": "${NESTED_PW}"}}}, [EnvVarResolver()]
    )
    a = out.config["a"]
    assert isinstance(a, dict)
    b = a["b"]
    assert isinstance(b, dict)
    assert b["pw"] == "nestedsecret"
    assert "nestedsecret" in out.secret_values


def test_datahubenv_refs_resolve_through_the_gms_block(monkeypatch, tmp_path):
    """The file nests under `gms:`, so a flat top-level lookup found nothing.

    Both spellings must work: the dotted path a recipe can write directly, and
    the DATAHUB_GMS_* names the CLI documents as env vars, which people carry
    into recipes out of habit.
    """
    env_file = tmp_path / "datahubenv"
    env_file.write_text("gms:\n  server: http://gms:8080\n  token: tok-abc\n")
    monkeypatch.setattr(config_utils, "DATAHUB_CONFIG_PATH", str(env_file))
    resolver = DatahubEnvResolver()
    assert resolver.resolve("gms.server") == "http://gms:8080"
    assert resolver.resolve("DATAHUB_GMS_TOKEN") == "tok-abc"


def test_datahubenv_resolver_declines_what_it_cannot_supply(monkeypatch, tmp_path):
    """A miss must return None so the caller raises "unresolved ref" rather
    than substituting a subtree or a partial path."""
    env_file = tmp_path / "datahubenv"
    env_file.write_text("gms:\n  server: http://gms:8080\n")
    monkeypatch.setattr(config_utils, "DATAHUB_CONFIG_PATH", str(env_file))
    resolver = DatahubEnvResolver()
    assert resolver.resolve("gms") is None
    assert resolver.resolve("gms.server.deeper") is None
    assert resolver.resolve("nope") is None
