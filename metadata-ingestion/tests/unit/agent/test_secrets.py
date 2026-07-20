import pytest

from datahub.ingestion.agent.secrets import EnvVarResolver, resolve_config


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
    assert out["a"]["host"] == "host1"
    assert out["hosts"] == ["host1", "plain"]


def test_unresolved_ref_raises():
    with pytest.raises(ValueError):
        resolve_config({"password": "${NOPE_MISSING}"}, [EnvVarResolver()])
