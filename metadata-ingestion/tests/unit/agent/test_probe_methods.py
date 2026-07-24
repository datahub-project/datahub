from typing import Callable

import pytest

from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    ProbeParam,
    _coerce,
    list_probe_methods,
    probe_method,
    run_probe_method,
)


def _spec(fn: Callable) -> ProbeMethodSpec:
    return getattr(fn, "__probe_command__")  # noqa: B009


def test_from_func_derives_params_and_full_docstring():
    class P:
        @probe_method()
        def foreign_keys(self, schema: str, table: str) -> list:
            """First line of help.

            A second paragraph the agent should also see."""
            return []

    spec = _spec(P.foreign_keys)
    assert spec.command == "foreign_keys"
    assert spec.description.startswith("First line of help.")
    assert "second paragraph" in spec.description  # FULL docstring, not just line 1
    assert [(p.name, p.type, p.required) for p in spec.params] == [
        ("schema", "str", True),
        ("table", "str", True),
    ]


def test_name_override_and_optional_param():
    class P:
        @probe_method(name="topics")
        def list_topics(self, limit: int = 500) -> list:
            "List topics."
            return []

    spec = _spec(P.list_topics)
    assert spec.command == "topics"
    assert spec.params[0].name == "limit"
    assert spec.params[0].type == "int"
    assert spec.params[0].required is False
    assert spec.params[0].default == 500


def test_optional_annotation_is_not_required():
    from typing import Optional

    class P:
        @probe_method()
        def m(self, database: Optional[str] = None) -> list:
            "m"
            return []

    assert _spec(P.m).params[0].required is False


def test_missing_docstring_rejected():
    with pytest.raises(ValueError):

        class P:
            @probe_method()
            def m(self, a: str) -> list:
                return []


def test_unsupported_param_type_rejected():
    with pytest.raises(TypeError):

        class P:
            @probe_method()
            def m(self, a: dict) -> list:
                "m"
                return []


def test_to_dict_shape():
    class P:
        @probe_method()
        def m(self, a: str) -> list:
            "help"
            return []

    d = _spec(P.m).to_dict()
    assert d == {
        "command": "m",
        "description": "help",
        "params": [{"name": "a", "type": "str", "required": True, "default": None}],
    }


def test_iter_specs_walks_mro_sorted():
    from datahub.ingestion.agent.probe_methods import _iter_specs

    class Base:
        @probe_method()
        def a(self, x: str) -> list:
            "a"
            return []

    class Sub(Base):
        @probe_method()
        def b(self, y: int = 1) -> list:
            "b"
            return []

    assert [c for c, _ in _iter_specs(Sub)] == ["a", "b"]


class _FakeProvider:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    @probe_method()
    def foreign_keys(self, schema: str, table: str) -> list:
        "FKs."
        return [{"schema": schema, "table": table}]


class _FakeConfig:
    @classmethod
    def probe_provider_class(cls):
        return _FakeProvider

    @classmethod
    def model_validate(cls, d):
        return cls()

    def build_probe_provider(self):
        return _FakeProvider()


def _patch(monkeypatch):
    import datahub.ingestion.agent.probe_methods as pm

    monkeypatch.setattr(pm, "_provider_class", lambda st: _FakeProvider)
    monkeypatch.setattr(pm, "_config_class", lambda st: _FakeConfig)
    return pm


def test_list_probe_methods(monkeypatch):
    pm = _patch(monkeypatch)
    assert [s.command for s in pm.list_probe_methods("x")] == ["foreign_keys"]


def test_run_probe_method_dispatches_and_coerces(monkeypatch):
    pm = _patch(monkeypatch)
    res = pm.run_probe_method("x", {}, "foreign_keys", {"schema": "s", "table": "t"})
    assert res.result == [{"schema": "s", "table": "t"}]
    assert res.to_dict()["command"] == "foreign_keys"


def test_run_probe_method_missing_required(monkeypatch):
    pm = _patch(monkeypatch)
    with pytest.raises(ValueError):
        pm.run_probe_method("x", {}, "foreign_keys", {"schema": "s"})


def test_run_probe_method_unknown_command(monkeypatch):
    pm = _patch(monkeypatch)
    with pytest.raises(ValueError):
        pm.run_probe_method("x", {}, "nope", {})


def test_run_probe_method_unknown_param(monkeypatch):
    pm = _patch(monkeypatch)
    with pytest.raises(ValueError):
        pm.run_probe_method(
            "x", {}, "foreign_keys", {"schema": "s", "table": "t", "z": "1"}
        )


def test_list_probe_methods_unknown_source_raises_value_error():
    # Exercises the real registry (no _config_class/_provider_class monkeypatch)
    # so the KeyError -> ValueError guard in _config_class is actually hit.
    with pytest.raises(ValueError):
        list_probe_methods("definitely_not_a_source")


def test_run_probe_method_unknown_source_raises_value_error():
    with pytest.raises(ValueError):
        run_probe_method("definitely_not_a_source", {}, "x", {})


def test_coerce_int_accepts_native_int_float_and_numeric_string():
    param = ProbeParam(name="limit", type="int", required=True)
    assert _coerce(param, 5) == 5
    assert _coerce(param, 5.0) == 5
    assert _coerce(param, "7") == 7


def test_coerce_bool_from_string():
    param = ProbeParam(name="flag", type="bool", required=True)
    assert _coerce(param, "true") is True
    assert _coerce(param, "no") is False
