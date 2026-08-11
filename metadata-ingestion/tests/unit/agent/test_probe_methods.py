from typing import Callable, Dict, List

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
        # Empty because this command takes no container argument. A listing that does
        # -- tables(schema) -- names it here, and the result carries the value so the
        # caller need not restate it as --parent.
        "parent_params": [],
        # None because this command declares no kind: `probe filter` then needs
        # the caller to say, which is only true for commands like `sql`.
        "kind": None,
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
    @classmethod
    def for_config(cls, config):
        return cls()

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


def _patch(monkeypatch):
    import datahub.ingestion.agent.probe_methods as pm

    monkeypatch.setattr(pm, "_provider_class", lambda st: _FakeProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _FakeConfig)
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


def test_run_probe_method_reports_no_warnings_when_provider_has_none(monkeypatch):
    # _FakeProvider exposes no `warnings` attribute at all -- the common case,
    # since most providers have nothing to degrade.
    pm = _patch(monkeypatch)
    res = pm.run_probe_method("x", {}, "foreign_keys", {"schema": "s", "table": "t"})
    assert res.warnings == []


class _FakeProviderWithWarnings(_FakeProvider):
    """A provider that degraded a sub-fetch (see agent.verdicts.ProbeSoftError)
    and reports it via its own `warnings` attribute -- duck-typed, not part
    of the ProbeProvider Protocol, since run_probe_method reads it via
    getattr rather than requiring every provider to declare it."""

    def __init__(self):
        self.warnings = ["definitions listing returned HTTP 403; treating it as empty."]


def test_run_probe_method_surfaces_a_providers_own_warnings(monkeypatch):
    import datahub.ingestion.agent.probe_methods as pm

    monkeypatch.setattr(pm, "_provider_class", lambda st: _FakeProviderWithWarnings)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _FakeConfig)
    res = pm.run_probe_method("x", {}, "foreign_keys", {"schema": "s", "table": "t"})
    assert res.warnings == [
        "definitions listing returned HTTP 403; treating it as empty."
    ]


def test_list_probe_methods_unknown_source_raises_value_error():
    # Exercises the real registry (no config_class_for/_provider_class monkeypatch)
    # so the KeyError -> ValueError guard in config_class_for is actually hit.
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


# --- the gate is wired into the execution path, not just importable ----------
# _enforce_gates and run_probe_method are each covered above and in
# test_scoped_probe_methods, but nothing exercised them TOGETHER: the call
# joining them could be deleted and every other test would still pass. These
# drive a scoped method through run_probe_method so the wiring itself is pinned.


class _GatedProvider:
    """A provider whose sql method declares its raw-SQL parameter."""

    sql_dialect = "postgres"
    ran: List[str] = []

    @classmethod
    def for_config(cls, config: object) -> "_GatedProvider":
        return cls()

    def __enter__(self) -> "_GatedProvider":
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    @probe_method(name="sql", scoped_sql_param="query")
    def sql(self, query: str) -> Dict[str, object]:
        """Run a catalog query."""
        _GatedProvider.ran.append(query)
        return {"ok": True}


class _GatedConfig:
    @classmethod
    def probe_provider_class(cls) -> type:
        return _GatedProvider

    @classmethod
    def model_validate(cls, d: object) -> "_GatedConfig":
        return cls()


def _patch_gated(monkeypatch):
    import datahub.ingestion.agent.probe_methods as pm

    _GatedProvider.ran = []
    monkeypatch.setattr(pm, "_provider_class", lambda st: _GatedProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _GatedConfig)
    return pm


def test_run_probe_method_refuses_a_query_the_gate_rejects(monkeypatch):
    from datahub.ingestion.agent.sql_gate import SqlScopeError

    pm = _patch_gated(monkeypatch)
    with pytest.raises(SqlScopeError):
        pm.run_probe_method("x", {}, "sql", {"query": "SELECT * FROM public.orders"})
    # The provider must never have been called: the gate runs before dispatch.
    assert _GatedProvider.ran == []


def test_run_probe_method_admits_a_query_the_gate_allows(monkeypatch):
    pm = _patch_gated(monkeypatch)
    query = "SELECT table_name FROM information_schema.tables"
    result = pm.run_probe_method("x", {}, "sql", {"query": query})
    assert result.result == {"ok": True}
    assert _GatedProvider.ran == [query]


def test_a_dialect_that_cannot_answer_says_so_instead_of_looking_unreachable(
    monkeypatch,
):
    """An unsupported reflection method must not read as a connection failure.

    SQLAlchemy dialects raise NotImplementedError for reflection they do not support
    -- table_comment on Trino, MSSQL and ClickHouse among them. Without this branch
    the exception reaches recipe_cli's catch-all and exits 3, "I could not reach the
    source", so an agent concludes the source is unreachable and retries. Exit 2 is
    the truth: the connection was fine, the command was the wrong one to ask for.
    """
    import datahub.ingestion.agent.probe_methods as pm

    class _Unsupporting:
        @classmethod
        def for_config(cls, config: object) -> "_Unsupporting":
            return cls()

        def __enter__(self) -> "_Unsupporting":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        @probe_method()
        def table_comment(self, schema: str, table: str) -> dict:
            """The table's stored comment, where the dialect has them."""
            raise NotImplementedError()

    class _Config:
        @classmethod
        def probe_provider_class(cls) -> type:
            return _Unsupporting

        @classmethod
        def model_validate(cls, d: object) -> "_Config":
            return cls()

    monkeypatch.setattr(pm, "_provider_class", lambda st: _Unsupporting)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _Config)

    with pytest.raises(ValueError, match="does not support the 'table_comment'") as err:
        pm.run_probe_method("trino", {}, "table_comment", {"schema": "s", "table": "t"})
    # ValueError is what recipe_cli maps to the user-error exit code; anything else
    # lands in the catch-all and is reported as a connection problem.
    assert "reached" in str(err.value)
