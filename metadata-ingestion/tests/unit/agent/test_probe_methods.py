from typing import Callable, Dict, List, Optional

import pytest

import datahub.ingestion.agent.probe_methods as pm
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    ProbeParam,
    _coerce,
    _iter_specs,
    list_probe_methods,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.sql_gate import SqlScopeError


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

    _GatedProvider.ran = []
    monkeypatch.setattr(pm, "_provider_class", lambda st: _GatedProvider)
    monkeypatch.setattr(pm, "config_class_for", lambda st: _GatedConfig)
    return pm


def test_run_probe_method_refuses_a_query_the_gate_rejects(monkeypatch):

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


def test_a_failure_recorded_before_a_raise_is_not_discarded(monkeypatch):
    """The report was only read on the success path.

    A getter can record report.failure() and *then* raise -- Hex's
    _project_id_or_raise raises ProbeSoftError("no project titled 'x'") after
    its /projects fetch already failed and was recorded. Reading the report only
    after a successful return threw that reason away, and ProbeSoftError being a
    ValueError meant the caller was told at exit 2 to fix a title when the
    listing had 401'd.
    """
    from datahub.ingestion.agent.verdicts import ProbeReadFailed, ProbeSoftError

    class _Report:
        def __init__(self) -> None:
            self.failures = ["Listing projects failed: 403 Forbidden"]
            self.warnings: List[str] = []

    class _Prov:
        def __init__(self) -> None:
            self._report = _Report()

        @property
        def probe_report(self) -> object:
            return self._report

        def __enter__(self) -> "_Prov":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        @classmethod
        def for_config(cls, config: object) -> "_Prov":
            return cls()

        @probe_method(name="projects")
        def projects(self) -> object:
            """Records a failure, then raises about the title."""
            raise ProbeSoftError("no project titled 'x' found in this workspace")

    # monkeypatch, not direct assignment: these are module globals, and
    # setting them unrestored leaks into every later test in the session.
    monkeypatch.setattr(pm, "_provider_class", lambda st: _Prov)
    monkeypatch.setattr(
        pm,
        "config_class_for",
        lambda st: type("C", (), {"model_validate": staticmethod(lambda d: None)}),
    )
    with pytest.raises(ProbeReadFailed, match="403 Forbidden") as exc_info:
        pm.run_probe_method("hex", {}, "projects", {})
    # Not a ValueError, so it maps to exit 3 rather than blaming the argument.
    assert not isinstance(exc_info.value, ValueError)


def test_a_plain_failures_list_recorded_before_a_raise_is_not_discarded(monkeypatch):
    """The raise path read only probe_report.

    Both shapes are supported -- the success path merges `provider.failures`
    and `provider.probe_report.failures` -- so a provider using the plain list
    and then raising had its reason dropped and reached the CLI as a user
    error (exit 2, "fix your argument") rather than an unreachable source.
    """
    from datahub.ingestion.agent.verdicts import ProbeReadFailed, ProbeSoftError

    class _Prov:
        def __init__(self) -> None:
            self.failures = ["Listing projects failed: 403 Forbidden"]
            self.warnings: List[str] = []

        def __enter__(self) -> "_Prov":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        @classmethod
        def for_config(cls, config: object) -> "_Prov":
            return cls()

        @probe_method(name="projects")
        def projects(self) -> object:
            """Records a failure on the plain list, then raises."""
            raise ProbeSoftError("no project titled 'x' found in this workspace")

    monkeypatch.setattr(pm, "_provider_class", lambda st: _Prov)
    monkeypatch.setattr(
        pm,
        "config_class_for",
        lambda st: type("C", (), {"model_validate": staticmethod(lambda d: None)}),
    )
    with pytest.raises(ProbeReadFailed, match="403 Forbidden"):
        pm.run_probe_method("hex", {}, "projects", {})


# --- what "required" means -------------------------------------------------


def test_optional_says_nullable_and_the_default_says_omittable():
    """These were conflated: any Optional[...] was advertised required=False,
    so a parameter with no default was described as omittable and then invoked
    without it -- TypeError, which recipe_cli maps to exit 2, blaming the
    caller for input the framework had described as optional."""

    class Provider:
        @probe_method()
        def one(self, must: Optional[str]) -> str:
            """Nullable, but you still have to pass it."""
            return str(must)

        @probe_method()
        def two(self, may: Optional[str] = None) -> str:
            """Nullable and omittable."""
            return str(may)

    specs = {c: s for c, s in _iter_specs(Provider)}
    assert {p.name: p.required for p in specs["one"].params} == {"must": True}
    assert {p.name: p.required for p in specs["two"].params} == {"may": False}


def test_a_plain_parameter_with_a_default_is_still_omittable():
    class Provider:
        @probe_method()
        def cmd(self, needed: str, limit: int = 10) -> str:
            """Two shapes."""
            return needed

    spec = dict(_iter_specs(Provider))["cmd"]
    assert {p.name: p.required for p in spec.params} == {
        "needed": True,
        "limit": False,
    }


class _UnboundedProvider:
    """A listing that declares no row_limit_param -- Mode's spaces, Hex's
    connections and the SQLAlchemy family's columns are all this shape."""

    @classmethod
    def for_config(cls, config):
        return cls()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    @probe_method()
    def everything(self) -> list:
        "No row limit declared."
        return [f"n{i}" for i in range(pm.MAX_PROBE_ITEMS + 25)]


def test_a_listing_with_no_declared_row_limit_is_still_capped(monkeypatch):
    """MAX_PROBE_ITEMS calls itself "the most items any probe command may
    return, whatever the caller asked for", and was wired only to commands
    declaring row_limit_param. Mode's spaces/reports/datasets/queries, Hex's
    connections and the SQLAlchemy family's columns/indexes/foreign_keys all
    returned everything with truncated: false -- and Mode's listings page the
    whole workspace, so a large one returned every report and called the
    answer complete.
    """
    monkeypatch.setattr(pm, "_provider_class", lambda st: _UnboundedProvider)

    class _Config:
        @classmethod
        def probe_provider_class(cls):
            return _UnboundedProvider

        @classmethod
        def model_validate(cls, d):
            return cls()

    monkeypatch.setattr(pm, "config_class_for", lambda st: _Config)

    result = pm.run_probe_method("x", {}, "everything", {})
    assert isinstance(result.result, list)
    assert len(result.result) == pm.MAX_PROBE_ITEMS
    assert result.truncated is True, "a cut-short listing must say so"


def test_a_short_listing_with_no_row_limit_is_not_marked_truncated(monkeypatch):
    """The control -- a cap that always reports truncated is no better than
    one that never does."""

    class _Short(_UnboundedProvider):
        @probe_method()
        def everything(self) -> list:
            "Short."
            return ["a", "b"]

    monkeypatch.setattr(pm, "_provider_class", lambda st: _Short)

    class _Config:
        @classmethod
        def probe_provider_class(cls):
            return _Short

        @classmethod
        def model_validate(cls, d):
            return cls()

    monkeypatch.setattr(pm, "config_class_for", lambda st: _Config)

    result = pm.run_probe_method("x", {}, "everything", {})
    assert result.result == ["a", "b"]
    assert result.truncated is False
