from typing import Callable

import pytest

from datahub.ingestion.agent.probe_methods import ProbeMethodSpec, probe_method


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
