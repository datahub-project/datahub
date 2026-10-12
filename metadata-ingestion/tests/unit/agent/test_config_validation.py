import pathlib
from typing import Dict, List, Optional

import pytest
from click.testing import CliRunner
from pydantic import ConfigDict, ValidationInfo, field_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.recipe import validate_recipe


class _Contextual(ConfigModel):
    flavour: str = ""

    @field_validator("flavour")
    @classmethod
    def _needs_context(cls, v: str, info: ValidationInfo) -> str:
        if v and not (info.context or {}).get("allow_flavour"):
            raise ValueError("flavour is only legal on the flavoured source type")
        return v

    @classmethod
    def probe_validation_context(cls, source_type: str) -> Optional[Dict[str, object]]:
        return {"allow_flavour": source_type == "fake-flavoured"}


def test_the_source_types_context_reaches_the_validators() -> None:
    config = validate_source_config(_Contextual, "fake-flavoured", {"flavour": "x"})
    assert config.flavour == "x"


def test_without_the_context_the_same_recipe_is_refused() -> None:
    with pytest.raises(ValueError) as info:
        validate_source_config(_Contextual, "fake", {"flavour": "x"})
    # The validator's own message is kept: it is what tells the author why.
    assert "only legal on the flavoured source type" in str(info.value)


def test_a_config_declaring_no_context_validates_as_before() -> None:
    class _Plain(ConfigModel):
        name: str = ""

    assert validate_source_config(_Plain, "anything", {"name": "n"}).name == "n"


def test_probe_filter_validates_with_the_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(filter_check, "require_config_class", lambda _st: _Contextual)
    monkeypatch.setattr(filter_check, "list_probe_methods", lambda _st: [])
    result = check_filters(
        source_type="fake-flavoured",
        config_dict={"flavour": "x"},
        kind="Thing",
        parent_path=[],
        names=["a"],
    )
    assert [r.included for r in result.results] == [True]


_MSSQL_BASE: Dict[str, object] = {
    "host_port": "localhost:1433",
    "username": "u",
    "password": "p",
}


def test_an_odbc_recipe_with_uri_args_is_valid_on_mssql_odbc() -> None:
    recipe: Dict[str, object] = {
        "source": {
            "type": "mssql-odbc",
            "config": {
                **_MSSQL_BASE,
                "uri_args": {"driver": "ODBC Driver 18 for SQL Server"},
            },
        }
    }
    report = validate_recipe(recipe)
    assert report["valid"] is True, report["errors"]


def test_uri_args_stay_refused_on_plain_mssql() -> None:
    recipe: Dict[str, object] = {
        "source": {
            "type": "mssql",
            "config": {**_MSSQL_BASE, "uri_args": {"driver": "x"}},
        }
    }
    assert validate_recipe(recipe)["valid"] is False


# Long enough that pydantic truncates its repr: a fragment of it then matches
# no registered secret value, so only not echoing the input at all protects it.
_LONG_SECRET = "PLANTEDhead" + "q" * 120 + "PLANTEDtail"


class _EchoesInput(ConfigModel):
    # What every ConfigModel is under DATAHUB_DEBUG=true: hide_input_in_errors
    # is read once, when the class is created.
    model_config = ConfigDict(hide_input_in_errors=False)

    port: int = 0


def test_a_validation_error_never_echoes_the_input_under_debug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_DEBUG", "true")
    with pytest.raises(ValueError) as info:
        validate_source_config(_EchoesInput, "fake", {"port": _LONG_SECRET})
    assert "PLANTED" not in str(info.value)
    # Still says where and what: the field, and pydantic's error type.
    assert "port" in str(info.value)
    assert "int_parsing" in str(info.value)
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__


def test_probe_run_never_echoes_a_config_input_under_debug(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    import datahub.cli.recipe_cli as rc
    from datahub.cli.recipe_cli import recipe
    from datahub.ingestion.agent import probe_methods

    class _Provider:
        @classmethod
        def for_config(cls, config: object) -> "_Provider":
            return cls()

        def __enter__(self) -> "_Provider":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        @probe_method()
        def things(self) -> List[str]:
            """Things."""
            return []

    class _Config(_EchoesInput):
        @classmethod
        def probe_provider_class(cls) -> type:
            return _Provider

    monkeypatch.setattr(rc, "_stdin_secrets", {})
    monkeypatch.setattr(
        rc,
        "_resolve_for_probe",
        lambda _r: ("fake", {"port": _LONG_SECRET}, {_LONG_SECRET}),
    )
    monkeypatch.setattr(rc, "_ping_probe", lambda *a, **k: None)
    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)
    recipe_file = tmp_path / "r.yml"
    recipe_file.write_text("source:\n  type: fake\n  config: {}\n")
    res = CliRunner().invoke(
        recipe, ["probe", "run", "things", "--recipe", str(recipe_file)]
    )
    assert res.exit_code == 2, res.output
    assert "PLANTED" not in res.output
    assert "port" in res.output
