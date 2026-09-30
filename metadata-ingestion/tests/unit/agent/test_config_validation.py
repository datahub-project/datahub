from typing import Dict, Optional

import pytest
from pydantic import ValidationError, ValidationInfo, field_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent import filter_check
from datahub.ingestion.agent.config_validation import validate_source_config
from datahub.ingestion.agent.filter_check import check_filters
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
    with pytest.raises(ValidationError):
        validate_source_config(_Contextual, "fake", {"flavour": "x"})


def test_a_config_declaring_no_context_validates_as_before() -> None:
    class _Plain(ConfigModel):
        name: str = ""

    assert validate_source_config(_Plain, "anything", {"name": "n"}).name == "n"


def test_probe_filter_validates_with_the_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(filter_check, "config_class_for", lambda _st: _Contextual)
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
    recipe = {
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
    recipe = {
        "source": {
            "type": "mssql",
            "config": {**_MSSQL_BASE, "uri_args": {"driver": "x"}},
        }
    }
    assert validate_recipe(recipe)["valid"] is False
