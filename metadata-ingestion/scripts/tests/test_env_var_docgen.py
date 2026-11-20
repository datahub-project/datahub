import pathlib
import sys
from typing import Any, Dict

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from env_var_docgen import (
    backtick_known_names,
    format_default,
    format_description,
    render,
)

from datahub.configuration.env_var_registry import (
    EnvVarAudience,
    EnvVarGroup,
    EnvVarSpec,
    EnvVarTier,
)


def spec(**overrides: Any) -> EnvVarSpec:
    kwargs: Dict[str, Any] = {
        "name": "DATAHUB_EXAMPLE",
        "type": str,
        "description": "An example variable.",
        "group": EnvVarGroup.MISC,
    }
    kwargs.update(overrides)
    return EnvVarSpec(**kwargs)


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, "_(unset)_"),
        ("", "_(empty)_"),
        (True, "`true`"),
        (False, "`false`"),
        (15, "`15`"),
    ],
)
def test_format_default(value: Any, expected: str) -> None:
    assert format_default(spec(default=value)) == expected


def test_deprecated_rows_name_their_replacement() -> None:
    rendered = format_description(
        spec(deprecated=True, replaced_by="DATAHUB_GMS_URL", deprecated_in="0.15.0"),
        frozenset({"DATAHUB_EXAMPLE", "DATAHUB_GMS_URL"}),
    )
    assert "**Deprecated since 0.15.0.**" in rendered
    assert "Use `DATAHUB_GMS_URL` instead." in rendered


def test_secret_and_autodetected_rows_are_flagged() -> None:
    assert "**Secret.**" in format_description(spec(secret=True), frozenset())
    assert "**Detected, not set by you.**" in format_description(
        spec(audience=EnvVarAudience.AUTO_DETECTED), frozenset()
    )


def test_choices_and_recipe_field_are_rendered() -> None:
    rendered = format_description(
        spec(choices=("SYNC", "ASYNC"), recipe_field="mode"), frozenset()
    )
    assert "Accepts: `SYNC`, `ASYNC`." in rendered
    assert "Recipe field: `mode` (takes precedence)." in rendered


def test_known_names_are_backticked_once() -> None:
    known = frozenset({"DATAHUB_GMS_URL"})
    assert (
        backtick_known_names("Use DATAHUB_GMS_URL.", known) == "Use `DATAHUB_GMS_URL`."
    )
    assert (
        backtick_known_names("Use `DATAHUB_GMS_URL`.", known)
        == "Use `DATAHUB_GMS_URL`."
    )
    assert backtick_known_names("Use SOME_OTHER_VAR.", known) == "Use SOME_OTHER_VAR."


def test_pipes_in_descriptions_do_not_break_the_table() -> None:
    rendered = format_description(spec(description="Either a | or b."), frozenset())
    assert "\\|" in rendered


def test_render_groups_in_declaration_order_and_tags_non_default_tiers() -> None:
    out = render(
        [
            spec(name="DATAHUB_LATER", group=EnvVarGroup.MISC),
            spec(
                name="DATAHUB_FIRST",
                group=EnvVarGroup.CONNECTION,
                tier=EnvVarTier.BASIC,
            ),
            spec(
                name="DATAHUB_RISKY",
                group=EnvVarGroup.MISC,
                tier=EnvVarTier.EXPERIMENTAL,
            ),
        ]
    )
    assert out.index(EnvVarGroup.CONNECTION.value) < out.index(EnvVarGroup.MISC.value)
    assert "`DATAHUB_FIRST`<br />_Basic_" in out
    assert "`DATAHUB_RISKY`<br />_Experimental_" in out
    # Advanced is the default tier and is deliberately untagged.
    assert "`DATAHUB_LATER` |" in out
    assert "_Advanced_" not in out
    assert out.count("<div className='config-table'>") == 2
