"""Keep the shipped documentation honest.

Recipe examples rot silently: a config field gets renamed, the docs keep the old name,
and the first thing a new user copies no longer works. These tests make that a CI
failure instead.
"""

import re
from pathlib import Path

import yaml

from datahub.ingestion.source.qualytics.config import QualyticsSourceConfig
from datahub.ingestion.source.qualytics.source import QualyticsSource


def _docs_dir() -> Path:
    # Searched for rather than fixed: the docs sit two levels above these tests here and
    # three in the DataHub monorepo (metadata-ingestion/tests/unit/qualytics/).
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "docs" / "sources" / "qualytics"
        if candidate.is_dir():
            return candidate
    raise AssertionError("docs/sources/qualytics not found above the tests")


DOCS = _docs_dir()
RECIPE = DOCS / "qualytics_recipe.yml"


def test_the_documented_recipe_is_a_valid_config() -> None:
    recipe = yaml.safe_load(RECIPE.read_text())
    config = dict(recipe["source"]["config"])
    # The recipe uses ${QUALYTICS_TOKEN}, which is resolved by DataHub at run time.
    config["token"] = "test"

    # Raises if the shipped recipe has drifted from the config class.
    QualyticsSourceConfig.model_validate(config)


def test_the_recipe_never_inlines_a_token() -> None:
    # A copied-and-pasted literal token in a public repo's example is how secrets get
    # committed by well-meaning users.
    recipe = yaml.safe_load(RECIPE.read_text())
    assert recipe["source"]["config"]["token"].startswith("${")


def test_every_capability_the_source_declares_is_documented() -> None:
    # The standards require a Required Permissions section organised by capability.
    # If a capability is added to the source and not to the docs, users cannot tell
    # what permissions it needs.
    documented = (DOCS / "qualytics_pre.md").read_text()
    # get_capabilities is attached at runtime by the @capability decorator.
    capabilities = QualyticsSource.get_capabilities()  # type: ignore[attr-defined]
    declared = {c.capability.name for c in capabilities}

    missing = {name for name in declared if name not in documented}

    assert missing == set(), (
        f"capabilities missing from the permissions table: {sorted(missing)}"
    )


def test_every_emit_toggle_appears_in_the_recipe() -> None:
    # An undocumented toggle is one nobody discovers.
    toggles = {
        name for name in QualyticsSourceConfig.model_fields if name.startswith("emit_")
    }
    recipe_text = RECIPE.read_text()

    missing = {
        t for t in toggles if not re.search(rf"^\s*#?\s*{t}:", recipe_text, re.M)
    }

    assert missing == set(), (
        f"emit toggles missing from the example recipe: {sorted(missing)}"
    )
