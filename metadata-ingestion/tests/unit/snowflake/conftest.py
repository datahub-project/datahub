import os
from typing import Iterator, Type

import pytest

# Imported for their side effect: a subclass must be defined before
# __subclasses__() can see it, and these are the configs the suite builds.
from datahub.ingestion.source.snowflake.snowflake_config import (  # noqa: F401
    SnowflakeIdentifierConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_queries import (  # noqa: F401
    SnowflakeQueriesSourceConfig,
)

# An opt-in flag gets the whole existing suite as a regression net for its OFF
# path and nothing for its ON path. Setting DATAHUB_TEST_PRESERVE_COLUMN_CASE=1
# flips the default so every Snowflake test runs with the flag enabled, which is
# what surfaces aspects that disagree with each other on a column's field path.
#
#   DATAHUB_TEST_PRESERVE_COLUMN_CASE=1 pytest tests/unit/snowflake/
#
# Tests that set preserve_column_case explicitly are unaffected.
PRESERVE_COLUMN_CASE_SWEEP = "DATAHUB_TEST_PRESERVE_COLUMN_CASE"


def _config_models() -> Iterator[Type[SnowflakeIdentifierConfig]]:
    """Every config class that carries the flag, base and subclasses alike.

    Pydantic compiles each model's defaults into its own schema at class
    definition, so rebuilding only the base leaves subclasses untouched — and
    the suite builds subclasses (`SnowflakeV2Config`, `SnowflakeQueriesSourceConfig`),
    not the base. Rebuilding just the base makes the sweep a silent no-op.
    """

    def walk(cls: type) -> Iterator[type]:
        for subclass in cls.__subclasses__():
            yield subclass
            yield from walk(subclass)

    yield SnowflakeIdentifierConfig
    yield from walk(SnowflakeIdentifierConfig)  # type: ignore[misc]


@pytest.fixture(autouse=True, scope="session")
def _preserve_column_case_sweep() -> None:
    if os.environ.get(PRESERVE_COLUMN_CASE_SWEEP) != "1":
        return

    flipped = []
    for model in _config_models():
        field = model.model_fields.get("preserve_column_case")
        if field is None or field.default is True:
            continue
        field.default = True
        model.model_rebuild(force=True)
        flipped.append(model.__name__)

    # Fail loudly rather than reporting a green run that proved nothing.
    assert "SnowflakeV2Config" in flipped, (
        f"sweep did not reach SnowflakeV2Config; flipped only {flipped}"
    )
