import os
from typing import Iterator

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeConfig,
    SnowflakeIdentifierConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_queries import (
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


# Every class the suite builds that carries the flag. Pydantic compiles each
# model's defaults at class definition, so each needs its own rebuild — flipping
# only the base is a silent no-op.
#
# SnowflakeQueriesExtractorConfig is deliberately absent: it derives from
# ConfigModel, not SnowflakeIdentifierConfig, and carries no identifier fields.
# Only SnowflakeQueriesSourceConfig mixes the two in.
_CONFIG_MODELS = (
    SnowflakeIdentifierConfig,
    SnowflakeConfig,
    SnowflakeV2Config,
    SnowflakeQueriesSourceConfig,
)


@pytest.fixture(autouse=True, scope="session")
def _preserve_column_case_sweep() -> Iterator[None]:
    if os.environ.get(PRESERVE_COLUMN_CASE_SWEEP) != "1":
        yield
        return

    for model in _CONFIG_MODELS:
        field = model.model_fields.get("preserve_column_case")
        # Catches a rename or removal, which would otherwise make the sweep a
        # no-op for that class without anyone noticing.
        assert field is not None, f"{model.__name__} no longer carries the flag"
        field.default = True
        model.model_rebuild(force=True)

    yield

    # These are class-level defaults, so without this the flip leaks into every
    # Snowflake config built for the rest of the session -- including tests outside
    # this directory, when someone runs a wider selection with the env var set.
    for model in _CONFIG_MODELS:
        model.model_fields["preserve_column_case"].default = False
        model.model_rebuild(force=True)
