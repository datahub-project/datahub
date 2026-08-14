import os

import pytest

from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeIdentifierConfig,
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


@pytest.fixture(autouse=True, scope="session")
def _preserve_column_case_sweep() -> None:
    if os.environ.get(PRESERVE_COLUMN_CASE_SWEEP) != "1":
        return

    field = SnowflakeIdentifierConfig.model_fields["preserve_column_case"]
    field.default = True
    SnowflakeIdentifierConfig.model_rebuild(force=True)
