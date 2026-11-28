from typing import List

import pytest

from datahub.ingestion.source.microstrategy.models import Datasource


@pytest.fixture
def mixed_datasources() -> List[Datasource]:
    return [
        Datasource.model_validate(
            {
                "id": "AAA111",
                "name": "oracle_ds",
                "database": {"type": "oracle", "name": "ORA_DB", "schema": "ORA"},
            }
        ),
        Datasource.model_validate(
            {
                "id": "BBB222",
                "name": "snowflake_ds",
                "database": {"type": "snow_flake", "name": "SNOW_DB", "schema": "SNOW"},
            }
        ),
    ]
