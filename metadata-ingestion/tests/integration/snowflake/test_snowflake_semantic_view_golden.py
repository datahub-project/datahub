from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import default_query_results

pytestmark = pytest.mark.integration_batch_1

# Semantic views had no end-to-end coverage at all: no golden file contained one,
# no fixture created one, and every test asserted on a hand-picked slice. That is
# structurally why casing bugs kept surviving here -- a change to one producer
# could alter a part of the output nothing asserted on.
#
# The fixture in common.py is deliberately awkward: a mixed-case logical table
# ("Orders"), a case-only column pair on it, a primary key on a quoted column, a
# join key whose dimension is spelled differently, and a derived metric that
# references another by its quoted name. Recorded with preserve_column_case on,
# because with it off those shapes collapse and the golden pins nothing new.


def _config(**overrides: Any) -> SnowflakeV2Config:
    config = SnowflakeV2Config(
        account_id="ABC12345.ap-south-1.aws",
        username="TST_USR",
        password="TST_PWD",  # type: ignore[arg-type]
        match_fully_qualified_names=True,
        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
        include_technical_schema=True,
        include_table_lineage=True,
        include_usage_stats=False,
        include_operational_stats=False,
        use_queries_v2=False,
        preserve_column_case=True,
        start_time=datetime(2022, 6, 6).replace(tzinfo=timezone.utc),
        end_time=datetime(2022, 6, 7, 7, 17).replace(tzinfo=timezone.utc),
        **overrides,
    )
    config.semantic_views.enabled = True
    config.semantic_views.emit_semantic_model_entities = True
    config.semantic_views.column_lineage = True
    return config


def test_snowflake_semantic_view_golden(pytestconfig: Any, tmp_path: Path) -> None:
    output_file = tmp_path / "snowflake_semantic_view_events.json"
    golden_file = (
        pytestconfig.rootpath
        / "tests/integration/snowflake/snowflake_semantic_view_golden.json"
    )

    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=_config()),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastUpdatedTimestamp'\]",
        ],
    )
