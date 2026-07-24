import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest import mock

import pytest

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import default_query_results

pytestmark = pytest.mark.integration_batch_5


def _base_config(**overrides: Any) -> SnowflakeV2Config:
    defaults = dict(
        account_id="ABC12345.ap-south-1.aws",
        username="TST_USR",
        password="TST_PWD",
        match_fully_qualified_names=True,
        schema_pattern=AllowDenyPattern(allow=["test_db.test_schema"]),
        include_technical_schema=True,
        include_table_lineage=False,
        include_column_lineage=False,
        include_usage_stats=False,
        start_time=datetime(2022, 6, 6, 0, 0, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2022, 6, 7, 7, 17, 0, 0, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return SnowflakeV2Config(**defaults)


def _run_pipeline(config: SnowflakeV2Config, output_file: Path) -> SnowflakeV2Report:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = default_query_results

        pipeline = Pipeline(
            config=PipelineConfig(
                source=SourceConfig(type="snowflake", config=config),
                sink=DynamicTypedConfig(
                    type="file", config={"filename": str(output_file)}
                ),
            )
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()
        return cast(SnowflakeV2Report, pipeline.source.get_report())


def test_snowflake_stages_tasks_pipes(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"
    output_file = tmp_path / "snowflake_stages_tasks_pipes_events.json"
    golden_file = test_resources_dir / "snowflake_stages_tasks_pipes_golden.json"

    config = _base_config(
        include_stages=True,
        include_tasks=True,
        include_pipes=True,
    )
    report = _run_pipeline(config, output_file)

    assert report.stages_scanned == 2
    assert report.tasks_scanned == 3
    assert report.pipes_scanned == 1

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['created'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            r"root\[\d+\]\['systemMetadata'\]",
        ],
    )


def test_snowflake_pipes_without_stages_still_resolves_lineage(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """Pipes enabled without stages enabled should still populate stage_lookup for lineage."""
    output_file = tmp_path / "snowflake_pipes_only_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=False,
        include_pipes=True,
    )
    report = _run_pipeline(config, output_file)

    assert report.stages_scanned == 2
    assert report.pipes_scanned == 1

    with open(output_file) as f:
        events = json.load(f)

    entity_types = [e.get("entityType") for e in events]
    assert "dataJob" in entity_types
    assert "dataFlow" in entity_types

    # Stage containers should NOT be emitted (include_stages=False)
    container_events = [
        e
        for e in events
        if e.get("entityType") == "container"
        and e.get("aspectName") == "subTypes"
        and "Snowflake Stage" in str(e.get("aspect", {}).get("json", {}))
    ]
    assert len(container_events) == 0


def test_snowflake_tasks_only(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    """Tasks can be enabled independently of stages and pipes."""
    output_file = tmp_path / "snowflake_tasks_only_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
    )
    report = _run_pipeline(config, output_file)

    assert report.stages_scanned == 0
    assert report.tasks_scanned == 3
    assert report.pipes_scanned == 0


def test_snowflake_task_lineage_extracted_end_to_end(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """With table/column lineage enabled, task SQL bodies are parsed into
    real dataset- and column-level lineage on the DataJobInputOutput aspect."""
    output_file = tmp_path / "snowflake_task_lineage_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
        include_table_lineage=True,
        include_column_lineage=True,
    )
    _run_pipeline(config, output_file)

    with open(output_file) as f:
        events = json.load(f)

    root_task_io = next(
        e["aspect"]["json"]
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and e["entityUrn"].endswith("root_task)")
    )
    assert any("table_2" in u for u in root_task_io["inputDatasets"])
    assert any("table_1" in u for u in root_task_io["outputDatasets"])
    assert root_task_io["fineGrainedLineages"]

    child_task_2_io = next(
        e["aspect"]["json"]
        for e in events
        if e.get("aspectName") == "dataJobInputOutput"
        and e["entityUrn"].endswith("child_task_2)")
    )
    assert any("table_1" in u for u in child_task_2_io["inputDatasets"])
    assert any("table_3" in u for u in child_task_2_io["outputDatasets"])
    assert child_task_2_io["fineGrainedLineages"]
    # Predecessor lineage (from ROOT_TASK) is still combined with the parsed
    # SQL-body lineage on the same aspect.
    assert any("root_task" in u for u in child_task_2_io["inputDatajobs"])

    # CHILD_TASK_1's body is a CALL statement, unsupported by sqlglot's
    # lineage engine — no dataset lineage should be parsed for it.
    child_task_1_io = next(
        (
            e["aspect"]["json"]
            for e in events
            if e.get("aspectName") == "dataJobInputOutput"
            and e["entityUrn"].endswith("child_task_1)")
        ),
        None,
    )
    assert child_task_1_io is not None
    assert child_task_1_io["inputDatasets"] == []
    assert child_task_1_io["outputDatasets"] == []
