import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast
from unittest import mock

import pytest

from datahub.configuration.common import AllowDenyPattern, DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.testing import mce_helpers
from tests.integration.snowflake.common import RowCountList, default_query_results

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


def _task_row(name: str, definition: str, predecessors: str = "[]") -> Dict[str, Any]:
    return {
        "name": name,
        "created_on": datetime(2021, 6, 8, 0, 0, 0, 0),
        "owner": "ACCOUNTADMIN",
        "database_name": "TEST_DB",
        "schema_name": "TEST_SCHEMA",
        "comment": f"Task {name}",
        "warehouse": "COMPUTE_WH",
        "schedule": "USING CRON 0 * * * * UTC",
        "predecessors": predecessors,
        "state": "STARTED",
        "definition": definition,
        "condition": "",
        "allow_overlapping_execution": "false",
        "owner_role_type": "ROLE",
    }


def _query_results_with_tasks(
    task_rows: List[Dict[str, Any]],
) -> Callable[[str], Any]:
    """default_query_results, with the task list swapped out for this test.

    Editing the shared fixture instead would churn every golden built on it.
    """
    tasks_query = SnowflakeQuery.show_tasks_for_schema("TEST_SCHEMA", "TEST_DB")

    def query_results(query: str, *args: Any, **kwargs: Any) -> Any:
        if query == tasks_query:
            # RowCountList, not a plain list: the connection reads .rowcount off
            # whatever the cursor returns.
            return RowCountList(task_rows)
        return default_query_results(query, *args, **kwargs)

    return query_results


def _run_pipeline(
    config: SnowflakeV2Config,
    output_file: Path,
    query_results: Optional[Callable[[str], Any]] = None,
) -> SnowflakeV2Report:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf_connection = mock.MagicMock()
        sf_cursor = mock.MagicMock()
        mock_connect.return_value = sf_connection
        sf_connection.cursor.return_value = sf_cursor
        sf_cursor.execute.side_effect = query_results or default_query_results

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
    real dataset- and column-level lineage on the DataJobInputOutput aspect.

    Snapshotted rather than hand-asserted so the full aspect shape is covered —
    the exact column pairs in fineGrainedLineages, every inputDatajobs urn
    (including the CALL-derived one on CHILD_TASK_1), and the absence of any
    spurious event.

    CHILD_TASK_1's CALL-derived edge is the interesting one: its body is
    ``CALL TEST_DB.TEST_SCHEMA.MY_PROCEDURE('arg1')`` and the fixture defines two
    ``my_procedure`` overloads, so the golden pins that the one-argument overload's
    hashed, lower-cased urn is what gets emitted.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/snowflake"
    output_file = tmp_path / "snowflake_task_lineage_events.json"
    golden_file = test_resources_dir / "snowflake_task_lineage_golden.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
        include_table_lineage=True,
        include_column_lineage=True,
    )
    report = _run_pipeline(config, output_file)

    # The golden file can't see these: nothing about a failed parse or a dropped
    # edge reaches the file sink.
    assert report.tasks_scanned == 3
    assert report.tasks_failed == 0
    assert report.tasks_with_sql_parse_failures == 0
    assert not report.warnings

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


def _aspects(
    output_file: Path, entity_urn_fragment: str, aspect_name: str
) -> List[Any]:
    with open(output_file) as f:
        events = json.load(f)
    return [
        e["aspect"]["json"]
        for e in events
        if e.get("aspectName") == aspect_name
        and entity_urn_fragment in (e.get("entityUrn") or "")
    ]


def test_snowflake_task_sql_parse_failure_is_reported(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """A task body that fails to parse has to say so. Counting it as
    "no lineage here" would hide the feature no-oping on every run."""
    output_file = tmp_path / "snowflake_task_parse_failure_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
        include_table_lineage=True,
        include_column_lineage=True,
    )
    report = _run_pipeline(
        config,
        output_file,
        _query_results_with_tasks(
            [_task_row("BROKEN_TASK", "INSERT INTO tgt SELECT FROM WHERE ((")]
        ),
    )

    assert report.tasks_scanned == 1
    assert report.tasks_with_sql_parse_failures == 1
    assert report.tasks_without_sql_lineage == 0
    assert report.tasks_failed == 0
    titles = [w.title for w in report.warnings]
    assert any("Task SQL Parse Failure" in (t or "") for t in titles), titles

    # The task itself is still fully ingested — a failed parse costs lineage only.
    assert _aspects(output_file, "broken_task", "dataJobInfo")
    assert _aspects(output_file, "broken_task", "ownership")
    assert not _aspects(output_file, "broken_task", "dataJobInputOutput")


def test_snowflake_task_without_lineage_is_counted_not_warned(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """COPY INTO is the canonical Snowflake task body and carries no lineage
    sqlglot can read. That is not a failure, and must not warn."""
    output_file = tmp_path / "snowflake_task_no_lineage_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
        include_table_lineage=True,
        include_column_lineage=True,
    )
    report = _run_pipeline(
        config,
        output_file,
        _query_results_with_tasks(
            [
                _task_row(
                    "LOAD_TASK", "COPY INTO TEST_DB.TEST_SCHEMA.TABLE_1 FROM @my_stage"
                ),
                _task_row("CLEANUP_TASK", "TRUNCATE TABLE TEST_DB.TEST_SCHEMA.TABLE_2"),
            ]
        ),
    )

    assert report.tasks_scanned == 2
    assert report.tasks_without_sql_lineage == 2
    assert report.tasks_with_sql_parse_failures == 0
    assert not report.warnings


def test_snowflake_filtered_predecessor_emits_no_datajob_edge(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """task_pattern excluded the predecessor, so no DataJob exists for it. The
    edge is dropped and reported rather than left pointing at nothing."""
    output_file = tmp_path / "snowflake_filtered_predecessor_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
        task_pattern=AllowDenyPattern(deny=[".*ROOT_TASK.*"]),
    )
    report = _run_pipeline(config, output_file)

    assert report.tasks_scanned == 2
    titles = [w.title for w in report.warnings]
    assert any("Predecessor Task Filtered Out" in (t or "") for t in titles), titles

    # The children are still ingested, and ROOT_TASK is emitted nowhere — neither
    # as an entity nor as an edge.
    assert _aspects(output_file, "child_task_1", "dataJobInfo")
    assert _aspects(output_file, "child_task_2", "dataJobInfo")
    assert not _aspects(output_file, "root_task", "dataJobInfo")
    with open(output_file) as f:
        assert "root_task" not in f.read()

    # Positive control: the same fixture without the deny pattern does produce
    # that edge, so the assertions above are about the filter, not the fixture.
    unfiltered_output = output_file.parent / "snowflake_unfiltered_predecessor.json"
    _run_pipeline(
        _base_config(include_stages=False, include_tasks=True, include_pipes=False),
        unfiltered_output,
    )
    assert any(
        any("root_task" in urn for urn in (io.get("inputDatajobs") or []))
        for io in _aspects(unfiltered_output, "child_task_1", "dataJobInputOutput")
    )


def test_snowflake_task_call_edge_respects_procedure_pattern(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    """CHILD_TASK_1 calls MY_PROCEDURE. With that procedure filtered out no
    DataJob is emitted for it, so the CALL edge has to go too."""
    output_file = tmp_path / "snowflake_task_filtered_procedure_events.json"

    config = _base_config(
        include_stages=False,
        include_tasks=True,
        include_pipes=False,
        include_procedures=True,
        procedure_pattern=AllowDenyPattern(deny=[".*my_procedure.*"]),
    )
    report = _run_pipeline(config, output_file)
    assert report.tasks_scanned == 3

    def call_edges(path: Path) -> List[str]:
        return [
            urn
            for io in _aspects(path, "child_task_1", "dataJobInputOutput")
            for urn in (io.get("inputDatajobs") or [])
            if "stored_procedures" in urn
        ]

    assert call_edges(output_file) == []

    # Positive control: with the procedure allowed, the very same body resolves to
    # the procedure's DataJob — so the empty result above is the filter at work.
    allowed_output = output_file.parent / "snowflake_task_allowed_procedure.json"
    _run_pipeline(
        _base_config(
            include_stages=False,
            include_tasks=True,
            include_pipes=False,
            include_procedures=True,
        ),
        allowed_output,
    )
    assert call_edges(allowed_output), "expected a CALL-derived edge when allowed"
