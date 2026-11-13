from datetime import datetime
from typing import List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTask,
    SnowflakeTaskState,
)
from datahub.ingestion.source.snowflake.snowflake_tasks import (
    SnowflakeTasksExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    OwnershipClass,
    SubTypesClass,
)


def _make_config() -> SnowflakeV2Config:
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore
        include_tasks=True,
    )


def _make_task(
    name: str = "etl_task",
    definition: str = "INSERT INTO target SELECT * FROM source",
    predecessors: Optional[List[str]] = None,
    state: SnowflakeTaskState = SnowflakeTaskState.STARTED,
    schedule: str = "USING CRON 0 * * * * UTC",
    warehouse: str = "COMPUTE_WH",
) -> SnowflakeTask:
    return SnowflakeTask(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        definition=definition,
        state=state,
        owner_role_type="ROLE",
        comment=f"Task {name}",
        warehouse=warehouse,
        schedule=schedule,
        predecessors=list(predecessors) if predecessors else [],
    )


def _collect_workunits(
    tasks: List[SnowflakeTask],
    config: Optional[SnowflakeV2Config] = None,
) -> tuple:
    if config is None:
        config = _make_config()
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    )
    data_dict = MagicMock()
    data_dict.get_tasks_for_schema.return_value = tasks

    extractor = SnowflakeTasksExtractor(
        config=config,
        report=report,
        data_dictionary=data_dict,
        identifiers=identifiers,
    )
    wus = list(extractor.get_workunits("TEST_DB", "PUBLIC"))
    return wus, report


class TestSnowflakeTasksExtractor:
    def test_no_tasks_emits_nothing(self) -> None:
        wus, report = _collect_workunits([])
        assert len(wus) == 0
        assert report.tasks_scanned == 0

    def test_single_task_emits_flow_and_job(self) -> None:
        task = _make_task()
        wus, report = _collect_workunits([task])

        assert report.tasks_scanned == 1
        assert len(wus) >= 5  # DataFlow(3) + DataJob(at least 3-4)

        # Verify subtypes
        subtype_values = []
        for wu in wus:
            mcp = wu.metadata
            if hasattr(mcp, "aspect") and isinstance(mcp.aspect, SubTypesClass):
                subtype_values.extend(mcp.aspect.typeNames)
        assert "Snowflake Task Group" in subtype_values
        assert "Snowflake Task" in subtype_values

    def test_task_custom_properties(self) -> None:
        task = _make_task(
            schedule="USING CRON 0 * * * * UTC",
            warehouse="MY_WH",
            state=SnowflakeTaskState.STARTED,
        )
        wus, _ = _collect_workunits([task])

        job_infos = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInfoClass)
        ]
        assert len(job_infos) == 1
        props = job_infos[0].customProperties
        assert props["state"] == "STARTED"
        assert props["warehouse"] == "MY_WH"
        assert props["schedule"] == "USING CRON 0 * * * * UTC"

    def test_predecessor_dag(self) -> None:
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["task_a"])
        wus, report = _collect_workunits([task_a, task_b])

        assert report.tasks_scanned == 2

        # Find DataJobInputOutput for task_b
        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        # Only task_b has predecessors, so only 1 DataJobInputOutput
        assert len(input_outputs) == 1
        io = input_outputs[0]
        assert io.inputDatajobs is not None
        assert len(io.inputDatajobs) == 1
        assert "task_a" in io.inputDatajobs[0]

    def test_predecessor_fully_qualified_name(self) -> None:
        """Predecessors can be fully qualified like DB.SCHEMA.TASK_NAME."""
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["TEST_DB.PUBLIC.task_a"])
        wus, _ = _collect_workunits([task_a, task_b])

        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_outputs) == 1
        assert input_outputs[0].inputDatajobs is not None
        assert len(input_outputs[0].inputDatajobs) == 1
        assert "task_a" in input_outputs[0].inputDatajobs[0]

    def test_predecessor_not_in_schema_ignored(self) -> None:
        """Predecessor referencing a task not in the current schema is silently skipped."""
        task = _make_task(name="task_b", predecessors=["nonexistent_task"])
        wus, report = _collect_workunits([task])

        assert report.tasks_scanned == 1
        # No DataJobInputOutput emitted since predecessor not found
        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_outputs) == 0

    def test_ownership_emitted(self) -> None:
        task = _make_task()
        wus, _ = _collect_workunits([task])

        ownerships = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, OwnershipClass)
        ]
        assert len(ownerships) == 1
        assert "ADMIN" in ownerships[0].owners[0].owner

    def test_all_tasks_filtered_emits_nothing(self) -> None:
        config = _make_config()
        config.task_pattern.deny = [".*"]
        wus, report = _collect_workunits([_make_task()], config=config)
        assert len(wus) == 0
        assert report.tasks_scanned == 0

    def test_task_name_map_includes_all_tasks_for_predecessor_resolution(self) -> None:
        """Even if task_a is filtered out by pattern, it should still be in task_name_map
        for predecessor resolution of task_b. Currently we filter then iterate allowed_tasks,
        but build task_name_map from all tasks."""
        config = _make_config()
        config.task_pattern.deny = [".*TASK_A.*"]
        report = SnowflakeV2Report()
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["task_a"])

        data_dict = MagicMock()
        data_dict.get_tasks_for_schema.return_value = [task_a, task_b]

        extractor = SnowflakeTasksExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
        )
        wus = list(extractor.get_workunits("TEST_DB", "PUBLIC"))

        # Only task_b should be scanned
        assert report.tasks_scanned == 1

        # task_b should still have task_a as predecessor (task_name_map built from ALL tasks)
        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_outputs) == 1
        assert input_outputs[0].inputDatajobs is not None
        assert "task_a" in input_outputs[0].inputDatajobs[0]

    def test_multiple_tasks_same_flow(self) -> None:
        tasks = [_make_task(name=f"task_{i}") for i in range(3)]
        wus, report = _collect_workunits(tasks)

        assert report.tasks_scanned == 3

        # Only 1 DataFlow but 3 DataJobs
        flow_subtypes = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
            and "Snowflake Task Group" in wu.metadata.aspect.typeNames
        ]
        job_subtypes = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
            and "Snowflake Task" in wu.metadata.aspect.typeNames
        ]
        assert len(flow_subtypes) == 1
        assert len(job_subtypes) == 3
