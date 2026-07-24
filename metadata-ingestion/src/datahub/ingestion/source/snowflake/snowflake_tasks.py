import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_group_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DataJobSubTypes,
    FlowContainerSubTypes,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeTask,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    MAX_DEFINITION_LENGTH,
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import parse_procedure_code
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class SnowflakeTasksExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    schema_resolver: SchemaResolver
    is_temp_table: Callable[[str], bool] = field(default=lambda _: False)

    def get_workunits(
        self,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        tasks = self.data_dictionary.get_tasks_for_schema(db_name, schema_name)
        if not tasks:
            return

        allowed_tasks = [
            task
            for task in tasks
            if self.config.task_pattern.allowed(
                f"{db_name}.{schema_name}.{task.name}".upper()
            )
        ]
        if not allowed_tasks:
            return

        flow_id = self.identifiers.snowflake_identifier(
            f"{db_name}.{schema_name}.tasks"
        )
        flow_urn = make_data_flow_urn(
            orchestrator="snowflake",
            flow_id=flow_id,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        yield from self._gen_data_flow(flow_urn, db_name, schema_name)

        task_name_map: Dict[str, SnowflakeTask] = {
            task.name.upper(): task for task in tasks
        }

        for task in allowed_tasks:
            self.report.tasks_scanned += 1
            try:
                yield from self._gen_data_job(
                    task=task,
                    flow_urn=flow_urn,
                    db_name=db_name,
                    schema_name=schema_name,
                    task_name_map=task_name_map,
                )
            except Exception as e:
                self.report.warning(
                    title="Task Extraction Failed",
                    message="Failed to extract metadata for task; skipping remaining aspects",
                    context=f"{db_name}.{schema_name}.{task.name}",
                    exc=e,
                )

    def _gen_data_flow(
        self,
        flow_urn: str,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=f"{db_name}.{schema_name} Tasks",
                description=f"Snowflake Tasks in {db_name}.{schema_name}",
                customProperties={
                    "database": db_name,
                    "schema": schema_name,
                    "object_type": "SNOWFLAKE_TASKS",
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=SubTypesClass(
                typeNames=[FlowContainerSubTypes.SNOWFLAKE_TASK_GROUP],
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

    def _gen_data_job(
        self,
        task: SnowflakeTask,
        flow_urn: str,
        db_name: str,
        schema_name: str,
        task_name_map: Dict[str, SnowflakeTask],
    ) -> Iterable[MetadataWorkUnit]:
        job_id = self.identifiers.snowflake_identifier(task.name)
        job_urn = make_data_job_urn_with_flow(flow_urn, job_id)
        task_fqn = f"{db_name}.{schema_name}.{task.name}"

        custom_properties: Dict[str, str] = {
            "state": task.state.value,
        }
        if task.warehouse:
            custom_properties["warehouse"] = task.warehouse
        if task.schedule:
            custom_properties["schedule"] = task.schedule
        if task.condition:
            custom_properties["condition"] = task.condition
        if task.allow_overlapping_execution:
            custom_properties["allow_overlapping_execution"] = "true"
        if task.definition:
            custom_properties["definition"] = task.definition[:MAX_DEFINITION_LENGTH]

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=task.name,
                description=task.comment,
                type="COMMAND",
                customProperties=custom_properties,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=SubTypesClass(
                typeNames=[DataJobSubTypes.SNOWFLAKE_TASK],
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        input_datajobs: List[str] = []
        unresolved_predecessors: List[str] = []
        for predecessor_name in task.predecessors:
            pred_name_upper = predecessor_name.strip().upper()
            # Predecessors may be fully qualified or just task names
            # Handle both: "DB.SCHEMA.TASK" or just "TASK"
            pred_name_parts = pred_name_upper.split(".")
            simple_name = pred_name_parts[-1]
            # A fully-qualified name only resolves against the current schema's
            # task list if it actually points at this db/schema; otherwise a
            # same-named task in another schema would wrongly match on leaf name
            # alone.
            is_current_schema = len(pred_name_parts) == 1 or (
                len(pred_name_parts) == 3
                and pred_name_parts[0] == db_name.upper()
                and pred_name_parts[1] == schema_name.upper()
            )
            if is_current_schema and simple_name in task_name_map:
                pred_job_id = self.identifiers.snowflake_identifier(simple_name)
                pred_job_urn = make_data_job_urn_with_flow(flow_urn, pred_job_id)
                input_datajobs.append(pred_job_urn)
            else:
                unresolved_predecessors.append(predecessor_name)

        if unresolved_predecessors:
            # Snowflake allows cross-schema task DAGs via fully-qualified
            # predecessor names; those land here when we can't find the
            # predecessor in the current schema's task list.
            self.report.warning(
                title="Predecessor Task Not Found",
                message="Predecessor task not in current schema; input lineage incomplete",
                context=f"{task_fqn} -> {', '.join(unresolved_predecessors)}",
            )

        datajob_input_output = self._parse_task_definition_for_lineage(
            task, task_fqn, db_name, schema_name
        )

        if input_datajobs:
            if datajob_input_output is None:
                datajob_input_output = DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=[],
                    inputDatajobs=input_datajobs,
                )
            else:
                datajob_input_output.inputDatajobs = (
                    list(datajob_input_output.inputDatajobs or []) + input_datajobs
                )

        if datajob_input_output is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=datajob_input_output,
            ).as_workunit()

        if task.owner:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=make_group_urn(task.owner),
                            type=OwnershipTypeClass.TECHNICAL_OWNER,
                        )
                    ]
                ),
            ).as_workunit()

    def _parse_task_definition_for_lineage(
        self,
        task: SnowflakeTask,
        task_fqn: str,
        db_name: str,
        schema_name: str,
    ) -> Optional[DataJobInputOutputClass]:
        """Parse task SQL to extract dataset-level inputs/outputs and column lineage.

        Delegates to the same statement-splitting + SqlParsingAggregator path used
        for stored procedures, so multi-statement task bodies are handled naturally
        instead of being rejected outright.
        """
        if not task.definition or not self.config.include_table_lineage:
            return None

        datajob_input_output = parse_procedure_code(
            schema_resolver=self.schema_resolver,
            default_db=db_name,
            default_schema=schema_name,
            code=task.definition,
            is_temp_table=self.is_temp_table,
            procedure_name=task_fqn,
        )

        if datajob_input_output is None:
            self.report.warning(
                title="Task Lineage Extraction Failed",
                message="Failed to extract lineage from task definition",
                context=task_fqn,
            )
            return None

        if not self.config.include_column_lineage:
            datajob_input_output.fineGrainedLineages = None

        return datajob_input_output
