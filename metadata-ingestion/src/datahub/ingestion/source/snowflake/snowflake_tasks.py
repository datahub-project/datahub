import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_group_urn,
    make_schema_field_urn,
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
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    Urn,
    sqlglot_lineage,
)

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskLineage:
    """Dataset-level inputs/outputs and column lineage parsed from a task body.

    All names are URN strings. ``fine_grained`` elements are mutable
    DictWrapper objects; ``frozen=True`` prevents rebinding the tuple fields
    but not mutation of individual entries.
    """

    input_datasets: Tuple[Urn, ...] = field(default_factory=tuple)
    output_datasets: Tuple[Urn, ...] = field(default_factory=tuple)
    fine_grained: Tuple[FineGrainedLineageClass, ...] = field(default_factory=tuple)

    def has_any(self) -> bool:
        return bool(self.input_datasets or self.output_datasets or self.fine_grained)


@dataclass
class SnowflakeTasksExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    schema_resolver: SchemaResolverInterface

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
            simple_name = pred_name_upper.split(".")[-1]
            if simple_name in task_name_map:
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

        lineage = self._parse_task_definition_for_lineage(
            task, task_fqn, db_name, schema_name
        )

        if lineage.has_any() or input_datajobs:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=list(lineage.input_datasets),
                    outputDatasets=list(lineage.output_datasets),
                    inputDatajobs=input_datajobs,
                    fineGrainedLineages=list(lineage.fine_grained) or None,
                ),
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
    ) -> TaskLineage:
        """Parse task SQL to extract dataset-level inputs/outputs and column lineage.

        Multi-statement bodies (e.g. ``stmt1; stmt2``) and unsupported syntax
        like ``CALL <procedure>`` are handled by sqlglot; when sqlglot cannot
        extract table lineage, a warning is emitted and an empty TaskLineage is
        returned. Single-statement INSERT / MERGE / CREATE TABLE AS are cleanly
        supported.
        """
        if not task.definition:
            return TaskLineage()

        parsed = self._run_sql_parser(task, task_fqn, db_name, schema_name)
        if parsed is None:
            return TaskLineage()

        return TaskLineage(
            input_datasets=tuple(parsed.in_tables),
            output_datasets=tuple(parsed.out_tables),
            fine_grained=self._build_fine_grained_lineages(parsed, task_fqn),
        )

    def _run_sql_parser(
        self,
        task: SnowflakeTask,
        task_fqn: str,
        db_name: str,
        schema_name: str,
    ) -> Optional[SqlParsingResult]:
        try:
            result = sqlglot_lineage(
                sql=task.definition,
                schema_resolver=self.schema_resolver,
                default_db=db_name,
                default_schema=schema_name,
            )
            if result.debug_info.table_error:
                self.report.warning(
                    title="Task Table Lineage Extraction Failed",
                    message="Failed to extract table lineage from task definition",
                    context=task_fqn,
                    exc=result.debug_info.table_error,
                )
                return None
        except Exception as e:
            self.report.warning(
                title="Task SQL Parsing Failed",
                message="Failed to parse task definition for lineage",
                context=task_fqn,
                exc=e,
            )
            return None

        return result

    def _build_fine_grained_lineages(
        self, parsed: SqlParsingResult, task_fqn: str
    ) -> Tuple[FineGrainedLineageClass, ...]:
        # Surface column-lineage parser failures so users can tell that table
        # lineage worked but column-level extraction did not.
        if parsed.debug_info.column_error:
            self.report.warning(
                title="Task Column Lineage Extraction Failed",
                message="Failed to extract column lineage from task definition",
                context=task_fqn,
                exc=parsed.debug_info.column_error,
            )
            return ()
        if not parsed.column_lineage:
            return ()

        fine_grained: List[FineGrainedLineageClass] = []
        dropped = 0
        for col_lineage in parsed.column_lineage:
            if (
                not col_lineage.downstream
                or not col_lineage.downstream.table
                or not col_lineage.downstream.column
            ):
                dropped += 1
                continue
            downstream_field = make_schema_field_urn(
                col_lineage.downstream.table, col_lineage.downstream.column
            )
            upstream_fields = [
                make_schema_field_urn(ref.table, ref.column)
                for ref in col_lineage.upstreams
                if ref.table and ref.column
            ]
            if not upstream_fields:
                # An entry with no resolvable upstreams has no lineage value.
                dropped += 1
                continue
            fine_grained.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[downstream_field],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstream_fields,
                )
            )

        total = len(parsed.column_lineage)
        if dropped and not fine_grained:
            self.report.warning(
                title="All Column Lineage Entries Dropped",
                message="All column-lineage entries dropped (missing downstream table/column or upstream fields)",
                context=f"{task_fqn} ({dropped}/{total} entries)",
            )
        elif dropped:
            self.report.warning(
                title="Partial Column Lineage Dropped",
                message="Some column-lineage entries dropped (missing downstream table/column or upstream fields)",
                context=f"{task_fqn} ({dropped}/{total} entries)",
            )
        return tuple(fine_grained)
