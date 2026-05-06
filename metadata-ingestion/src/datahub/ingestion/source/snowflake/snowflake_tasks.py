import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

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
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult, sqlglot_lineage

logger: logging.Logger = logging.getLogger(__name__)

# Truncate the task definition stored in customProperties to stay well within
# DataHub's aspect size limits.
_MAX_DEFINITION_LENGTH = 4000


@dataclass
class SnowflakeTasksExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    aggregator: SqlParsingAggregator

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

            yield from self._gen_data_job(
                task=task,
                flow_urn=flow_urn,
                db_name=db_name,
                schema_name=schema_name,
                task_name_map=task_name_map,
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
            custom_properties["definition"] = task.definition[:_MAX_DEFINITION_LENGTH]

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
        for predecessor_name in task.predecessors:
            pred_name_upper = predecessor_name.strip().upper()
            # Predecessors may be fully qualified or just task names
            # Handle both: "DB.SCHEMA.TASK" or just "TASK"
            simple_name = pred_name_upper.split(".")[-1]
            if simple_name in task_name_map:
                pred_job_id = self.identifiers.snowflake_identifier(simple_name)
                pred_job_urn = make_data_job_urn_with_flow(flow_urn, pred_job_id)
                input_datajobs.append(pred_job_urn)

        input_datasets, output_datasets, fine_grained_lineages = (
            self._parse_task_definition_for_lineage(task, db_name, schema_name)
        )

        if input_datasets or output_datasets or input_datajobs:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=output_datasets,
                    inputDatajobs=input_datajobs,
                    fineGrainedLineages=fine_grained_lineages or None,
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
        db_name: str,
        schema_name: str,
    ) -> "tuple[List[str], List[str], List[FineGrainedLineageClass]]":
        """Parse task SQL to extract dataset-level inputs/outputs and column lineage.

        Multi-statement bodies (e.g. ``stmt1; stmt2``) and unsupported syntax
        like ``CALL <procedure>`` are skipped with a warning — sqlglot's lineage
        engine handles single-statement INSERT / MERGE / CREATE TABLE AS cleanly.
        """
        if not task.definition:
            return [], [], []

        parsed = self._run_sql_parser(task, db_name, schema_name)
        if parsed is None:
            return [], [], []

        return (
            list(parsed.in_tables),
            list(parsed.out_tables),
            self._build_fine_grained_lineages(parsed),
        )

    def _run_sql_parser(
        self,
        task: SnowflakeTask,
        db_name: str,
        schema_name: str,
    ) -> Optional[SqlParsingResult]:
        try:
            result = sqlglot_lineage(
                sql=task.definition,
                schema_resolver=self.aggregator._schema_resolver,
                default_db=db_name,
                default_schema=schema_name,
            )
        except Exception as e:
            self.report.warning(
                "Failed to parse task definition for lineage",
                f"{db_name}.{schema_name}.{task.name}",
                exc=e,
            )
            return None

        if result.debug_info.table_error:
            self.report.warning(
                "Failed to extract table lineage from task definition",
                f"{db_name}.{schema_name}.{task.name}",
                exc=result.debug_info.table_error,
            )
            return None
        return result

    def _build_fine_grained_lineages(
        self, parsed: SqlParsingResult
    ) -> List[FineGrainedLineageClass]:
        if parsed.debug_info.column_error or not parsed.column_lineage:
            return []

        fine_grained: List[FineGrainedLineageClass] = []
        for cll in parsed.column_lineage:
            if (
                not cll.downstream
                or not cll.downstream.table
                or not cll.downstream.column
            ):
                continue
            downstream_field = make_schema_field_urn(
                cll.downstream.table, cll.downstream.column
            )
            upstream_fields = [
                make_schema_field_urn(ref.table, ref.column) for ref in cll.upstreams
            ]
            fine_grained.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[downstream_field],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstream_fields,
                )
            )
        return fine_grained
