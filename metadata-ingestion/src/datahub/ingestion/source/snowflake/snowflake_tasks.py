import functools
import logging
from dataclasses import dataclass
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
    split_qualified_name,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import (
    parse_procedure_code,
)
from datahub.ingestion.source.sql.stored_procedures.models import (
    BaseProcedure,
    ProcedureReference,
)
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


def _signature_arity(argument_signature: Optional[str]) -> Optional[int]:
    """Parameter count from a declared signature like ``(arg1 VARCHAR, arg2 INT)``.

    Snowflake reports ``()`` for a zero-parameter procedure.
    """
    if argument_signature is None:
        return None
    inner = argument_signature.strip()
    if not inner.startswith("(") or not inner.endswith(")"):
        return None
    inner = inner[1:-1].strip()
    return len(inner.split(",")) if inner else 0


@dataclass
class SnowflakeTasksExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    schema_resolver: SchemaResolver
    is_temp_table: Callable[[str], bool]

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

        # Keyed on the upper-cased 3-part FQN so predecessor references can be
        # normalised to the same shape and looked up directly.
        task_name_map: Dict[str, SnowflakeTask] = {
            f"{db_name}.{schema_name}.{task.name}".upper(): task for task in tasks
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
                # Belt-and-braces. The one call that can realistically throw
                # (SQL parsing) guards itself in _parse_task_definition_for_lineage
                # so the other aspects still emit, which should leave this
                # unreachable — but a single malformed task must never halt the
                # rest of the schema.
                self.report.tasks_failed += 1
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
            pred_task = task_name_map.get(
                self._predecessor_fqn(predecessor_name, db_name, schema_name)
            )
            if pred_task is None:
                unresolved_predecessors.append(predecessor_name)
                continue
            # Resolve the URN off the task's own name, not the predecessor
            # literal, so it matches the job_id that task is emitted under
            # (which may be lower-cased by snowflake_identifier).
            pred_job_urn = make_data_job_urn_with_flow(
                flow_urn, self.identifiers.snowflake_identifier(pred_task.name)
            )
            if pred_job_urn not in input_datajobs:
                input_datajobs.append(pred_job_urn)

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
            task, task_fqn, db_name, schema_name, input_datajobs
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

    def _resolve_procedure_urn(
        self, task_db: str, task_schema: str, ref: ProcedureReference
    ) -> Optional[str]:
        """Map a CALL target onto a procedure this run actually ingested.

        The urn of a Snowflake procedure ends in a hash of its *declared*
        argument signature, which the call site can't see, so the only way to
        name it correctly is to look it up among the procedures we fetched.
        Those are already cached from METADATA_EXTRACTION, so this costs no
        extra query.
        """
        if not self.config.include_procedures or not ref.database or not ref.db_schema:
            return None

        # Snowflake folds unquoted identifiers to upper case and quoting is
        # already stripped by the time we see the reference, so `CALL db.sc.p()`
        # and `CALL DB.SC.P()` name the same procedure. When the call points at
        # the task's own db/schema we reuse those verbatim — they came from
        # Snowflake and are correctly cased; otherwise upper-casing matches
        # Snowflake's own folding. A genuinely lower-case quoted database name is
        # not resolvable here, matching the connector's handling elsewhere.
        database = (
            task_db if ref.database.upper() == task_db.upper() else ref.database.upper()
        )

        try:
            # TODO: get_procedures_for_database is @serialized_lru_cache(maxsize=1),
            # so a schema whose tasks call procedures across several databases
            # evicts and refetches per call. Correct, just wasteful — raise maxsize
            # or key a lookup once per run if it shows up in profiling.
            by_schema = self.data_dictionary.get_procedures_for_database(database)
        except Exception as e:
            logger.debug(f"Could not list procedures in {database}: {e}")
            return None

        in_schema: List[BaseProcedure] = next(
            (
                procedures
                for schema_key, procedures in by_schema.items()
                if schema_key.upper() == ref.db_schema.upper()
            ),
            [],
        )
        name_matches = [
            procedure
            for procedure in in_schema
            if procedure.name.upper() == ref.name.upper()
        ]
        if not name_matches:
            # Filtered out by procedure_pattern, or in a database this run never
            # scanned. Nothing was emitted to point at, so no edge.
            logger.debug(
                f"No ingested procedure named {ref.database}.{ref.db_schema}.{ref.name}"
            )
            return None

        candidates = name_matches
        if ref.argument_count is not None:
            # Arity is a constraint, not just a tiebreaker: a call whose argument
            # count matches no overload is one we've mis-read or a procedure that
            # changed shape, and neither justifies inventing an edge. Candidates
            # whose declared signature we couldn't parse stay in — unknown is not
            # a contradiction.
            candidates = [
                procedure
                for procedure in name_matches
                if _signature_arity(procedure.argument_signature)
                in (None, ref.argument_count)
            ]

        if not candidates:
            self.report.warning(
                title="Procedure Call Signature Mismatch",
                message="Task calls a procedure with an argument count no overload declares; no job lineage emitted",
                context=f"{ref.database}.{ref.db_schema}.{ref.name} called with {ref.argument_count} argument(s)",
            )
            return None

        if len(candidates) > 1:
            # Same-arity overloads (VARCHAR vs NUMBER) need Snowflake's
            # implicit-cast rules applied to the literals to tell apart.
            self.report.warning(
                title="Ambiguous Procedure Call",
                message="Task calls an overloaded procedure that the call site can't disambiguate; no job lineage emitted",
                context=f"{ref.database}.{ref.db_schema}.{ref.name}",
            )
            return None

        return candidates[0].to_urn(
            self.identifiers.gen_database_key(ref.database),
            self.identifiers.gen_schema_key(ref.database, ref.db_schema),
        )

    @staticmethod
    def _predecessor_fqn(
        predecessor_name: str,
        db_name: str,
        schema_name: str,
    ) -> str:
        """Normalise a predecessor reference to an upper-cased 3-part FQN.

        Snowflake reports predecessors as ``TASK``, ``SCHEMA.TASK`` or
        ``DB.SCHEMA.TASK``, optionally quoted. Unqualified parts default to the
        task's own database and schema; anything else is returned as-is so it
        simply fails to match and lands in the unresolved bucket.
        """
        parts = [
            part.upper() for part in split_qualified_name(predecessor_name.strip())
        ]
        if len(parts) == 1:
            parts = [db_name.upper(), schema_name.upper(), *parts]
        elif len(parts) == 2:
            parts = [db_name.upper(), *parts]
        return ".".join(parts)

    def _parse_task_definition_for_lineage(
        self,
        task: SnowflakeTask,
        task_fqn: str,
        db_name: str,
        schema_name: str,
        input_datajobs: List[str],
    ) -> Optional[DataJobInputOutputClass]:
        """Build the task's DataJobInputOutput aspect, if it has one.

        Parses the SQL body via the same statement-splitting + SqlParsingAggregator
        path used for stored procedures, so multi-statement bodies are handled
        naturally, and merges in the predecessor DAG edges the caller resolved.
        """
        datajob_input_output: Optional[DataJobInputOutputClass] = None

        if task.definition:
            try:
                datajob_input_output = parse_procedure_code(
                    schema_resolver=self.schema_resolver,
                    default_db=db_name,
                    default_schema=schema_name,
                    code=task.definition,
                    is_temp_table=self.is_temp_table,
                    procedure_name=task_fqn,
                    additional_input_jobs=input_datajobs,
                    resolve_procedure_urn=functools.partial(
                        self._resolve_procedure_urn, db_name, schema_name
                    ),
                )
            except Exception as e:
                # Guarded here rather than around the whole task so a body we
                # can't parse still leaves DataJobInfo/SubTypes/Status/Ownership
                # intact — dropping Ownership would silently discard what a
                # previous run wrote.
                self.report.tasks_failed += 1
                self.report.warning(
                    title="Task Lineage Extraction Failed",
                    message="Failed to parse task definition; task metadata is still ingested",
                    context=task_fqn,
                    exc=e,
                )
            else:
                if datajob_input_output is None:
                    # Not a failure: plenty of valid task bodies carry no lineage
                    # at all (COPY INTO, TRUNCATE, ALTER TASK, DELETE, literal
                    # INSERT ... VALUES). parse_procedure_code returns None for
                    # those as well as for genuine parse errors, and swallows the
                    # latter at debug level inside its throwaway aggregator, so we
                    # can't distinguish the two here without surfacing that
                    # sub-report. Count rather than warn.
                    self.report.tasks_without_sql_lineage += 1
                    logger.debug(
                        f"No lineage-bearing statements in task body: {task_fqn}"
                    )

        if datajob_input_output is None:
            # No body, nothing lineage-bearing in it, or the parse threw. The
            # predecessor DAG edges still deserve an aspect of their own.
            if not input_datajobs:
                return None
            return DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[],
                inputDatajobs=list(input_datajobs),
            )

        if not self.config.include_table_lineage:
            # include_table_lineage is documented as table-to-table lineage, so it
            # strips the dataset halves only. The job-to-job edges stay: they're
            # DAG structure, the predecessor-derived ones have never been gated,
            # and procedures run this same CALL path gated only on
            # include_procedures.
            datajob_input_output.inputDatasets = []
            datajob_input_output.outputDatasets = []
            datajob_input_output.fineGrainedLineages = None
        elif not self.config.include_column_lineage:
            datajob_input_output.fineGrainedLineages = None

        if not (
            datajob_input_output.inputDatasets
            or datajob_input_output.outputDatasets
            or datajob_input_output.inputDatajobs
        ):
            return None

        return datajob_input_output
