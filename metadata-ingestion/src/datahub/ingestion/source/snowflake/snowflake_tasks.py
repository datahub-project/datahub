import functools
import logging
import re
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Set

from datahub.configuration.pattern_utils import is_schema_allowed
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
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
    split_qualified_name,
)
from datahub.ingestion.source.sql.stored_procedures.lineage import (
    ProcedureParseReport,
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


_DEFAULTED_PARAM_RE = re.compile(r"\bDEFAULT\b", re.IGNORECASE)


def _split_top_level(params: str) -> List[str]:
    """Split a parameter list on commas that separate parameters.

    A comma nested in a type's parens (``NUMBER(10,2)``) or inside a string
    literal (``DEFAULT ','``) doesn't separate anything.
    """
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    quote: Optional[str] = None
    for char in params:
        if quote is not None:
            if char == quote:
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        current.append(char)
    parts.append("".join(current))
    return parts


@dataclass(frozen=True)
class _Arity:
    """How many arguments a declared procedure signature accepts."""

    minimum: int
    maximum: int

    def accepts(self, argument_count: Optional[int]) -> bool:
        # Unknown is not a contradiction.
        return argument_count is None or self.minimum <= argument_count <= self.maximum


def _signature_accepts(procedure: BaseProcedure, argument_count: Optional[int]) -> bool:
    arity = _signature_arity(procedure.argument_signature)
    # A signature we couldn't read constrains nothing.
    return arity is None or arity.accepts(argument_count)


def _signature_arity(argument_signature: Optional[str]) -> Optional[_Arity]:
    """Parse a declared signature like ``(arg1 VARCHAR, arg2 INT DEFAULT 1)``.

    Snowflake reports ``()`` for a zero-parameter procedure. A parameter declared
    with a ``DEFAULT`` may be omitted at the call site, hence a range. None means
    no signature was reported, or one we couldn't read.
    """
    if argument_signature is None:
        return None
    signature = argument_signature.strip()
    if not signature.startswith("(") or not signature.endswith(")"):
        return None
    inner = signature[1:-1].strip()
    if not inner:
        return _Arity(minimum=0, maximum=0)
    params = _split_top_level(inner)
    defaulted = sum(1 for param in params if _DEFAULTED_PARAM_RE.search(param))
    return _Arity(minimum=len(params) - defaulted, maximum=len(params))


@dataclass(frozen=True)
class SnowflakeTasksExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    filters: SnowflakeFilter
    # Borrowed from the main SqlParsingAggregator; not ours to close.
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
        # normalised to the same shape and looked up directly. Only the tasks this
        # run emits are in here: an edge to a task_pattern-filtered predecessor
        # would point at a DataJob that was never created.
        task_name_map: Dict[str, SnowflakeTask] = {
            f"{db_name}.{schema_name}.{task.name}".upper(): task
            for task in allowed_tasks
        }
        filtered_task_fqns = {
            f"{db_name}.{schema_name}.{task.name}".upper() for task in tasks
        } - set(task_name_map)

        for task in allowed_tasks:
            self.report.tasks_scanned += 1
            try:
                # Drained inside the guard rather than yielded from it: a `yield`
                # here would also swallow exceptions the consumer throws back in.
                # One malformed task must not halt the rest of the schema.
                workunits = list(
                    self._gen_data_job(
                        task=task,
                        flow_urn=flow_urn,
                        db_name=db_name,
                        schema_name=schema_name,
                        task_name_map=task_name_map,
                        filtered_task_fqns=filtered_task_fqns,
                    )
                )
            except AssertionError:
                # A wiring fault, not a bad task: let it out rather than
                # reporting it as one task among many that failed.
                raise
            except Exception as e:
                self.report.tasks_failed += 1
                self.report.warning(
                    title="Task Extraction Failed",
                    message="Failed to extract metadata for task; task is skipped",
                    context=f"{db_name}.{schema_name}.{task.name}",
                    exc=e,
                )
                continue
            yield from workunits

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
        filtered_task_fqns: Set[str],
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
        filtered_predecessors: List[str] = []
        for predecessor_name in task.predecessors:
            predecessor_fqn = self._predecessor_fqn(
                predecessor_name, db_name, schema_name
            )
            pred_task = task_name_map.get(predecessor_fqn)
            if pred_task is None:
                if predecessor_fqn in filtered_task_fqns:
                    filtered_predecessors.append(predecessor_name)
                else:
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

        if filtered_predecessors:
            self.report.warning(
                title="Predecessor Task Filtered Out",
                message="Predecessor task excluded by task_pattern, so no DataJob exists to link to; input lineage incomplete",
                context=f"{task_fqn} -> {', '.join(filtered_predecessors)}",
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
        if not self.config.include_procedures or not ref.db_schema:
            return None

        # Snowflake folds unquoted identifiers to upper case, so the db/schema in
        # the CALL text may be spelled any way; the task's own db name is already
        # correctly cased, and upper-casing matches Snowflake's folding otherwise.
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
            self.report.warning(
                title="Procedure Lookup Failed",
                message="Could not list procedures; task-to-procedure lineage is incomplete",
                context=f"{database} (called from {task_db}.{task_schema})",
                exc=e,
            )
            return None

        # Take the schema and procedure names from the catalog, not from the CALL
        # text: the catalog casing is what the procedure was ingested under, and
        # with convert_urns_to_lowercase disabled the two are not interchangeable.
        schema, in_schema = next(
            (
                (schema_key, procedures)
                for schema_key, procedures in by_schema.items()
                if schema_key.upper() == ref.db_schema.upper()
            ),
            (ref.db_schema, []),
        )
        name_matches = [
            procedure
            for procedure in in_schema
            if procedure.name.upper() == ref.name.upper()
        ]
        if not name_matches:
            # In a database this run never scanned, or dropped before we got here.
            # Nothing was emitted to point at, so no edge.
            logger.debug(
                f"No ingested procedure named {ref.database}.{ref.db_schema}.{ref.name}"
            )
            return None

        if len(name_matches) == 1:
            # Arity disambiguates overloads; it can't refute a lone match. A
            # declared parameter may be omitted at the call site and our reading
            # of the call may be off, neither of which justifies dropping the
            # only procedure of that name in the schema.
            procedure = name_matches[0]
        else:
            candidates = [
                candidate
                for candidate in name_matches
                if _signature_accepts(candidate, ref.argument_count)
            ]
            if len(candidates) != 1:
                # Same-arity overloads (VARCHAR vs NUMBER) need Snowflake's
                # implicit-cast rules applied to the literals to tell apart.
                self.report.warning(
                    title="Ambiguous Procedure Call",
                    message="Task calls an overloaded procedure that the call site can't disambiguate; no job lineage emitted",
                    context=f"{ref.database}.{ref.db_schema}.{ref.name} called with {ref.argument_count} argument(s)",
                )
                return None
            procedure = candidates[0]

        if not self._procedure_in_scope(database, schema, procedure.name):
            return None

        return procedure.to_urn(
            self.identifiers.gen_database_key(database),
            self.identifiers.gen_schema_key(database, schema),
        )

    def _procedure_in_scope(self, db_name: str, schema_name: str, name: str) -> bool:
        """Would this run have emitted a DataJob for this procedure?

        get_procedures_for_database returns every procedure Snowflake listed; the
        database/schema/procedure patterns are only applied later, in the
        schema-generation loop. A procedure those patterns dropped has no DataJob
        for a task to point at.
        """
        filter_config = self.filters.filter_config
        return (
            filter_config.database_pattern.allowed(db_name)
            and is_schema_allowed(
                filter_config.schema_pattern,
                schema_name,
                db_name,
                filter_config.match_fully_qualified_names,
            )
            and self.filters.is_procedure_allowed(
                self.identifiers.get_dataset_identifier(name, schema_name, db_name)
            )
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
            parse_report = ProcedureParseReport()
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
                    parse_report=parse_report,
                )
            except AssertionError:
                # An ordering/programming fault (see _is_temp_table) is not a
                # parse failure and must not be downgraded into one: fail closed
                # so it surfaces as itself.
                raise
            except Exception as e:
                # Guarded here rather than around the whole task so a body we
                # can't parse still leaves DataJobInfo/SubTypes/Status/Ownership
                # intact — dropping Ownership would silently discard what a
                # previous run wrote.
                self.report.tasks_with_sql_parse_failures += 1
                self.report.warning(
                    title="Task Lineage Extraction Failed",
                    message="Failed to parse task definition; task metadata is still ingested",
                    context=task_fqn,
                    exc=e,
                )
            else:
                self._report_parse_outcome(task_fqn, parse_report, datajob_input_output)

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

    def _report_parse_outcome(
        self,
        task_fqn: str,
        parse_report: ProcedureParseReport,
        datajob_input_output: Optional[DataJobInputOutputClass],
    ) -> None:
        """Say which kind of nothing we got, when we got nothing.

        parse_procedure_code returns None both for a body that failed to parse
        and for one that simply carries no lineage — ``COPY INTO``, ``TRUNCATE``,
        ``ALTER TASK``, ``INSERT ... VALUES``. Warning on the second would fire on
        nearly every run; staying quiet about the first hides the feature
        no-oping. The counts tell them apart.
        """
        if parse_report.failed:
            self.report.tasks_with_sql_parse_failures += 1
            self.report.warning(
                title="Task SQL Parse Failure",
                message="Could not parse part of the task body; its lineage is incomplete",
                context=f"{task_fqn}: {parse_report.statements_failed} statement(s) and "
                f"{parse_report.queries_failed} query(s) failed; first error: {parse_report.first_error}",
            )
        elif parse_report.queries_column_failed and (
            self.config.include_table_lineage and self.config.include_column_lineage
        ):
            # Table-level lineage survived; only the column-level half was lost.
            # Silent when column lineage is switched off, since the aspect would
            # have had its fineGrainedLineages stripped anyway.
            self.report.warning(
                title="Task Column Lineage Parse Failure",
                message="Table-level lineage extracted, but column-level lineage failed for part of the task body",
                context=f"{task_fqn}: {parse_report.queries_column_failed} query(s); "
                f"first error: {parse_report.first_error}",
            )
        elif datajob_input_output is None:
            self.report.tasks_without_sql_lineage += 1
            logger.debug(f"No lineage-bearing statements in task body: {task_fqn}")
