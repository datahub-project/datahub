import logging
import re
import uuid
from dataclasses import dataclass
from typing import Callable, List, Optional, Set, Tuple

import sqlglot
from sqlglot.dialects.dialect import Dialect

from datahub.emitter.mce_builder import DEFAULT_ENV, make_data_job_urn
from datahub.ingestion.source.sql.stored_procedures.constants import (
    STORED_PROCEDURES_CONTAINER,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.datajob import to_datajob_input_output
from datahub.sql_parsing.query_types import get_query_type_of_sql
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sql_parsing_common import QueryType, get_dialect_str
from datahub.sql_parsing.sqlglot_utils import (
    get_dialect,
    is_dialect_instance,
    parse_statement,
)

logger = logging.getLogger(__name__)

# Match a fully or partially qualified procedure identifier as it appears in a
# CALL/EXEC statement. We don't try to parse argument lists — sqlglot has
# already shown us they're unparseable across the dialects we care about — so
# we just extract the dotted identifier prefix. Square-bracket quoting is
# accepted to support TSQL ``[db].[schema].[proc]`` syntax; backtick and
# double-quote quoting are accepted for MySQL/PostgreSQL.
_QUOTED_IDENT = r"(?:`[^`]+`|\"[^\"]+\"|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_$#]*)"
_CALL_TARGET_RE = re.compile(
    rf"^\s*(?:CALL|EXEC|EXECUTE)\s+"
    rf"(?P<part1>{_QUOTED_IDENT})"
    rf"(?:\s*\.\s*(?P<part2>{_QUOTED_IDENT}))?"
    rf"(?:\s*\.\s*(?P<part3>{_QUOTED_IDENT}))?",
    re.IGNORECASE,
)

# split_statements splits on ``;``, gluing a block-opening ``BEGIN`` onto the first
# body statement (e.g. ``BEGIN\n CALL foo()``), which then fails to parse and drops
# the statement. Strip the opener (``label:``, ``BEGIN``, ``BEGIN NOT ATOMIC``); TSQL
# ``BEGIN TRY/CATCH/TRANSACTION`` are left alone (no lineage, handled elsewhere).
_LEADING_BLOCK_OPENER_RE = re.compile(
    r"^\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*:\s*)?"
    r"BEGIN\b"
    r"(?!\s+(?:TRY|CATCH|TRANSACTION|TRAN)\b)"
    r"(?:\s+NOT\s+ATOMIC\b)?"
    r"\s*",
    re.IGNORECASE,
)

# A block/control closer carries no lineage: bare ``END``, a labeled MariaDB block
# closer (``END my_label``), or a compound-statement closer (``END IF/WHILE/LOOP/CASE``).
_BLOCK_CLOSER_RE = re.compile(
    r"^\s*END(?:\s+[A-Za-z_][A-Za-z0-9_]*)?\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _ProcedureCall:
    """A reference to a stored procedure invoked from another procedure body."""

    database: Optional[str]
    schema: Optional[str]
    name: str


def _strip_identifier_quotes(ident: str) -> str:
    """Drop surrounding ``[]``, ``""``, or backtick quoting from an identifier."""
    if len(ident) >= 2 and ident[0] == ident[-1] and ident[0] in ('"', "`"):
        return ident[1:-1]
    if len(ident) >= 2 and ident[0] == "[" and ident[-1] == "]":
        return ident[1:-1]
    return ident


def _parse_call_target(literal: str) -> Optional[_ProcedureCall]:
    """Parse the ``CALL <name>...`` / ``EXEC <name>...`` literal sqlglot hands us.

    Returns ``None`` when the literal isn't recognisably a procedure call (e.g.
    sqlglot has captured an unrelated ``Command`` we shouldn't treat as one).
    """
    match = _CALL_TARGET_RE.match(literal)
    if not match:
        return None

    parts: List[str] = [
        _strip_identifier_quotes(p)
        for p in (match.group("part1"), match.group("part2"), match.group("part3"))
        if p is not None
    ]

    if len(parts) == 1:
        return _ProcedureCall(database=None, schema=None, name=parts[0])
    if len(parts) == 2:
        # Caller resolves the leading qualifier against default_db / default_schema
        # based on whether the source is two-tier or three-tier.
        return _ProcedureCall(database=None, schema=parts[0], name=parts[1])
    return _ProcedureCall(database=parts[0], schema=parts[1], name=parts[2])


def _extract_procedure_call(parsed: sqlglot.exp.Expression) -> Optional[_ProcedureCall]:
    """Return the called procedure for a CALL/EXEC statement, or ``None``.

    Handles two shapes sqlglot produces depending on dialect:

    * ``Execute`` (TSQL ``EXEC``/``EXECUTE``): structured node with a ``Table``
      child carrying ``db``/``catalog``/``this`` identifiers.
    * ``Command`` (MySQL/PG/Snowflake/Oracle ``CALL``, plus TSQL fallback):
      unparseable; rest of the statement is captured as a literal string. We
      regex out the dotted target.
    """
    if isinstance(parsed, sqlglot.exp.Execute):
        target = parsed.this
        if isinstance(target, sqlglot.exp.Table):
            return _ProcedureCall(
                database=target.args["catalog"].name
                if target.args.get("catalog")
                else None,
                schema=target.args["db"].name if target.args.get("db") else None,
                name=target.name,
            )
        return None

    if isinstance(parsed, sqlglot.exp.Command):
        keyword = (parsed.this or "").upper()
        if keyword not in {"CALL", "EXEC", "EXECUTE"}:
            return None
        # Rebuild the surface form sqlglot saw so the regex sees the keyword too.
        expression = parsed.args.get("expression")
        literal = expression.name if expression is not None else ""
        return _parse_call_target(f"{keyword} {literal}")

    return None


def _build_call_datajob_urn(
    *,
    call: _ProcedureCall,
    schema_resolver: SchemaResolver,
    default_db: Optional[str],
    default_schema: Optional[str],
) -> Optional[str]:
    """Compose a DataJob URN for ``call`` using caller defaults to fill gaps.

    Two-tier sources (MySQL/MariaDB) signal themselves by passing
    ``default_schema=None``; in that mode a leading qualifier on the CALL is
    treated as the database rather than the schema, matching how table URNs
    are composed for the same sources.

    Note: the called procedure's argument signature is unavailable at the call
    site, so the resulting URN never carries the ``_<hash>`` suffix that
    ``BaseProcedure.get_procedure_identifier`` adds for procedures with
    arguments. For sources that emit argument-hashed procedure URNs (Oracle,
    MSSQL), call-site lineage will only match procedures whose URNs are
    unhashed. Two-tier sources (MySQL/MariaDB) never hash, so this caveat is
    only relevant for three-tier sources that populate ``argument_signature``.
    """
    is_two_tier = default_schema is None

    if is_two_tier:
        # In two-tier, the schema slot doesn't exist. Anything the parser put
        # in ``call.schema`` is really a database qualifier.
        database = call.database or call.schema or default_db
        schema: Optional[str] = None
    else:
        database = call.database or default_db
        schema = call.schema or default_schema

    if not database and not schema:
        # Can't compose a flow URN with nothing — skip silently.
        return None

    flow_parts = [p for p in (database, schema, STORED_PROCEDURES_CONTAINER) if p]
    flow_name = ".".join(flow_parts)

    return make_data_job_urn(
        orchestrator=schema_resolver.platform,
        flow_id=flow_name,
        job_id=call.name,
        cluster=schema_resolver.env or DEFAULT_ENV,
        platform_instance=schema_resolver.platform_instance,
    )


# TSQL control flow keywords that don't produce lineage
TSQL_CONTROL_FLOW_KEYWORDS = {
    "BEGIN",
    "END",
    "BEGIN TRY",
    "END TRY",
    "BEGIN CATCH",
    "END CATCH",
    "BEGIN TRANSACTION",
    "BEGIN TRAN",
    "COMMIT",
    "ROLLBACK",
    "SAVE TRANSACTION",
    "SAVE TRAN",
    "DECLARE",
    "SET",
    "IF",
    "ELSE",
    "WHILE",
    "BREAK",
    "CONTINUE",
    "RETURN",
    "GOTO",
    "THROW",
    "EXECUTE",
    "EXEC",
    "GO",
    "PRINT",
    "RAISERROR",
    "WAITFOR",
}

# Sort keywords by length descending to ensure longest match wins
# This prevents shorter keywords from matching first (e.g., "BEGIN" before "BEGIN TRANSACTION")
_TSQL_CONTROL_FLOW_KEYWORDS_SORTED = sorted(
    TSQL_CONTROL_FLOW_KEYWORDS, key=len, reverse=True
)


def _is_tsql_control_flow_statement(stmt_upper: str) -> bool:
    """Check if statement starts with a TSQL control flow keyword with word boundary."""
    for kw in _TSQL_CONTROL_FLOW_KEYWORDS_SORTED:
        if stmt_upper.startswith(kw):
            if len(stmt_upper) == len(kw):
                return True
            next_char = stmt_upper[len(kw)]
            if not next_char.isalnum() and next_char != "_":
                return True
    return False


def _classify_statements(
    *,
    statements: List[str],
    dialect: Dialect,
    platform: str,
    procedure_name: Optional[str],
) -> Tuple[List[str], List[_ProcedureCall]]:
    """Classify each statement as DML, a procedure call, or neither.

    Returns ``(dml_statements, procedure_calls)``. DML statements are returned
    as the original SQL strings so the aggregator can re-parse them in its
    own context; procedure calls are returned as structured ``_ProcedureCall``
    records so the caller can resolve them to DataJob URNs.
    """
    dml_statements: List[str] = []
    procedure_calls: List[_ProcedureCall] = []

    for stmt in statements:
        stmt_stripped = stmt.strip()
        if not stmt_stripped:
            continue

        stmt_upper = stmt_stripped.upper()

        # Skip CREATE PROCEDURE wrapper (prevent recursion)
        if stmt_upper.startswith("CREATE PROCEDURE") or stmt_upper.startswith(
            "CREATE OR REPLACE PROCEDURE"
        ):
            continue

        # Unwrap a glued-on BEGIN so the underlying CALL/DML isn't lost.
        stmt_stripped = _LEADING_BLOCK_OPENER_RE.sub("", stmt_stripped, count=1)
        if not stmt_stripped:
            continue

        # Explicitly drop block closers (``END``, ``END my_label``, ``END IF`` ...);
        # they'd parse to UNKNOWN and be dropped anyway, but skip them by design.
        if _BLOCK_CLOSER_RE.match(stmt_stripped):
            continue
        stmt_upper = stmt_stripped.upper()

        # Try to parse — needed both to classify DML and to recognise
        # ``EXEC``/``EXECUTE`` (which sqlglot models as a structured node) and
        # ``CALL`` (which falls back to a ``Command`` literal).
        try:
            parsed = parse_statement(stmt_stripped, dialect=dialect)
        except Exception as e:
            logger.debug(f"Skipping statement in {procedure_name}: {type(e).__name__}")
            continue

        # Procedure calls produce datajob-to-datajob lineage. Detect them
        # before the TSQL control-flow filter, which would otherwise swallow
        # ``EXEC``/``EXECUTE``.
        call = _extract_procedure_call(parsed)
        if call is not None:
            procedure_calls.append(call)
            continue

        # Skip TSQL control flow keywords that don't produce lineage.
        if is_dialect_instance(dialect, "tsql") and _is_tsql_control_flow_statement(
            stmt_upper
        ):
            continue

        try:
            query_type, _ = get_query_type_of_sql(parsed, dialect=platform)
        except Exception as e:
            logger.debug(f"Skipping statement in {procedure_name}: {type(e).__name__}")
            continue

        # Skip non-DML statements that don't produce lineage
        # UNKNOWN: RAISERROR, unsupported SQL, etc.
        # CREATE_DDL: table definitions without data operations
        if query_type in (QueryType.UNKNOWN, QueryType.CREATE_DDL):
            continue

        # Skip SELECT without FROM clause (variable assignments)
        if query_type == QueryType.SELECT:
            has_from = any(isinstance(node, sqlglot.exp.From) for node in parsed.walk())
            if not has_from:
                continue

        dml_statements.append(stmt_stripped)

    return dml_statements, procedure_calls


def parse_procedure_code(
    *,
    schema_resolver: SchemaResolver,
    default_db: Optional[str],
    default_schema: Optional[str],
    code: str,
    is_temp_table: Callable[[str], bool],
    raise_: bool = False,
    procedure_name: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Optional[DataJobInputOutputClass]:
    """
    Parse stored procedure code and extract lineage.

    Splits procedure into individual DML statements and adds each to the aggregator
    separately. This ensures each statement is tracked with its own inputs/outputs
    and temp tables are resolved correctly within the procedure's session.

    Args:
        schema_resolver: Schema resolver for table lookups
        default_db: Default database context
        default_schema: Default schema context
        code: Stored procedure SQL code
        is_temp_table: Callback to check if a table is temporary
        raise_: Whether to raise on parse failures
        procedure_name: Name of the procedure for logging
        session_id: Optional session ID for deterministic temp table resolution.
            If not provided, generates a random UUID. Useful for testing.
    """
    # Derive dialect from schema_resolver's platform to support multiple databases
    platform = schema_resolver.platform
    dialect = get_dialect(platform)

    statements = list(split_statements(code, dialect=get_dialect_str(platform)))

    # Classify each statement: DML statements feed the lineage aggregator,
    # procedure calls feed dataJob → dataJob lineage.
    dml_statements, procedure_calls = _classify_statements(
        statements=statements,
        dialect=dialect,
        platform=platform,
        procedure_name=procedure_name,
    )

    # Resolve procedure calls to DataJob URNs. Deduplicate while preserving
    # first-call order so the output is deterministic for golden files.
    input_datajobs: List[str] = []
    seen_input_datajobs: Set[str] = set()
    for call in procedure_calls:
        urn = _build_call_datajob_urn(
            call=call,
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
        )
        if urn is None or urn in seen_input_datajobs:
            continue
        seen_input_datajobs.add(urn)
        input_datajobs.append(urn)

    if not dml_statements and not input_datajobs:
        logger.debug(
            f"No DML statements or procedure calls found in procedure {procedure_name}, "
            "skipping lineage extraction"
        )
        return None

    result: Optional[DataJobInputOutputClass] = None
    if dml_statements:
        # Generate a shared session_id for all statements in this procedure
        # This is critical for temp table resolution across statements
        if session_id is None:
            session_id = str(uuid.uuid4())

        aggregator = SqlParsingAggregator(
            platform=schema_resolver.platform,
            platform_instance=schema_resolver.platform_instance,
            env=schema_resolver.env,
            schema_resolver=schema_resolver,
            generate_lineage=True,
            generate_queries=False,
            generate_usage_statistics=False,
            generate_operations=False,
            generate_query_subject_fields=False,
            generate_query_usage_statistics=False,
            is_temp_table=is_temp_table,
        )

        # Add each DML statement as a separate ObservedQuery
        # All share the same session_id to enable temp table resolution
        for query in dml_statements:
            aggregator.add_observed_query(
                observed=ObservedQuery(
                    default_db=default_db,
                    default_schema=default_schema,
                    query=query,
                    session_id=session_id,
                )
            )

        if aggregator.report.num_observed_queries_failed and raise_:
            logger.info(aggregator.report.as_string())
            raise ValueError(
                f"Failed to parse {aggregator.report.num_observed_queries_failed} queries."
            )

        mcps = list(aggregator.gen_metadata())

        result = to_datajob_input_output(
            mcps=mcps,
            ignore_extra_mcps=True,
        )

    if input_datajobs:
        if result is None:
            result = DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[],
                inputDatajobs=list(input_datajobs),
            )
        elif result.inputDatajobs:
            for urn in input_datajobs:
                if urn not in result.inputDatajobs:
                    result.inputDatajobs.append(urn)
        else:
            result.inputDatajobs = list(input_datajobs)

    return result
