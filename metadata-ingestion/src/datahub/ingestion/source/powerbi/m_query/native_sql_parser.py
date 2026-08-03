import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

import sqlglot
import sqlparse
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError

from datahub.ingestion.api.common import PipelineContext
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_from_sql_statements,
    create_lineage_sql_parsed_result,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect

# It is the PowerBI M-Query way to mentioned \n , \t
SPECIAL_CHARACTERS = {
    "#(lf)": "\n",
    "(lf)": "\n",
    "#(tab)": "\t",
}

ANSI_ESCAPE_CHARACTERS = r"\x1b\[[0-9;]*m"

# BigQuery federation function: EXTERNAL_QUERY(connection_id, external_sql). Its
# arguments are string literals (a connection resource id and a SQL string executed
# on the external engine), not table identifiers, so the generic parser cannot resolve
# an upstream URN from it. It is handled explicitly instead. See extract_external_queries.
EXTERNAL_QUERY_FUNCTION_NAME = "EXTERNAL_QUERY"

# Inert derived table used to replace each EXTERNAL_QUERY source in the outer query so
# the remaining (native) query still parses without the federation yielding a bogus URN.
# It is left unaliased here; the original EXTERNAL_QUERY source's alias is reused (so
# column references like ``a.col`` and join conditions stay valid), falling back to a
# synthesized unique alias when the source had none.
EXTERNAL_QUERY_PLACEHOLDER_SQL = "(SELECT 1 AS pbi_federation_placeholder)"

# Alias prefix for placeholder subqueries whose original EXTERNAL_QUERY source had no
# alias. It deliberately avoids the substring "EXTERNAL_QUERY" so a rewritten query never
# re-triggers federation handling, and is suffixed with an index to stay unique when a
# query contains multiple unaliased federations.
EXTERNAL_QUERY_PLACEHOLDER_ALIAS_PREFIX = "pbi_federation_source"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalQueryReference:
    # connection resource id, i.e. the first EXTERNAL_QUERY argument
    # (``project.region.connection``)
    connection: str
    # SQL executed on the external engine, i.e. the second EXTERNAL_QUERY argument
    inner_sql: str


@dataclass(frozen=True)
class ExternalQueryExtraction:
    # EXTERNAL_QUERY federations found in the outer query.
    references: List[ExternalQueryReference]
    # Outer query with each EXTERNAL_QUERY table source replaced by an inert placeholder.
    rewritten_query: str
    # True when the outer query could not be parsed. Distinguishes "no federation found"
    # (references empty, parse_failed False) from "federation present but parse failed"
    # (references empty, parse_failed True) so the caller can report the dropped lineage.
    parse_failed: bool = False
    # Rendered SQL of EXTERNAL_QUERY calls that were detected but could not be turned into
    # a usable reference (non-string-literal arguments, or placement outside a FROM/JOIN
    # table position). Each represents dropped federated lineage; the caller surfaces them
    # via report.warning so the skip is visible in the run summary rather than debug-only.
    unresolvable: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # A parse failure means the tree could not be walked at all, so no federations can
        # have been extracted. Enforce the invariant instead of relying on the convention
        # documented above, so a contradictory instance can never be constructed.
        if self.parse_failed and (self.references or self.unresolvable):
            raise ValueError(
                "parse_failed extraction cannot carry references or unresolvable federations"
            )


def remove_special_characters(native_query: str) -> str:
    for char in SPECIAL_CHARACTERS:
        native_query = native_query.replace(char, SPECIAL_CHARACTERS[char])

    ansi_escape_regx = re.compile(ANSI_ESCAPE_CHARACTERS)

    native_query = ansi_escape_regx.sub("", native_query)

    # Replace "" quotes by ". Sqlglot is not handling column name alias surrounded with two double quotes

    native_query = native_query.replace('""', '"')

    return native_query


def get_tables(native_query: str) -> List[str]:
    native_query = remove_special_characters(native_query)
    logger.debug(f"Processing native query = {native_query}")
    tables: List[str] = []
    parsed = sqlparse.parse(native_query)[0]
    tokens: List[sqlparse.sql.Token] = list(parsed.tokens)
    length: int = len(tokens)
    from_index: int = -1
    for index, token in enumerate(tokens):
        logger.debug(f"{token.value}={token.ttype}")
        if (
            token.value.lower().strip() == "from"
            and str(token.ttype) == "Token.Keyword"
        ):
            from_index = index + 1
            break

    # Collect all identifier after FROM clause till we reach to the end or WHERE clause encounter
    while (
        from_index < length
        and isinstance(tokens[from_index], sqlparse.sql.Where) is not True
    ):
        logger.debug(f"{tokens[from_index].value}={tokens[from_index].ttype}")
        logger.debug(f"Type={type(tokens[from_index])}")
        if isinstance(tokens[from_index], sqlparse.sql.Identifier):
            # Split on as keyword and collect the table name from 0th position. strip any spaces
            tables.append(tokens[from_index].value.split("as")[0].strip())
        from_index = from_index + 1

    return tables


def remove_tsql_control_statements(query: str) -> str:
    # PowerBI M-Queries embed T-SQL control statements (USE, SET, GO, DROP) that are
    # not valid SQL and break the parser. Each separates statements, so replace it
    # with ';' instead of deleting it — preserving the boundary as a real terminator
    # rather than an ambiguous blank line.

    patterns = [
        # DROP TABLE IF EXISTS #<temp> — temp table cleanup between statements
        r"DROP\s+TABLE\s+IF\s+EXISTS\s+(?:#?\w+(?:,\s*#?\w+)*)[;\n]",
        # USE <database> — T-SQL database context switch; \S+ handles both plain
        # identifiers (USE Reports) and bracketed ones (USE [Reports])
        r"^\s*USE\s+\S+\s*$",
        # SET <option> ON|OFF — T-SQL session-level options (NOCOUNT, QUOTED_IDENTIFIER, etc.)
        r"^\s*SET\s+\w+\s+(?:ON|OFF)\s*;?\s*$",
        # GO — T-SQL batch separator
        r"^\s*GO\s*$",
    ]

    new_query = query

    for pattern in patterns:
        new_query = re.sub(pattern, ";", new_query, flags=re.IGNORECASE | re.MULTILINE)

    # SELECT … INTO #<temp> — strip only the INTO clause so FROM/WHERE lineage remains
    # parseable. Anchored to SELECT so INSERT INTO and MERGE INTO are never matched.
    # [^;\n] stops at semicolons and uses \n(?!\s*\n) to not cross blank lines,
    # preventing a SELECT in one statement from reaching an INSERT INTO in the next.
    new_query = re.sub(
        r"(SELECT\b(?:[^;\n]|\n(?!\s*\n))*?)\s+INTO\s+##?\w+",
        r"\1",
        new_query,
        flags=re.IGNORECASE,
    )

    # Collapse runs of separators introduced above into one ';' and drop leading
    # ones. Only adjacent/leading semicolons match, so a lone ';' inside a string
    # literal is left untouched.
    new_query = re.sub(r";(?:\s*;)+", ";", new_query)
    new_query = re.sub(r"^(?:\s*;)+\s*", "", new_query)

    # Only normalize multiple consecutive spaces (but preserve newlines and tabs)
    # This fixes spacing issues caused by statement removal without
    # collapsing the entire query into a single line
    new_query = re.sub(r"[ \t]+", " ", new_query)
    # Remove spaces at the start of lines
    new_query = re.sub(r"\n[ \t]+", "\n", new_query)
    # Collapse 3+ consecutive blank lines down to one
    new_query = re.sub(r"\n{3,}", "\n\n", new_query)
    # Remove trailing spaces
    new_query = new_query.strip()

    return new_query


def remove_drop_statement(query: str) -> str:
    # Kept for backwards compatibility — delegates to the broader T-SQL cleanup function.
    return remove_tsql_control_statements(query)


def _is_single_statement(query: str, platform: str) -> bool:
    """Return True if the query parses as a single statement in the platform's dialect.

    Single statements are parsed as-is; anything that parses as multiple statements
    or fails to parse is handled by the multi-statement path.
    """
    try:
        dialect = get_dialect(platform)
    except (ValueError, AttributeError):
        # Platform has no sqlglot dialect (e.g. unresolved 'odbc') or is None;
        # fall back to the default dialect, which classifies these cases the same.
        dialect = None
    try:
        statements = [
            stmt for stmt in sqlglot.parse(query, dialect=dialect) if stmt is not None
        ]
    except SqlglotError:
        # Not valid single SQL (e.g. separator-less juxtaposed statements).
        return False
    return len(statements) <= 1


def extract_external_queries(query: str, platform: str) -> ExternalQueryExtraction:
    """Extract BigQuery ``EXTERNAL_QUERY`` federations from a query.

    Returns the list of ``(connection id, inner SQL)`` references found and a rewritten
    copy of the outer query with each ``EXTERNAL_QUERY`` table source replaced by an inert
    placeholder subquery. The rewrite lets the outer query be parsed for any remaining
    native tables without the unresolvable federation resolving to an empty/garbage URN.

    If the query contains no ``EXTERNAL_QUERY``, the original query is returned unchanged
    with an empty reference list. If the query fails to parse, ``parse_failed`` is set so
    the caller can report the federated lineage it could not extract.
    """
    try:
        dialect = get_dialect(platform)
    except (ValueError, AttributeError):
        dialect = None

    try:
        # PowerBI native SQL can contain multiple statements; parse (not parse_one) so a
        # federation in a later statement is not dropped when the query is re-serialized.
        statements = [
            stmt for stmt in sqlglot.parse(query, dialect=dialect) if stmt is not None
        ]
    except SqlglotError:
        logger.debug("Failed to parse query for EXTERNAL_QUERY extraction: %s", query)
        return ExternalQueryExtraction(
            references=[], rewritten_query=query, parse_failed=True
        )

    references: List[ExternalQueryReference] = []
    unresolvable: List[str] = []
    placeholder_index = 0
    mutated = False
    for statement in statements:
        # Materialize before mutating the tree, since replacing nodes during a lazy
        # find_all traversal is unsafe.
        for func in list(statement.find_all(exp.Anonymous)):
            if func.name.upper() != EXTERNAL_QUERY_FUNCTION_NAME:
                continue

            func_sql = func.sql(dialect=dialect)
            # EXTERNAL_QUERY appears as a table-valued function wrapped in a Table node
            # when used in a FROM/JOIN position.
            table_source = func.parent
            in_table_position = isinstance(table_source, exp.Table)

            # EXTERNAL_QUERY is (connection, sql) with an optional third JSON options arg;
            # only the first two are needed. A usable reference requires two string-literal
            # arguments in a table position; anything else can't be turned into an upstream.
            args = func.expressions
            connection_arg = args[0] if len(args) >= 1 else None
            inner_arg = args[1] if len(args) >= 2 else None
            if len(args) < 2:
                reason: Optional[str] = f"unexpected argument count {len(args)}"
            elif not (
                isinstance(connection_arg, exp.Literal)
                and connection_arg.is_string
                and isinstance(inner_arg, exp.Literal)
                and inner_arg.is_string
            ):
                reason = "non-string-literal arguments"
            elif not in_table_position:
                reason = "not in a FROM/JOIN table position"
            else:
                reason = None

            if reason is None:
                assert connection_arg is not None and inner_arg is not None
                references.append(
                    ExternalQueryReference(
                        connection=connection_arg.this,
                        inner_sql=inner_arg.this,
                    )
                )
            else:
                # Record every federation we can't extract so the caller surfaces the
                # dropped lineage via report.warning instead of it being lost at debug
                # level only.
                logger.debug("Skipping EXTERNAL_QUERY (%s): %s", reason, func_sql)
                unresolvable.append(func_sql)

            # Strip the federation from the outer query whenever it sits in a table
            # position - extractable or not - so the generic parser never resolves it to a
            # bogus URN. Preserve the original source's alias (or synthesize a unique one)
            # so column references and joins stay valid and multiple federations don't
            # collide. A federation outside a table position can't be cleanly stripped; it
            # is left in place and surfaced as unresolvable.
            if in_table_position:
                # ``in_table_position`` already guarantees this, but assert so mypy narrows
                # ``table_source`` from Optional[Expression] to a concrete Table node.
                assert isinstance(table_source, exp.Table)
                placeholder = sqlglot.parse_one(
                    EXTERNAL_QUERY_PLACEHOLDER_SQL, dialect=dialect
                )
                original_alias = table_source.args.get("alias")
                if original_alias is not None:
                    placeholder.set("alias", original_alias.copy())
                else:
                    placeholder.set(
                        "alias",
                        exp.TableAlias(
                            this=exp.to_identifier(
                                f"{EXTERNAL_QUERY_PLACEHOLDER_ALIAS_PREFIX}_{placeholder_index}"
                            )
                        ),
                    )
                placeholder_index += 1
                table_source.replace(placeholder)
                mutated = True

    # Re-serialize every statement (not just the mutated ones) so the rewritten query keeps
    # all original statements, joined back into a single multi-statement string.
    rewritten_query = (
        ";\n".join(stmt.sql(dialect=dialect) for stmt in statements)
        if mutated
        else query
    )
    return ExternalQueryExtraction(
        references=references,
        rewritten_query=rewritten_query,
        unresolvable=unresolvable,
    )


def parse_custom_sql(
    ctx: PipelineContext,
    query: str,
    schema: Optional[str],
    database: Optional[str],
    platform: str,
    env: str,
    platform_instance: Optional[str],
) -> Optional["SqlParsingResult"]:
    logger.debug("Using sqlglot_lineage to parse custom sql")
    logger.debug(f"Processing native query using DataHub Sql Parser = {query}")

    if _is_single_statement(query, platform):
        result = create_lineage_sql_parsed_result(
            query=query,
            default_schema=schema,
            default_db=database,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=ctx.graph,
        )
    else:
        result = create_lineage_from_sql_statements(
            queries=query,
            default_schema=schema,
            default_db=database,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=ctx.graph,
        )

    return result
