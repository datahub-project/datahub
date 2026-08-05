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

# EXTERNAL_QUERY args are string literals, not table ids — handled in extract_external_queries.
EXTERNAL_QUERY_FUNCTION_NAME = "EXTERNAL_QUERY"

EXTERNAL_QUERY_PATTERN = re.compile(
    rf"\b{EXTERNAL_QUERY_FUNCTION_NAME}\b", re.IGNORECASE
)

# Inert FROM/JOIN stand-in; original source alias is reused when present.
EXTERNAL_QUERY_PLACEHOLDER_SQL = "(SELECT 1 AS pbi_federation_placeholder)"

# Must not contain "EXTERNAL_QUERY" or the rewritten query re-triggers federation handling.
EXTERNAL_QUERY_PLACEHOLDER_ALIAS_PREFIX = "pbi_federation_source"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalQueryReference:
    connection: str  # first EXTERNAL_QUERY arg (project.region.connection)
    inner_sql: str  # second EXTERNAL_QUERY arg (SQL on the external engine)


@dataclass
class ExternalQueryExtraction:
    references: List[ExternalQueryReference]
    rewritten_query: str
    # Distinguishes "no federation" from "federation present but outer parse failed".
    parse_failed: bool = False
    # Detected EXTERNAL_QUERY calls that could not be extracted (non-literal args, etc.).
    unresolvable: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
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
    """Extract EXTERNAL_QUERY federations and rewrite them to inert placeholders."""
    try:
        dialect = get_dialect(platform)
    except (ValueError, AttributeError):
        dialect = None

    try:
        # parse (not parse_one) so a federation in a later statement is not dropped.
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
        # Materialize before mutating — replacing nodes during lazy find_all is unsafe.
        for func in list(statement.find_all(exp.Anonymous)):
            if func.name.upper() != EXTERNAL_QUERY_FUNCTION_NAME:
                continue

            func_sql = func.sql(dialect=dialect)
            table_source = func.parent
            in_table_position = isinstance(table_source, exp.Table)

            # (connection, sql[, options]); only literal args in a table position are usable.
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
                logger.debug("Skipping EXTERNAL_QUERY (%s): %s", reason, func_sql)
                unresolvable.append(func_sql)

            # Strip table-position federations so the generic parser never emits a bogus URN.
            if in_table_position:
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
