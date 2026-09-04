import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

import sqlglot
import sqlparse
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError
from sqlglot.tokens import TokenType

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
    rf"\b{EXTERNAL_QUERY_FUNCTION_NAME}\s*\(", re.IGNORECASE
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
    statements = sqlparse.parse(native_query)
    if not statements:
        # sqlparse yields nothing for blank input, which a native query can be
        # once the M-Query escape sequences are stripped.
        return tables
    parsed = statements[0]
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


def _resolve_dialect(platform: str) -> Optional[sqlglot.Dialect]:
    """Resolve the sqlglot dialect for a platform, falling back to the default.

    Platforms with no sqlglot dialect (e.g. an unresolved 'odbc') or a None platform
    fall back to the default dialect rather than raising.
    """
    try:
        return get_dialect(platform)
    except (ValueError, AttributeError):
        return None


def contains_external_query_call(query: str, platform: str) -> bool:
    """Return True if the SQL has a real ``EXTERNAL_QUERY(...)`` function call.

    The detection runs on the sqlglot token stream (which discards comments and collapses
    string literals into single tokens) rather than a raw regex over the source. A raw
    regex matches ``EXTERNAL_QUERY(`` even when it only appears inside a ``--``/``/* */``
    comment or a string constant; that false positive routes an unrelated query into
    EXTERNAL_QUERY handling, and if the query also fails to parse it is discarded whole
    (native lineage included) with a misleading federation warning.

    The tokenizer is lenient and still succeeds on SQL the parser rejects, so a genuine
    federation call is detected even inside an otherwise-unparseable batch. If tokenizing
    itself fails (pathological input), fall back to the raw regex so detection is never
    weaker than before.
    """
    dialect = _resolve_dialect(platform)
    try:
        tokens = (dialect or sqlglot.Dialect()).tokenize(query)
    except Exception:
        return bool(EXTERNAL_QUERY_PATTERN.search(query))

    for index, token in enumerate(tokens):
        if (
            (token.text or "").upper() == EXTERNAL_QUERY_FUNCTION_NAME
            and index + 1 < len(tokens)
            and tokens[index + 1].token_type == TokenType.L_PAREN
        ):
            return True
    return False


def _parse_statements(
    query: str, dialect: Optional[sqlglot.Dialect]
) -> List[exp.Expression]:
    """Parse a query into its non-empty statements in the given dialect.

    Uses ``sqlglot.parse`` (not ``parse_one``) so multi-statement PowerBI SQL is kept.
    Raises ``SqlglotError`` if the query is not valid SQL; callers decide how to react.
    """
    return [
        stmt
        for stmt in sqlglot.parse(query, dialect=dialect)
        if isinstance(stmt, exp.Expression)
    ]


def _is_single_statement(query: str, platform: str) -> bool:
    """Return True if the query parses as a single statement in the platform's dialect.

    Single statements are parsed as-is; anything that parses as multiple statements
    or fails to parse is handled by the multi-statement path.
    """
    try:
        statements = _parse_statements(query, _resolve_dialect(platform))
    except SqlglotError:
        # Not valid single SQL (e.g. separator-less juxtaposed statements).
        return False
    return len(statements) <= 1


def _is_string_literal(node: Optional[exp.Expression]) -> bool:
    # BigQuery raw strings (r'...') parse as exp.RawString, which is not an exp.Literal
    # subclass, so a plain isinstance(node, exp.Literal) check drops EXTERNAL_QUERY
    # federations written with raw strings. Both carry the text in `.this`.
    return isinstance(node, exp.RawString) or (
        isinstance(node, exp.Literal) and node.is_string
    )


def extract_external_queries(query: str, platform: str) -> ExternalQueryExtraction:
    """Extract EXTERNAL_QUERY federations and rewrite them to inert placeholders."""
    dialect = _resolve_dialect(platform)

    try:
        statements = _parse_statements(query, dialect)
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
                _is_string_literal(connection_arg) and _is_string_literal(inner_arg)
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
