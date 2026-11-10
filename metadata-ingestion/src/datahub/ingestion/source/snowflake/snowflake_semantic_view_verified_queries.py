import hashlib
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
import sqlglot.expressions
from sqlglot.tokens import Token, TokenType

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeSemanticView
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    AiContextClass,
    AuditStampClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    QuerySubjectClass,
    QuerySubjectsClass,
)
from datahub.metadata.urns import QueryUrn

logger = logging.getLogger(__name__)

_SNOWFLAKE_DIALECT = sqlglot.Dialect.get_or_raise("snowflake")

# GET_DDL renders a semantic view's verified queries (Cortex Analyst's
# AI_VERIFIED_QUERIES / "verified query repository") as
#   ai_verified_queries ( NAME AS ( QUESTION '...' VERIFIED_BY '...'
#     VERIFIED_AT 1714497970 ONBOARDING_QUESTION true SQL '...' ), ... )
_AI_VERIFIED_QUERIES_KEYWORD = "AI_VERIFIED_QUERIES"
_KEY_QUESTION = "QUESTION"
_KEY_SQL = "SQL"
_KEY_VERIFIED_BY = "VERIFIED_BY"
_KEY_VERIFIED_AT = "VERIFIED_AT"
_KEY_ONBOARDING_QUESTION = "ONBOARDING_QUESTION"
_VALUE_TOKEN_TYPES = frozenset(
    {TokenType.STRING, TokenType.NUMBER, TokenType.TRUE, TokenType.FALSE}
)

# Cortex Analyst verified SQL addresses the semantic view's logical tables with
# this prefix (`FROM __orders`), not the physical base tables.
_LOGICAL_TABLE_PREFIX = "__"

_QUERY_ID_PREFIX = "snowflake-semantic-view-verified-query"
_UNKNOWN_ACTOR_URN = "urn:li:corpuser:unknown"

PROP_VERIFIED_QUERY_NAME = "verified_query_name"
PROP_VERIFIED_BY = "verified_by"
PROP_VERIFIED_AT = "verified_at"
PROP_ONBOARDING_QUESTION = "use_as_onboarding_question"
PROP_SEMANTIC_VIEW = "semantic_view"


@dataclass(frozen=True)
class SnowflakeVerifiedQuery:
    """One entry of a semantic view's AI_VERIFIED_QUERIES."""

    name: str
    sql: str
    question: Optional[str] = None
    verified_by: Optional[str] = None
    # Epoch seconds, as Snowflake stores it.
    verified_at: Optional[int] = None
    onboarding_question: Optional[bool] = None


@dataclass
class VerifiedQueriesParseResult:
    queries: List[SnowflakeVerifiedQuery] = field(default_factory=list)
    # Entries that could not be turned into a verified query, with the reason.
    skipped: List[str] = field(default_factory=list)


def parse_verified_queries_from_ddl(ddl: str) -> VerifiedQueriesParseResult:
    """Extract the ``ai_verified_queries ( ... )`` block from a semantic view's
    GET_DDL output.

    sqlglot has no grammar for CREATE SEMANTIC VIEW, so the DDL is tokenized and
    the block is walked on the token stream: string literals come back unescaped
    (``''`` -> ``'``), and a keyword inside a comment or string can't be
    mistaken for the block. A DDL without the block yields no queries.

    Raises ``sqlglot.errors.TokenError`` when the DDL cannot be tokenized.
    """
    result = VerifiedQueriesParseResult()
    tokens = _SNOWFLAKE_DIALECT.tokenize(ddl)

    block_start: Optional[int] = None
    for idx in range(len(tokens) - 1):
        if (
            tokens[idx].token_type == TokenType.VAR
            and tokens[idx].text.upper() == _AI_VERIFIED_QUERIES_KEYWORD
            and tokens[idx + 1].token_type == TokenType.L_PAREN
        ):
            block_start = idx + 2
            break
    if block_start is None:
        return result

    for entry in _split_block_entries(tokens[block_start:]):
        query, reason = _parse_entry(entry)
        if query is not None:
            result.queries.append(query)
        elif reason is not None:
            result.skipped.append(reason)
    return result


def _split_block_entries(tokens: List[Token]) -> List[List[Token]]:
    # Split on top-level commas, stopping at the paren that closes the block.
    entries: List[List[Token]] = []
    current: List[Token] = []
    depth = 1
    for token in tokens:
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                break
        if depth == 1 and token.token_type == TokenType.COMMA:
            entries.append(current)
            current = []
        else:
            current.append(token)
    if current:
        entries.append(current)
    return entries


def _parse_entry(
    entry: List[Token],
) -> Tuple[Optional[SnowflakeVerifiedQuery], Optional[str]]:
    if not entry:
        return None, None
    # `NAME AS ( key value ... )`
    if (
        len(entry) < 3
        or entry[0].token_type not in (TokenType.VAR, TokenType.IDENTIFIER)
        or entry[1].token_type != TokenType.ALIAS
        or entry[2].token_type != TokenType.L_PAREN
    ):
        return None, f"unrecognized entry starting with {entry[0].text!r}"
    name = entry[0].text

    values: Dict[str, Token] = {}
    body = entry[3:]
    idx = 0
    while idx < len(body):
        token = body[idx]
        if token.token_type == TokenType.R_PAREN:
            break
        if idx + 1 < len(body) and body[idx + 1].token_type in _VALUE_TOKEN_TYPES:
            values[token.text.upper()] = body[idx + 1]
            idx += 2
        else:
            # An unknown keyword without a literal value; skip it rather than
            # misread the next key as its value.
            idx += 1

    sql_token = values.get(_KEY_SQL)
    if sql_token is None or not sql_token.text.strip():
        return None, f"{name}: no SQL"

    question_token = values.get(_KEY_QUESTION)
    verified_by_token = values.get(_KEY_VERIFIED_BY)
    return (
        SnowflakeVerifiedQuery(
            name=name,
            sql=sql_token.text,
            question=(question_token.text.strip() or None) if question_token else None,
            verified_by=verified_by_token.text if verified_by_token else None,
            verified_at=_int_value(values.get(_KEY_VERIFIED_AT)),
            onboarding_question=_bool_value(values.get(_KEY_ONBOARDING_QUESTION)),
        ),
        None,
    )


def _int_value(token: Optional[Token]) -> Optional[int]:
    if token is None:
        return None
    try:
        return int(token.text)
    except ValueError:
        return None


def _bool_value(token: Optional[Token]) -> Optional[bool]:
    if token is None:
        return None
    if token.token_type == TokenType.TRUE:
        return True
    if token.token_type == TokenType.FALSE:
        return False
    text = token.text.strip().lower()
    if text in ("true", "false"):
        return text == "true"
    return None


class SnowflakeVerifiedQueriesExtractor:
    """Emits a semantic view's verified queries as curated Query entities.

    Each verified query becomes a ``query`` entity with ``source=MANUAL`` (shown
    as a Highlighted Query), whose subjects are the semantic view (semantic
    model or legacy dataset) and the physical tables the SQL reads. In
    semantic-model mode the questions are also added to the model's
    ``aiContext.examples``.
    """

    def __init__(
        self,
        report: SnowflakeV2Report,
        identifiers: SnowflakeIdentifierBuilder,
        graph: Optional[DataHubGraph] = None,
    ) -> None:
        self.report = report
        self.identifiers = identifiers
        self.graph = graph
        self._ai_context_unmerged_reported = False

    def gen_workunits(
        self,
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
        view_urn: str,
        semantic_model_urn: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        """``view_urn`` is the semantic view's own entity (semanticModel or
        legacy dataset); ``semantic_model_urn`` is set only in semantic-model
        mode, where the questions also go to the model's aiContext."""
        view_identifier = self.identifiers.get_dataset_identifier(
            semantic_view.name, schema_name, db_name
        )
        queries = self._parse(semantic_view, view_identifier)
        if not queries:
            return

        for query in queries:
            yield from self._gen_query_workunits(
                query=query,
                semantic_view=semantic_view,
                db_name=db_name,
                schema_name=schema_name,
                view_identifier=view_identifier,
                view_urn=view_urn,
            )
            self.report.num_semantic_view_verified_queries_emitted += 1

        if semantic_model_urn is not None:
            questions = [q.question for q in queries if q.question]
            ai_context = self._merged_ai_context(semantic_model_urn, questions)
            if ai_context is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=semantic_model_urn, aspect=ai_context
                ).as_workunit()

    def _parse(
        self, semantic_view: SnowflakeSemanticView, view_identifier: str
    ) -> List[SnowflakeVerifiedQuery]:
        ddl = semantic_view.view_definition
        if not ddl:
            self.report.num_semantic_view_verified_queries_skipped += 1
            self.report.warning(
                title="Semantic view verified queries not read",
                message="The semantic view's definition (GET_DDL) is not "
                "available, so its verified queries could not be read. Check that "
                "the role can run GET_DDL on the semantic view.",
                context=view_identifier,
            )
            return []
        try:
            parsed = parse_verified_queries_from_ddl(ddl)
        except sqlglot.errors.SqlglotError as e:
            self.report.num_semantic_view_verified_queries_skipped += 1
            self.report.warning(
                title="Semantic view verified queries not read",
                message="The semantic view's definition could not be tokenized, "
                "so its verified queries were skipped.",
                context=view_identifier,
                exc=e,
            )
            return []
        if parsed.skipped:
            self.report.num_semantic_view_verified_queries_skipped += len(
                parsed.skipped
            )
            self.report.warning(
                title="Semantic view verified queries skipped",
                message="Some verified queries in the semantic view's definition "
                "could not be read and were skipped.",
                context=f"{view_identifier}: {parsed.skipped}",
            )
        # A name is unique within a view; keep the first if the DDL repeats one,
        # since both would land on the same query URN.
        unique: Dict[str, SnowflakeVerifiedQuery] = {}
        for query in parsed.queries:
            unique.setdefault(query.name, query)
        return list(unique.values())

    def query_urn(self, view_identifier: str, query_name: str) -> str:
        # Keyed on the view's identifier (not the semanticModel/dataset URN) so a
        # switch between semantic-model and legacy mode updates the same query
        # entity instead of leaving a duplicate. Hashed because a verified query
        # name may be a quoted identifier with characters a URN can't carry.
        key = "|".join(
            [
                self.identifiers.identifier_config.platform_instance or "",
                view_identifier,
                query_name,
            ]
        )
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return QueryUrn(f"{_QUERY_ID_PREFIX}-{digest}").urn()

    def _gen_query_workunits(
        self,
        query: SnowflakeVerifiedQuery,
        semantic_view: SnowflakeSemanticView,
        db_name: str,
        schema_name: str,
        view_identifier: str,
        view_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        query_urn = self.query_urn(view_identifier, query.name)

        custom_properties: Dict[str, str] = {
            PROP_SEMANTIC_VIEW: view_identifier,
            PROP_VERIFIED_QUERY_NAME: query.name,
        }
        if query.verified_by:
            custom_properties[PROP_VERIFIED_BY] = query.verified_by
        if query.verified_at is not None:
            custom_properties[PROP_VERIFIED_AT] = str(query.verified_at)
        if query.onboarding_question is not None:
            custom_properties[PROP_ONBOARDING_QUESTION] = str(
                query.onboarding_question
            ).lower()

        timestamp_millis = self._timestamp_millis(query, semantic_view)
        stamp = AuditStampClass(time=timestamp_millis, actor=_UNKNOWN_ACTOR_URN)

        yield MetadataChangeProposalWrapper(
            entityUrn=query_urn,
            aspect=QueryPropertiesClass(
                statement=QueryStatementClass(
                    value=query.sql, language=QueryLanguageClass.SQL
                ),
                # Curated by a person in Snowflake; DataHub shows MANUAL queries
                # as Highlighted Queries.
                source=QuerySourceClass.MANUAL,
                name=query.question or query.name,
                created=stamp,
                lastModified=stamp,
                customProperties=custom_properties,
            ),
        ).as_workunit()

        subjects = [view_urn] + [
            urn
            for urn in self._referenced_table_urns(
                query, semantic_view, db_name, schema_name
            )
            if urn != view_urn
        ]
        yield MetadataChangeProposalWrapper(
            entityUrn=query_urn,
            aspect=QuerySubjectsClass(
                subjects=[QuerySubjectClass(entity=urn) for urn in subjects]
            ),
        ).as_workunit()

    @staticmethod
    def _timestamp_millis(
        query: SnowflakeVerifiedQuery, semantic_view: SnowflakeSemanticView
    ) -> int:
        if query.verified_at is not None:
            return query.verified_at * 1000
        fallback = semantic_view.last_altered or semantic_view.created
        return int(fallback.timestamp() * 1000) if fallback else 0

    def _referenced_table_urns(
        self,
        query: SnowflakeVerifiedQuery,
        semantic_view: SnowflakeSemanticView,
        db_name: str,
        schema_name: str,
    ) -> List[str]:
        """Physical tables the verified SQL reads, in first-seen order.

        A reference resolves to a physical table when it is fully qualified, or
        names one of the view's logical tables (with or without the ``__``
        prefix Cortex Analyst uses); a 1-2 part name is otherwise resolved
        against the view's database and schema the way Snowflake would. If the
        SQL does not parse, every base table of the view is used instead, so the
        query is still reachable from the tables it was written for.
        """
        base_table_urns = [
            self._dataset_urn(table.table, table.schema, table.database)
            for table in semantic_view.base_tables
        ]
        try:
            statements = sqlglot.parse(query.sql, dialect=_SNOWFLAKE_DIALECT)
        except sqlglot.errors.SqlglotError as e:
            logger.debug(
                f"Could not parse verified query {query.name} of "
                f"{semantic_view.name}; linking all base tables: {e}"
            )
            self.report.num_semantic_view_verified_query_parse_failures += 1
            return _dedupe(base_table_urns)

        logical_tables = {
            name.upper(): physical
            for name, physical in semantic_view.logical_to_physical_table.items()
        }
        urns: List[str] = []
        for statement in statements:
            if statement is None:
                continue
            cte_names: Set[str] = {
                cte.alias_or_name.upper()
                for cte in statement.find_all(sqlglot.expressions.CTE)
            }
            for table in statement.find_all(sqlglot.expressions.Table):
                urn = self._resolve_table(
                    table, logical_tables, cte_names, db_name, schema_name
                )
                if urn is not None:
                    urns.append(urn)
        return _dedupe(urns)

    def _resolve_table(
        self,
        table: sqlglot.expressions.Table,
        logical_tables: Dict[str, Tuple[str, str, str]],
        cte_names: Set[str],
        db_name: str,
        schema_name: str,
    ) -> Optional[str]:
        name = _folded(table.args.get("this"))
        if not name:
            return None
        catalog = _folded(table.args.get("catalog"))
        db = _folded(table.args.get("db"))
        if not catalog and not db:
            if name.upper() in cte_names:
                return None
            logical_name = name.upper()
            if logical_name.startswith(_LOGICAL_TABLE_PREFIX):
                logical_name = logical_name[len(_LOGICAL_TABLE_PREFIX) :]
            physical = logical_tables.get(logical_name)
            if physical is not None:
                physical_db, physical_schema, physical_table = physical
                return self._dataset_urn(physical_table, physical_schema, physical_db)
        return self._dataset_urn(name, db or schema_name, catalog or db_name)

    def _dataset_urn(self, table: str, schema: str, db: str) -> str:
        return self.identifiers.gen_dataset_urn(
            self.identifiers.get_dataset_identifier(table, schema, db)
        )

    def _merged_ai_context(
        self, semantic_model_urn: str, questions: List[str]
    ) -> Optional[AiContextClass]:
        """The model's aiContext with the verified questions added to
        ``examples``.

        aiContext is written as a whole aspect, so with a DataHub connection the
        existing aspect is read and only missing questions are appended; its
        synonyms, instructions and other examples are kept. Without a
        connection the aspect is written with just the questions. When the
        existing aspect cannot be read nothing is written, rather than risk
        erasing context entered in DataHub.
        """
        if not questions:
            return None
        if self.graph is None:
            if not self._ai_context_unmerged_reported:
                self._ai_context_unmerged_reported = True
                self.report.info(
                    title="Verified questions written as whole AI context aspects",
                    message="No DataHub connection, so a semantic model's existing "
                    "AI context cannot be read; it is written with only the "
                    "verified questions as examples.",
                    context=semantic_model_urn,
                )
            return AiContextClass(examples=_dedupe(questions))
        try:
            existing = self.graph.get_aspect(semantic_model_urn, AiContextClass)
        except Exception as e:
            self.report.warning(
                title="Could not read existing AI context; verified questions not added",
                message="Skipped adding this semantic model's verified questions "
                "to its AI context examples to avoid overwriting context entered "
                "in DataHub. The verified queries themselves were emitted.",
                context=semantic_model_urn,
                exc=e,
            )
            return None
        if existing is None:
            return AiContextClass(examples=_dedupe(questions))
        current = list(existing.examples or [])
        missing = [q for q in _dedupe(questions) if q not in current]
        if not missing:
            return None
        existing.examples = current + missing
        return existing


def _folded(identifier: Optional[sqlglot.expressions.Expr]) -> Optional[str]:
    # Snowflake folds an unquoted identifier to uppercase and keeps a quoted one
    # as written.
    if not isinstance(identifier, sqlglot.expressions.Identifier):
        return None
    return identifier.name if identifier.quoted else identifier.name.upper()


def _dedupe(values: List[str]) -> List[str]:
    return list(dict.fromkeys(values))
