"""SQL-like filter string parser for DataHub search.

Parses filter strings like::

    platform = snowflake
    platform = snowflake AND env = PROD
    entity_type = dataset AND (platform = snowflake OR platform = bigquery)
    platform IN (snowflake, bigquery) AND NOT env = DEV

Grammar::

    expr       := term (OR term)*
    term       := factor (AND factor)*
    factor     := NOT factor | '(' expr ')' | condition
    condition  := field '=' value
                | field '!=' value
                | field ('>' | '>=' | '<' | '<=') value
                | field IN '(' value (',' value)* ')'
                | field IS NULL
                | field IS NOT NULL
    value      := quoted_string | unquoted_token | keyword_as_value

Supported filter fields (case-insensitive, with aliases):
    entity_type (or type), entity_subtype (or subtype), platform, domain,
    container, env (or environment), owner, tag, glossary_term, status

Known limitations:
    - Field names that collide with SQL keywords (``in``, ``not``, ``and``,
      ``or``) cannot be used as field names.  No real DataHub fields have
      these names.
    - ``platform IN ()`` (empty IN list) produces a confusing parse error
      rather than a clear message.
    - ``status`` only accepts a single value (NOT_SOFT_DELETED, ALL, or
      ONLY_SOFT_DELETED) because ``RemovedStatusFilter`` is an enum.
    - LIKE / CONTAINS operators are not supported; fall back to the JSON
      filter format for advanced conditions.
    - The ``container`` filter's ``direct_descendants_only`` flag is not
      expressible in SQL syntax; use JSON format if needed.
    - Custom (unrecognized) field names preserve the user's original casing;
      built-in fields are normalized case-insensitively.
    - Values containing special characters (spaces, ``=``, parentheses,
      commas) must be quoted: ``tag = "urn:li:tag:my tag"``.
    - Comparison operators (``>``, ``>=``, ``<``, ``<=``) bypass the typed
      filter classes and emit raw ``_CustomCondition`` rules.  No validation
      is performed on whether the operator makes sense for the field; using
      them on string fields like ``platform`` will produce lexicographic
      comparisons in Elasticsearch, which is rarely useful.
"""

from enum import Enum
from typing import List, Sequence, Union

from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.sdk.search_filters import Filter, FilterDsl as F, _EnvFilter

FILTER_DOCS = """\
FILTER SYNTAX (SQL-like WHERE clause):
    Uses simple SQL-like syntax with AND, OR, NOT, and parentheses.

    Basic filters:
      platform = snowflake
      entity_type = dataset
      env = PROD

    Multiple values (IN):
      platform IN (snowflake, bigquery, redshift)
      entity_type IN (dataset, dashboard)

    Combining filters:
      platform = snowflake AND env = PROD
      entity_type = dataset AND platform IN (snowflake, bigquery)

    OR and parentheses:
      platform = snowflake OR platform = bigquery
      entity_type = dataset AND (platform = snowflake OR platform = bigquery)

    NOT:
      NOT env = DEV
      entity_type = dataset AND NOT platform = looker

    Comparison operators (>, >=, <, <=):
      columnCount > 10
      columnCount >= 5
      columnCount <= 100

    Existence checks (IS NULL / IS NOT NULL):
      glossary_term IS NOT NULL
      tag IS NOT NULL
      owner IS NULL

    Complex:
      entity_type = dataset AND env = PROD AND (platform = snowflake OR platform = bigquery)

    SUPPORTED FILTER FIELDS:
    - entity_type: dataset, dashboard, chart, corp_user, corp_group, dataProduct, etc.
    - entity_subtype (or subtype): Table, View, Model, etc.
    - platform: snowflake, bigquery, looker, tableau, etc.
    - domain: full URN required, e.g. urn:li:domain:marketing
    - container: full URN required, e.g. urn:li:container:abc123
    - tag: full URN required, e.g. urn:li:tag:PII
    - glossary_term: full URN required, e.g. urn:li:glossaryTerm:uuid
    - owner: full URN required, e.g. urn:li:corpuser:alice or urn:li:corpGroup:team
    - env: PROD, DEV, STAGING (only use if explicitly requested)
    - status: NOT_SOFT_DELETED, ALL, ONLY_SOFT_DELETED
    - deprecated: true or false (whether the entity is marked as deprecated)
    - hasActiveIncidents: true or false (whether the entity has active incidents)
    - hasFailingAssertions: true or false (whether the entity has failing assertions)
    - columnCount: number of columns (from dataset profiling)

    IMPORTANT: Domain, container, tag, glossary_term, and owner filters require
    full URN format (urn:li:...). Search with entity_type = domain first to find
    valid domain URNs, then use the exact URN from the results.

    Values containing special characters (spaces, =, parentheses) must be quoted:
      tag = "urn:li:tag:my tag"
      customProperties = "key=value"\
"""


def parse_filter_string(s: str) -> Filter:
    """Parse a SQL-like filter string into a Filter object.

    Examples::

        parse_filter_string("platform = snowflake")
        parse_filter_string("platform = snowflake AND env = PROD")
        parse_filter_string("platform IN (snowflake, bigquery)")
        parse_filter_string("entity_type = dataset AND (platform = snowflake OR platform = bigquery)")
        parse_filter_string("NOT env = DEV")
        parse_filter_string("tag = urn:li:tag:PII AND owner = urn:li:corpuser:alice")
    """
    if not s or not s.strip():
        raise ValueError("Filter string cannot be empty")

    tokens = _tokenize(s.strip())
    parser = _Parser(tokens)
    return parser.parse()


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------


class _TokenType(Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    IN = "IN"
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    LPAREN = "("
    RPAREN = ")"
    COMMA = ","
    STRING = "STRING"
    EOF = "EOF"


# Token types accepted in value positions (after =, !=, >, etc. and inside IN lists).
# Keywords are included so that unquoted values like "urn:li:tag:NOT" or bare "AND"
# are accepted instead of causing a confusing parse error.
_VALUE_TOKEN_TYPES = {
    _TokenType.STRING,
    _TokenType.AND,
    _TokenType.OR,
    _TokenType.NOT,
    _TokenType.IN,
}


class _Token:
    def __init__(self, type: _TokenType, value: str, pos: int):
        self.type = type
        self.value = value
        self.pos = pos

    def __repr__(self) -> str:
        return f"Token({self.type}, {self.value!r})"


_KEYWORDS = {"AND", "OR", "NOT", "IN"}


def _unescape(s: str) -> str:
    """Process backslash escape sequences in a quoted string value."""
    result: List[str] = []
    i = 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s):
            next_char = s[i + 1]
            if next_char in ("\\", "'", '"'):
                result.append(next_char)
                i += 2
                continue
        result.append(s[i])
        i += 1
    return "".join(result)


def _tokenize(s: str) -> List[_Token]:  # noqa: C901
    """Scan ``s`` left-to-right into a flat list of tokens.

    Recognises operators (``=``, ``!=``, ``>``, ``>=``, ``<``, ``<=``),
    parentheses, commas, single/double-quoted strings (with backslash
    escaping), SQL keywords (AND/OR/NOT/IN, case-insensitive), and
    unquoted identifier/value strings.  Unquoted strings break on any
    character in the stop set (whitespace, operators, quotes, etc.).
    """
    tokens: List[_Token] = []
    i = 0
    while i < len(s):
        prev_i = i

        if s[i].isspace():
            i += 1
            continue

        # Two-char operators first (>=, <=, !=), then single-char
        if s[i] == "!" and i + 1 < len(s) and s[i + 1] == "=":
            tokens.append(_Token(_TokenType.NEQ, "!=", i))
            i += 2
        elif s[i] == ">" and i + 1 < len(s) and s[i + 1] == "=":
            tokens.append(_Token(_TokenType.GTE, ">=", i))
            i += 2
        elif s[i] == "<" and i + 1 < len(s) and s[i + 1] == "=":
            tokens.append(_Token(_TokenType.LTE, "<=", i))
            i += 2
        elif s[i] == ">":
            tokens.append(_Token(_TokenType.GT, ">", i))
            i += 1
        elif s[i] == "<":
            tokens.append(_Token(_TokenType.LT, "<", i))
            i += 1
        elif s[i] == "(":
            tokens.append(_Token(_TokenType.LPAREN, "(", i))
            i += 1
        elif s[i] == ")":
            tokens.append(_Token(_TokenType.RPAREN, ")", i))
            i += 1
        elif s[i] == ",":
            tokens.append(_Token(_TokenType.COMMA, ",", i))
            i += 1
        elif s[i] == "=":
            tokens.append(_Token(_TokenType.EQ, "=", i))
            i += 1
        elif s[i] in ('"', "'"):
            quote = s[i]
            start = i
            i += 1
            while i < len(s) and s[i] != quote:
                if s[i] == "\\" and i + 1 < len(s):
                    i += 2
                else:
                    i += 1
            if i >= len(s):
                raise ValueError(f"Unterminated string starting at position {start}")
            raw_value = s[start + 1 : i]
            tokens.append(_Token(_TokenType.STRING, _unescape(raw_value), start))
            i += 1  # skip closing quote
        else:
            start = i
            while i < len(s) and s[i] not in " \t\n\r()=!<>,'\"":
                i += 1
            if i == start:
                raise ValueError(f"Unexpected character {s[i]!r} at position {i}")
            word = s[start:i]
            upper = word.upper()
            if upper in _KEYWORDS:
                tokens.append(_Token(_TokenType[upper], word, start))
            else:
                tokens.append(_Token(_TokenType.STRING, word, start))

        if i <= prev_i:
            raise ValueError(
                f"Filter parser internal error: stuck at position {prev_i}. "
                f"Please report this bug with your filter string."
            )

    tokens.append(_Token(_TokenType.EOF, "", len(s)))
    return tokens


# ---------------------------------------------------------------------------
# Recursive-descent parser
# ---------------------------------------------------------------------------


class _Parser:
    """Recursive-descent parser that converts a token list into a ``Filter`` tree.

    Operator precedence (lowest to highest): OR, AND, NOT / parentheses.
    See the module docstring for the full grammar.
    """

    def __init__(self, tokens: List[_Token]):
        self.tokens = tokens
        self.pos = 0

    def _peek(self) -> _Token:
        return self.tokens[self.pos]

    def _advance(self) -> _Token:
        token = self.tokens[self.pos]
        self.pos += 1
        return token

    def _expect(self, type: _TokenType) -> _Token:
        token = self._advance()
        if token.type != type:
            raise ValueError(
                f"Expected {type.value} at position {token.pos}, "
                f"got {token.type.value} ({token.value!r})"
            )
        return token

    def _expect_value(self) -> _Token:
        """Expect a value token. Accepts STRING tokens and keyword tokens used as values."""
        token = self._advance()
        if token.type in _VALUE_TOKEN_TYPES:
            return token
        raise ValueError(
            f"Expected value at position {token.pos}, "
            f"got {token.type.value} ({token.value!r})"
        )

    def parse(self) -> Filter:
        result = self._parse_expr()
        if self._peek().type != _TokenType.EOF:
            token = self._peek()
            raise ValueError(
                f"Unexpected token at position {token.pos}: {token.value!r}"
            )
        return result

    def _parse_expr(self) -> Filter:
        """expr := term (OR term)*"""
        terms = [self._parse_term()]
        while self._peek().type == _TokenType.OR:
            self._advance()  # consume OR
            terms.append(self._parse_term())
        if len(terms) == 1:
            return terms[0]
        return F.or_(*terms)

    def _parse_term(self) -> Filter:
        """term := factor (AND factor)*"""
        factors = [self._parse_factor()]
        while self._peek().type == _TokenType.AND:
            self._advance()  # consume AND
            factors.append(self._parse_factor())
        if len(factors) == 1:
            return factors[0]
        return F.and_(*factors)

    def _parse_factor(self) -> Filter:
        """factor := NOT factor | '(' expr ')' | condition"""
        if self._peek().type == _TokenType.NOT:
            self._advance()  # consume NOT
            inner = self._parse_factor()
            return _negate_filter(inner)
        elif self._peek().type == _TokenType.LPAREN:
            self._advance()  # consume (
            result = self._parse_expr()
            self._expect(_TokenType.RPAREN)
            return result
        else:
            return self._parse_condition()

    _COMPARISON_OPS: dict = {
        _TokenType.GT: "GREATER_THAN",
        _TokenType.GTE: "GREATER_THAN_OR_EQUAL_TO",
        _TokenType.LT: "LESS_THAN",
        _TokenType.LTE: "LESS_THAN_OR_EQUAL_TO",
    }

    def _parse_condition(self) -> Filter:
        """condition := field op value | field IN (...) | field IS [NOT] NULL"""
        field_token = self._expect(_TokenType.STRING)
        field = field_token.value

        if self._peek().type == _TokenType.EQ:
            self._advance()  # consume =
            value = self._expect_value().value
            return _make_filter(field, [value])
        elif self._peek().type == _TokenType.NEQ:
            self._advance()  # consume !=
            value = self._expect_value().value
            return _make_negated_filter(field, [value])
        elif self._peek().type in self._COMPARISON_OPS:
            op_token = self._advance()
            value = self._expect_value().value
            return F.custom_filter(field, self._COMPARISON_OPS[op_token.type], [value])
        elif self._peek().type == _TokenType.IN:
            self._advance()  # consume IN
            self._expect(_TokenType.LPAREN)
            values = [self._expect_value().value]
            while self._peek().type == _TokenType.COMMA:
                self._advance()  # consume comma
                values.append(self._expect_value().value)
            self._expect(_TokenType.RPAREN)
            return _make_filter(field, values)
        elif (
            self._peek().type == _TokenType.STRING
            and self._peek().value.upper() == "IS"
        ):
            return self._parse_is_null(field)
        else:
            raise ValueError(
                f"Expected =, !=, >, >=, <, <=, IN, or IS after field name at position {self._peek().pos}"
            )

    def _parse_is_null(self, field: str) -> Filter:
        """Parse IS NULL or IS NOT NULL after the field name."""
        self._advance()  # consume IS
        negated = False
        if self._peek().type == _TokenType.NOT:
            self._advance()  # consume NOT
            negated = True
        null_token = self._expect_value()
        if null_token.value.upper() != "NULL":
            raise ValueError(
                f"Expected NULL at position {null_token.pos}, got {null_token.value!r}"
            )
        return _make_exists_filter(field, negated=negated)


# ---------------------------------------------------------------------------
# Filter construction helpers
# ---------------------------------------------------------------------------

# Map of user-facing field names (case-insensitive) to canonical names.
# Used by _make_filter to dispatch to typed Filter classes (e.g. _PlatformFilter).
# Unrecognized fields fall through to _CustomCondition with the raw field name.
_FIELD_MAP = {
    "entity_type": "entity_type",
    "type": "entity_type",
    "entity_subtype": "entity_subtype",
    "subtype": "entity_subtype",
    "platform": "platform",
    "domain": "domain",
    "container": "container",
    "env": "env",
    "environment": "env",
    "owner": "owner",
    "tag": "tag",
    "glossary_term": "glossary_term",
    "status": "status",
}


# Maps canonical field names to their Elasticsearch index field names.
# Needed only for EXISTS conditions (IS NULL / IS NOT NULL) where we bypass
# the typed Filter classes and emit raw SearchFilterRule directly.
# For = / != / IN, the typed classes handle the mapping internally
# (e.g. _PlatformFilter adds ".keyword" to the platform field).
_BACKEND_FIELD_MAP = {
    "entity_type": "_entityType",
    "entity_subtype": "typeNames",
    "platform": "platform.keyword",
    "domain": "domains",
    "container": "browsePathV2",
    "owner": "owners",
    "tag": "tags",
    "glossary_term": "glossaryTerms",
}


def _make_exists_filter(field: str, negated: bool) -> Filter:
    """Create an EXISTS or NOT EXISTS filter for IS NOT NULL / IS NULL.

    Args:
        field: User-facing field name (e.g. "glossary_term")
        negated: True for IS NOT NULL (field EXISTS), False for IS NULL (field NOT EXISTS)
    """
    normalized = _FIELD_MAP.get(field.lower())

    if normalized == "env":
        # env maps to two backend fields (origin, env); EXISTS on either means "has env"
        origin_exists = F.custom_filter("origin", "EXISTS", [])
        env_exists = F.custom_filter("env", "EXISTS", [])
        if negated:
            # IS NOT NULL: origin EXISTS OR env EXISTS
            return F.or_(origin_exists, env_exists)
        else:
            # IS NULL: NOT origin EXISTS AND NOT env EXISTS
            return F.and_(F.not_(origin_exists), F.not_(env_exists))

    if normalized == "status":
        raise ValueError("IS NULL / IS NOT NULL is not supported for the status field")

    backend_field = _BACKEND_FIELD_MAP.get(normalized or "", field)
    exists_filter = F.custom_filter(backend_field, "EXISTS", [])
    if negated:
        # IS NOT NULL → field EXISTS
        return exists_filter
    else:
        # IS NULL → NOT field EXISTS
        return F.not_(exists_filter)


def _make_filter(field: str, values: Union[List[str], Sequence[str]]) -> Filter:
    """Create a typed Filter from a field name and values."""
    normalized = _FIELD_MAP.get(field.lower())

    if normalized == "entity_type":
        # values contains runtime strings from user input, but EntityTypeName is a
        # Literal[...] union for static analysis only. The actual normalization and
        # validation happens inside flexible_entity_type_to_graphql at filter compile time.
        return F.entity_type(values)  # type: ignore[arg-type]
    elif normalized == "entity_subtype":
        return F.entity_subtype(values)
    elif normalized == "platform":
        return F.platform(values)
    elif normalized == "domain":
        return F.domain(values)
    elif normalized == "container":
        return F.container(values)
    elif normalized == "env":
        return F.env(values)
    elif normalized == "owner":
        return F.owner(values)
    elif normalized == "tag":
        return F.tag(values)
    elif normalized == "glossary_term":
        return F.glossary_term(values)
    elif normalized == "status":
        if len(values) != 1:
            raise ValueError("Status filter must have exactly one value")
        return F.soft_deleted(RemovedStatusFilter(values[0]))
    else:
        return F.custom_filter(field, "EQUAL", list(values))


def _negate_filter(inner: Filter) -> Filter:
    """Negate a filter, handling special cases that _Not cannot handle.

    _Not.validate_not() rejects filters that compile to multiple OR clauses.
    _EnvFilter produces 2 OR clauses (origin and env fields), so we handle
    its negation with De Morgan's law: NOT(A OR B) = NOT A AND NOT B.
    """
    if isinstance(inner, _EnvFilter):
        return _negate_env_filter(inner.env)
    return F.not_(inner)


def _make_negated_filter(field: str, values: Union[List[str], Sequence[str]]) -> Filter:
    """Create a negated filter (for != operator)."""
    normalized = _FIELD_MAP.get(field.lower())
    if normalized == "env":
        return _negate_env_filter(list(values))
    return F.not_(_make_filter(field, values))


def _negate_env_filter(values: List[str]) -> Filter:
    """Negate an env filter using De Morgan's law.

    NOT (origin = X OR env = X) becomes (origin != X AND env != X).
    Each F.not_ wraps a single-OR-clause _CustomCondition, which _Not accepts.
    F.and_ then combines them via Cartesian product into one AND clause.
    """
    return F.and_(
        F.not_(F.custom_filter("origin", "EQUAL", list(values))),
        F.not_(F.custom_filter("env", "EQUAL", list(values))),
    )
