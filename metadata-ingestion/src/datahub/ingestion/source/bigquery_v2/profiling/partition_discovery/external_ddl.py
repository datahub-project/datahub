import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from sqlglot.dialects.bigquery import BigQuery
from sqlglot.errors import SqlglotError
from sqlglot.tokens import Token, TokenType

logger = logging.getLogger(__name__)

# OPTIONS keys that only appear on hive-partitioned external tables. The first two are the
# API/reconstructed-DDL spellings, the last two the documented GoogleSQL DDL spellings.
HIVE_PARTITIONING_OPTION_NAMES = frozenset(
    {
        "hive_partitioning_mode",
        "hive_partitioning_options",
        "hive_partition_uri_prefix",
        "require_hive_partition_filter",
    }
)

# File formats whose partitioning, if any, is always declared in the DDL as hive
# partitioning. DELTA_LAKE and ICEBERG keep their partition spec in table metadata outside
# the DDL, GOOGLE_SHEETS needs a Drive OAuth scope, and CLOUD_BIGTABLE is not a file scan,
# so none of those can be confirmed safe to profile unfiltered from the DDL alone.
UNPARTITIONED_SCANNABLE_FORMATS = frozenset(
    {"AVRO", "CSV", "JSON", "NEWLINE_DELIMITED_JSON", "ORC", "PARQUET"}
)

_STRING_TOKEN_TYPES = frozenset(
    {
        TokenType.STRING,
        TokenType.RAW_STRING,
        TokenType.BYTE_STRING,
        TokenType.HEREDOC_STRING,
    }
)


@dataclass
class ExternalTableDdl:
    is_external_create: bool
    declares_hive_partitioning: bool
    declares_partition_by: bool
    # Ordered as declared: a composite hive key is positional.
    hive_partition_columns: Dict[str, str] = field(default_factory=dict)
    format: Optional[str] = None

    def confirms_unpartitioned(self, source_format: Optional[str]) -> bool:
        # The table metadata's source format wins; the DDL `format` option is the
        # fallback when it is missing.
        resolved_format = (source_format or self.format or "").upper()
        return (
            self.is_external_create
            and not self.declares_hive_partitioning
            and not self.declares_partition_by
            and resolved_format in UNPARTITIONED_SCANNABLE_FORMATS
        )


def parse_external_table_ddl(ddl: str) -> Optional[ExternalTableDdl]:
    # sqlglot parses `WITH PARTITION COLUMNS` / `WITH CONNECTION` DDL as an opaque
    # exp.Command, so the DDL is classified from tokens instead. The tokenizer handles
    # GoogleSQL quoting (backslash escapes, raw, byte and triple-quoted strings) and drops
    # comments, so markers inside descriptions, URIs or comments are never matched.
    try:
        tokens = BigQuery().tokenize(ddl)
    except SqlglotError as e:
        logger.debug(f"Could not tokenize external table DDL: {e}")
        return None

    texts = [token.text.upper() for token in tokens]
    result = ExternalTableDdl(
        is_external_create=_is_external_create(tokens, texts),
        declares_hive_partitioning=False,
        declares_partition_by=TokenType.PARTITION_BY
        in {token.token_type for token in tokens},
    )

    for i, token in enumerate(tokens):
        if token.token_type in _STRING_TOKEN_TYPES:
            continue
        followed_by_eq = (
            i + 1 < len(tokens) and tokens[i + 1].token_type == TokenType.EQ
        )
        lowered = token.text.lower()
        if followed_by_eq and lowered in HIVE_PARTITIONING_OPTION_NAMES:
            result.declares_hive_partitioning = True
        elif (
            followed_by_eq
            and lowered == "format"
            and i + 2 < len(tokens)
            and tokens[i + 2].token_type in _STRING_TOKEN_TYPES
        ):
            result.format = tokens[i + 2].text.upper()
        elif texts[i : i + 3] == ["WITH", "PARTITION", "COLUMNS"]:
            result.declares_hive_partitioning = True
            result.hive_partition_columns = _parse_partition_column_list(tokens, i + 3)

    return result


def _is_external_create(tokens: List[Token], texts: List[str]) -> bool:
    if not tokens or tokens[0].token_type != TokenType.CREATE:
        return False
    return any(
        texts[i] == "EXTERNAL" and texts[i + 1] == "TABLE"
        for i in range(len(texts) - 1)
    )


def _parse_partition_column_list(tokens: List[Token], start: int) -> Dict[str, str]:
    # `WITH PARTITION COLUMNS (dt DATE, region STRING)`. The bare form (no list) asks
    # BigQuery to infer the keys, so there is nothing to read here.
    if start >= len(tokens) or tokens[start].token_type != TokenType.L_PAREN:
        return {}

    columns: Dict[str, str] = {}
    current: List[Token] = []
    depth = 0
    for token in tokens[start + 1 :]:
        if token.token_type in (TokenType.L_PAREN, TokenType.LT):
            depth += 1
        elif token.token_type in (TokenType.R_PAREN, TokenType.GT):
            if depth == 0:
                _add_column(columns, current)
                return columns
            depth -= 1
        elif token.token_type == TokenType.COMMA and depth == 0:
            _add_column(columns, current)
            current = []
            continue
        current.append(token)

    # Unterminated list: the DDL is malformed, so report no usable columns.
    return {}


def _add_column(columns: Dict[str, str], tokens: List[Token]) -> None:
    if len(tokens) < 2:
        return
    columns[tokens[0].text] = " ".join(token.text for token in tokens[1:]).upper()
