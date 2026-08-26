import re
from typing import Callable, Dict, Final, List, Tuple

from datahub.utilities.str_enum import StrEnum

# Dataset platform for Kafka topics — matches the platform the Kafka source emits
# topics under, so processing-job input/output URNs line up with the topic entities.
KAFKA_PLATFORM: Final[str] = "kafka"

# All processing DataJobs of a given engine hang off one synthetic DataFlow so they
# group in the UI instead of scattering one flow per query/statement/app.
DATA_JOB_TYPE: Final[str] = "COMMAND"


class StreamProcessingEngine(StrEnum):
    FLINK = "flink"


# engine -> (flow_id, flow_name, flow_description)
ENGINE_FLOW_METADATA: Final[Dict[StreamProcessingEngine, Tuple[str, str, str]]] = {
    StreamProcessingEngine.FLINK: (
        "flink_statements",
        "Flink SQL Statements",
        "Confluent Cloud Flink SQL statements reading from and writing to this Kafka cluster.",
    ),
}

# Custom-property keys on the processing DataJob.
PROP_ENGINE: Final[str] = "engine"
PROP_QUERY: Final[str] = "query"
PROP_STATE: Final[str] = "state"

# Keep a single query from bloating a DataJob's custom properties.
MAX_QUERY_PROPERTY_CHARS: Final[int] = 10_000

# --- Confluent Cloud Flink API ----------------------------------------------
FLINK_HOST_TEMPLATE: Final[str] = "https://flink.{region}.{cloud}.confluent.cloud"
FLINK_STATEMENTS_PATH_TEMPLATE: Final[str] = (
    "/sql/v1/organizations/{organization_id}/environments/{environment_id}/statements"
)
FLINK_KEY_DATA: Final[str] = "data"
FLINK_KEY_METADATA: Final[str] = "metadata"
FLINK_KEY_NEXT: Final[str] = "next"
FLINK_KEY_NAME: Final[str] = "name"
FLINK_KEY_SPEC: Final[str] = "spec"
FLINK_KEY_STATEMENT: Final[str] = "statement"
FLINK_KEY_STATUS: Final[str] = "status"
FLINK_KEY_PHASE: Final[str] = "phase"
FLINK_KEY_COMPUTE_POOL: Final[str] = "compute_pool_id"
FLINK_PAGE_SIZE_PARAM: Final[str] = "page_size"
FLINK_DEFAULT_PAGE_SIZE: Final[int] = 100
# Safety valve if the API keeps returning a next link.
FLINK_MAX_PAGES: Final[int] = 1_000
# Flink SQL is broadly ANSI; use the default dialect for best-effort column parsing.
FLINK_SQL_DIALECT: Final[str] = "postgres"

# --- SQL identifier extraction (table-level, dialect-agnostic) ---------------
# Match the target of INSERT INTO and the sources after FROM / JOIN. Identifiers may
# be backtick- or double-quote-quoted and dotted (catalog.database.table); we take
# the final segment as the topic name. FROM also allows comma-separated tables.
_SQL_IDENT: Final[str] = r"[`\"\w.\-]+"
INSERT_INTO_RE: Final["re.Pattern[str]"] = re.compile(
    rf"insert\s+into\s+({_SQL_IDENT})", re.IGNORECASE
)
FROM_JOIN_RE: Final["re.Pattern[str]"] = re.compile(
    rf"(?:from|join)\s+({_SQL_IDENT}(?:\s*,\s*{_SQL_IDENT})*)", re.IGNORECASE
)
CREATE_STREAM_TABLE_RE: Final["re.Pattern[str]"] = re.compile(
    rf"create\s+(?:or\s+replace\s+)?(?:table|stream)\s+({_SQL_IDENT})",
    re.IGNORECASE,
)
_PLAIN_SQL_IDENT_RE: Final["re.Pattern[str]"] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Comments and single-quoted literals so FROM/JOIN regexes do not match inside them.
_SQL_NOISE_RE: Final["re.Pattern[str]"] = re.compile(
    r"(--[^\n]*|/\*.*?\*/|'(?:''|[^'])*')",
    re.DOTALL,
)
# One quoted token (`...` / "...") or a bare ident; used to keep dots inside quotes.
_QUOTED_OR_BARE_IDENT_RE: Final["re.Pattern[str]"] = re.compile(
    r'`[^`]*`|"[^"]*"|[A-Za-z0-9_\-]+'
)


def strip_sql_noise(sql: str) -> str:
    return _SQL_NOISE_RE.sub(" ", sql)


def last_identifier_segment(identifier: str) -> str:
    # Fully-quoted names like `customer.events` keep their dots; dotted paths
    # (`cat`.`db`.`table` or cat.db.table) reduce to the final segment.
    parts = _QUOTED_OR_BARE_IDENT_RE.findall(identifier.strip())
    if not parts:
        return identifier.strip().strip('`"')
    return parts[-1].strip('`"')


def quote_sql_identifier(name: str) -> str:
    # Topic names with hyphens/dots would otherwise be parsed as arithmetic.
    if _PLAIN_SQL_IDENT_RE.match(name):
        return name
    return '"' + name.replace('"', '""') + '"'


def _idents_in_clause(clause: str) -> List[str]:
    idents: List[str] = []
    for raw in clause.split(","):
        ident = last_identifier_segment(raw.strip())
        if ident:
            idents.append(ident)
    return idents


def insert_into_identifiers(sql: str) -> List[str]:
    return [
        ident
        for match in INSERT_INTO_RE.finditer(strip_sql_noise(sql))
        for ident in _idents_in_clause(match.group(1))
    ]


def from_join_identifiers(sql: str) -> List[str]:
    return [
        ident
        for match in FROM_JOIN_RE.finditer(strip_sql_noise(sql))
        for ident in _idents_in_clause(match.group(1))
    ]


def rewrite_table_identifiers(sql: str, replace_ident: Callable[[str], str]) -> str:
    def replace_single(match: "re.Match[str]") -> str:
        keyword = match.group(0)[: match.start(1) - match.start(0)]
        return keyword + replace_ident(match.group(1))

    def replace_from_join(match: "re.Match[str]") -> str:
        keyword = match.group(0)[: match.start(1) - match.start(0)]
        parts = [part.strip() for part in match.group(1).split(",") if part.strip()]
        return keyword + ", ".join(replace_ident(part) for part in parts)

    rewritten = strip_sql_noise(sql)
    rewritten = CREATE_STREAM_TABLE_RE.sub(replace_single, rewritten)
    rewritten = INSERT_INTO_RE.sub(replace_single, rewritten)
    rewritten = FROM_JOIN_RE.sub(replace_from_join, rewritten)
    return rewritten
