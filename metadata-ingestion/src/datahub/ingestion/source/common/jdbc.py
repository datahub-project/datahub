import re
from typing import Dict, Final, FrozenSet, Optional
from urllib.parse import SplitResult, parse_qsl, urlsplit

from pydantic import BaseModel

JDBC_PREFIX: Final[str] = "jdbc:"

# Regex patterns for non-standard JDBC URL formats (see normalize_jdbc_url)
_ORACLE_THIN_RE: Final = re.compile(
    r"^(?:jdbc:)?oracle:(?:thin|oci):@/?"
    r"/?(?P<host>[^:/@\s]+)"
    r":(?P<port>\d+)"
    r"(?:[:/](?P<db>[^@?;\s]+))?"
    r"(?:[;?].*)?$"
)
_SQLSERVER_RE: Final = re.compile(
    r"^(?:jdbc:)?sqlserver://(?P<host>[^:;/\s]+)"
    r"(?::(?P<port>\d+))?"
    r"(?:;(?P<params>.*))?$"
)
_JTDS_RE: Final = re.compile(
    r"^(?:jdbc:)?jtds:sqlserver://(?P<host>[^:/@\s]+)"
    r"(?::(?P<port>\d+))?"
    r"(?:/(?P<db>[^;?\s]+))?"
    r"(?:[;?].*)?$"
)

# `jdbc:` plus the subprotocol tokens before `//` or `@`, matched without
# regard to case so `JDBC:MySQL://` parses like `jdbc:mysql://`.
_JDBC_SCHEME_PREFIX_RE: Final = re.compile(
    r"^(?:jdbc:)?[a-z][a-z0-9+.-]*(?::[a-z][a-z0-9+.-]*)*(?=:(?://|@))",
    re.IGNORECASE,
)
# `scheme:qualifier://`, e.g. `mysql:loadbalance://` or `redshift:iam://`.
_JDBC_QUALIFIER_RE: Final = re.compile(
    r"^(?P<scheme>[^:]+):(?P<qualifier>[^:/]+):(?=//)"
)

SQLSERVER_DEFAULT_PORT: Final[str] = "1433"
SQLSERVER_DATABASE_NAME_KEY: Final[str] = "databasename"
SQLSERVER_DATABASE_KEYS: Final[FrozenSet[str]] = frozenset(
    {SQLSERVER_DATABASE_NAME_KEY, "database"}
)
# normalize_jdbc_url predates parse_jdbc_url and kafka_connect relies on its
# output, so it keeps reading only `databaseName`.
_SQLSERVER_NORMALIZE_DATABASE_KEYS: Final[FrozenSet[str]] = frozenset(
    {SQLSERVER_DATABASE_NAME_KEY}
)

ORACLE_PLATFORM: Final[str] = "oracle"
MSSQL_PLATFORM: Final[str] = "mssql"
SNOWFLAKE_PLATFORM: Final[str] = "snowflake"
HANA_PLATFORM: Final[str] = "hana"
TERADATA_PLATFORM: Final[str] = "teradata"
POSTGRES_PLATFORM: Final[str] = "postgres"
REDSHIFT_PLATFORM: Final[str] = "redshift"
# The Redshift JDBC driver also accepts `jdbc:postgresql://` URLs.
REDSHIFT_HOST_MARKER: Final[str] = "redshift.amazonaws"

# JDBC subprotocols the SQLAlchemy URI mapper does not recognize but that are
# real DataHub platforms.
JDBC_PROTOCOL_PLATFORM_FALLBACK: Final[FrozenSet[str]] = frozenset(
    {"db2", TERADATA_PLATFORM, "cassandra"}
)
# Exact subprotocol matches only, so a mistyped or unrelated scheme such as
# `mysqlx` cannot invent a lineage platform.
JDBC_SCHEME_PLATFORMS: Final[Dict[str, str]] = {
    "awsathena": "athena",
    "bigquery": "bigquery",
    "cassandra": "cassandra",
    "clickhouse": "clickhouse",
    "db2": "db2",
    "hive": "hive",
    "hive2": "hive",
    "jtds": MSSQL_PLATFORM,
    "mariadb": "mariadb",
    "mongodb": "mongodb",
    "mysql": "mysql",
    "oracle": ORACLE_PLATFORM,
    "pinot": "pinot",
    "postgresql": POSTGRES_PLATFORM,
    "presto": "presto",
    "redshift": REDSHIFT_PLATFORM,
    "sap": HANA_PLATFORM,
    "snowflake": SNOWFLAKE_PLATFORM,
    "sqlserver": MSSQL_PLATFORM,
    "teradata": TERADATA_PLATFORM,
    "trino": "trino",
    "vertica": "vertica",
}
# Driver-mode tokens between the subprotocol and `//`. They pick a connection
# strategy, not a host or database.
JDBC_SCHEME_QUALIFIERS: Final[Dict[str, FrozenSet[str]]] = {
    "mysql": frozenset({"loadbalance", "replication"}),
    "mariadb": frozenset(
        {"loadbalance", "replication", "sequential", "failover", "aurora"}
    ),
    "redshift": frozenset({"iam"}),
    "clickhouse": frozenset({"http", "https", "grpc", "tcp"}),
}

# Platforms whose JDBC URL carries the database as the first path segment.
JDBC_PATH_DATABASE_PLATFORMS: Final[FrozenSet[str]] = frozenset(
    {"postgres", "redshift", "mysql", "mariadb", "db2", "clickhouse", "vertica", "hive"}
)
# Platforms whose JDBC URL path is `/catalog/schema`.
JDBC_PATH_CATALOG_SCHEMA_PLATFORMS: Final[FrozenSet[str]] = frozenset(
    {"trino", "presto"}
)

SNOWFLAKE_DATABASE_KEY: Final[str] = "db"
SNOWFLAKE_SCHEMA_KEY: Final[str] = "schema"
HANA_DATABASE_KEY: Final[str] = "databasename"
TERADATA_DATABASE_KEY: Final[str] = "database"

# DB2 appends `:prop=value;` to the database; Hive and others use `;` or `?`.
_PATH_SEGMENT_TERMINATORS_RE: Final = re.compile(r"[;:]")


class JdbcUrlInfo(BaseModel):
    platform: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None


class _FixedFormatJdbcUrl(BaseModel):
    """Oracle thin/OCI, SQL Server or jTDS URL fields, before normalization."""

    platform: str
    host: str
    port: Optional[str] = None
    database: Optional[str] = None


def normalize_jdbc_url(url: str) -> str:
    """Convert a JDBC URL to a form SQLAlchemy can parse.

    Non-standard formats handled:
    - Oracle thin/OCI (oracle:thin:@[//]host:port/service) → oracle://host:port/service
    - SQL Server JDBC (sqlserver://host[:port][;databaseName=db;...]) → mssql://host:port/db
    - jTDS SQL Server (jtds:sqlserver://host[:port][/db]) → mssql://host:port/db

    All other URLs are returned unchanged after stripping a leading jdbc:.
    """
    fixed = _match_fixed_format_url(
        url, _SQLSERVER_NORMALIZE_DATABASE_KEYS, skip_blank_database=False
    )
    if fixed is not None:
        # Oracle URLs always carry a port; SQL Server and jTDS default it.
        port = fixed.port or SQLSERVER_DEFAULT_PORT
        return f"{fixed.platform}://{fixed.host}:{port}/{fixed.database or ''}"

    return url[len(JDBC_PREFIX) :] if url.startswith(JDBC_PREFIX) else url


def _match_fixed_format_url(
    url: str, sqlserver_database_keys: FrozenSet[str], skip_blank_database: bool
) -> Optional[_FixedFormatJdbcUrl]:
    m = _ORACLE_THIN_RE.match(url)
    if m:
        return _FixedFormatJdbcUrl(
            platform=ORACLE_PLATFORM,
            host=m.group("host"),
            port=m.group("port"),
            database=m.group("db"),
        )

    m = _SQLSERVER_RE.match(url)
    if m:
        return _FixedFormatJdbcUrl(
            platform=MSSQL_PLATFORM,
            host=m.group("host"),
            port=m.group("port"),
            database=_sqlserver_database(
                m.group("params") or "", sqlserver_database_keys, skip_blank_database
            ),
        )

    m = _JTDS_RE.match(url)
    if m:
        return _FixedFormatJdbcUrl(
            platform=MSSQL_PLATFORM,
            host=m.group("host"),
            port=m.group("port"),
            database=m.group("db"),
        )
    return None


def validate_jdbc_url(url: str) -> bool:
    """Return whether url is a JDBC URL format we can parse."""
    if not url or not isinstance(url, str):
        return False
    if _ORACLE_THIN_RE.match(url) or _SQLSERVER_RE.match(url) or _JTDS_RE.match(url):
        return True
    if not url.startswith(JDBC_PREFIX):
        return False
    parts = url.split(":", 3)
    return len(parts) >= 3  # jdbc:protocol:connection_details


def parse_jdbc_url(url: str) -> JdbcUrlInfo:
    """Read the platform, host, port, database and schema from a JDBC URL.

    Returns an empty JdbcUrlInfo for anything it cannot parse, and one with no
    platform for a subprotocol it does not recognize. For Oracle, `database`
    holds the SID or service name, which DataHub does not use as a URN segment
    by default, so callers decide whether to keep it. The `jdbc:` prefix and
    subprotocol are matched case-insensitively.
    """
    if not url or not isinstance(url, str):
        return JdbcUrlInfo()
    url = _lowercase_scheme_prefix(url)
    if not validate_jdbc_url(url):
        return JdbcUrlInfo()
    try:
        return _parse_valid_jdbc_url(url)
    except ValueError:
        return JdbcUrlInfo()


def _lowercase_scheme_prefix(url: str) -> str:
    m = _JDBC_SCHEME_PREFIX_RE.match(url)
    if not m:
        return url
    return m.group(0).lower() + url[m.end() :]


def _parse_valid_jdbc_url(url: str) -> JdbcUrlInfo:
    fixed = _match_fixed_format_url(
        url, SQLSERVER_DATABASE_KEYS, skip_blank_database=True
    )
    if fixed is not None:
        return JdbcUrlInfo(
            platform=fixed.platform,
            host=fixed.host,
            port=_to_port(fixed.port),
            database=fixed.database or None,
        )

    body = _strip_scheme_qualifier(url[len(JDBC_PREFIX) :])
    parts = urlsplit(body)
    if not parts.hostname:
        return JdbcUrlInfo()
    platform = _platform_for_scheme(parts.scheme, parts.hostname)
    info = JdbcUrlInfo(platform=platform, host=parts.hostname, port=_split_port(parts))
    if platform is None:
        return info

    segments = [segment for segment in parts.path.split("/") if segment]
    if platform in JDBC_PATH_DATABASE_PLATFORMS:
        if segments:
            info.database = _clean_segment(segments[0])
    elif platform in JDBC_PATH_CATALOG_SCHEMA_PLATFORMS:
        if segments:
            info.database = _clean_segment(segments[0])
        if len(segments) > 1:
            info.schema_name = _clean_segment(segments[1])
    elif platform == SNOWFLAKE_PLATFORM:
        params = _query_params(parts.query)
        info.database = params.get(SNOWFLAKE_DATABASE_KEY) or None
        info.schema_name = params.get(SNOWFLAKE_SCHEMA_KEY) or None
    elif platform == HANA_PLATFORM:
        info.database = _query_params(parts.query).get(HANA_DATABASE_KEY) or None
    elif platform == TERADATA_PLATFORM:
        info.database = _teradata_database(parts.path)
    return info


def _strip_scheme_qualifier(body: str) -> str:
    m = _JDBC_QUALIFIER_RE.match(body)
    if m and m.group("qualifier") in JDBC_SCHEME_QUALIFIERS.get(
        m.group("scheme"), frozenset()
    ):
        return f"{m.group('scheme')}:{body[m.end() :]}"
    return body


def _platform_for_scheme(scheme: str, host: str) -> Optional[str]:
    platform = JDBC_SCHEME_PLATFORMS.get(scheme)
    if platform == POSTGRES_PLATFORM and REDSHIFT_HOST_MARKER in host:
        return REDSHIFT_PLATFORM
    return platform


def _sqlserver_database(
    params: str, keys: FrozenSet[str], skip_blank: bool
) -> Optional[str]:
    # normalize_jdbc_url stops at the first `databaseName` even when blank, and
    # kafka_connect depends on that; parse_jdbc_url keeps looking so
    # `databaseName=;database=db` still yields `db`.
    for param in params.split(";"):
        key, sep, value = param.partition("=")
        if not sep or key.strip().lower() not in keys:
            continue
        if value.strip() or not skip_blank:
            return value.strip()
    return None


def _teradata_database(path: str) -> Optional[str]:
    # jdbc:teradata://host/DATABASE=db,CHARSET=UTF8
    for param in path.strip("/").split(","):
        key, _, value = param.partition("=")
        if key.strip().lower() == TERADATA_DATABASE_KEY and value.strip():
            return value.strip()
    return None


def _query_params(query: str) -> Dict[str, str]:
    return {key.lower(): value for key, value in parse_qsl(query)}


def _clean_segment(segment: str) -> Optional[str]:
    return _PATH_SEGMENT_TERMINATORS_RE.split(segment, maxsplit=1)[0].strip() or None


def _to_port(port: Optional[str]) -> Optional[int]:
    return int(port) if port else None


def _split_port(parts: SplitResult) -> Optional[int]:
    # A malformed or multi-host netloc (`h1:3306,h2:3306`) makes `.port` raise;
    # the database in the path is still worth returning.
    try:
        return parts.port
    except ValueError:
        return None
