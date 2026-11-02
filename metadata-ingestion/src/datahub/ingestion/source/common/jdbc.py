import re
from typing import Dict, Final, FrozenSet, Optional
from urllib.parse import SplitResult, parse_qsl, urlsplit

from pydantic import BaseModel

from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)

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

SQLSERVER_DEFAULT_PORT: Final[str] = "1433"
SQLSERVER_DATABASE_NAME_KEY: Final[str] = "databasename"
SQLSERVER_DATABASE_KEYS: Final[FrozenSet[str]] = frozenset(
    {SQLSERVER_DATABASE_NAME_KEY, "database"}
)

ORACLE_PLATFORM: Final[str] = "oracle"
MSSQL_PLATFORM: Final[str] = "mssql"
SNOWFLAKE_PLATFORM: Final[str] = "snowflake"
HANA_PLATFORM: Final[str] = "hana"
TERADATA_PLATFORM: Final[str] = "teradata"
_SQLALCHEMY_UNKNOWN_PLATFORM: Final[str] = "external"

# JDBC subprotocols the SQLAlchemy mapper does not recognize but that are real
# DataHub platforms. Anything else stays unknown so a mistyped URL cannot invent
# a lineage platform.
JDBC_PROTOCOL_PLATFORM_FALLBACK: Final[FrozenSet[str]] = frozenset(
    {"db2", TERADATA_PLATFORM, "cassandra"}
)
# Subprotocols whose name differs from the DataHub platform, or that the
# SQLAlchemy mapper would otherwise miss.
JDBC_SCHEME_PLATFORM_OVERRIDES: Final[Dict[str, str]] = {
    "sap": HANA_PLATFORM,
    "mariadb": "mariadb",
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


def normalize_jdbc_url(url: str) -> str:
    """Convert a JDBC URL to a form SQLAlchemy can parse.

    Non-standard formats handled:
    - Oracle thin/OCI (oracle:thin:@[//]host:port/service) → oracle://host:port/service
    - SQL Server JDBC (sqlserver://host[:port][;databaseName=db;...]) → mssql://host:port/db
    - jTDS SQL Server (jtds:sqlserver://host[:port][/db]) → mssql://host:port/db

    All other URLs are returned unchanged after stripping a leading jdbc:.
    """
    m = _ORACLE_THIN_RE.match(url)
    if m:
        host = m.group("host")
        port = m.group("port")
        db = m.group("db") or ""
        return f"oracle://{host}:{port}/{db}"

    m = _SQLSERVER_RE.match(url)
    if m:
        host = m.group("host")
        port = m.group("port") or SQLSERVER_DEFAULT_PORT
        db = ""
        params_str = m.group("params") or ""
        for param in params_str.split(";"):
            if "=" in param:
                k, _, v = param.partition("=")
                if k.strip().lower() == SQLSERVER_DATABASE_NAME_KEY:
                    db = v.strip()
                    break
        return f"mssql://{host}:{port}/{db}"

    m = _JTDS_RE.match(url)
    if m:
        host = m.group("host")
        port = m.group("port") or SQLSERVER_DEFAULT_PORT
        db = m.group("db") or ""
        return f"mssql://{host}:{port}/{db}"

    return url[len(JDBC_PREFIX) :] if url.startswith(JDBC_PREFIX) else url


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
    by default, so callers decide whether to keep it.
    """
    if not validate_jdbc_url(url):
        return JdbcUrlInfo()
    try:
        return _parse_valid_jdbc_url(url)
    except ValueError:
        return JdbcUrlInfo()


def _parse_valid_jdbc_url(url: str) -> JdbcUrlInfo:
    m = _ORACLE_THIN_RE.match(url)
    if m:
        return JdbcUrlInfo(
            platform=ORACLE_PLATFORM,
            host=m.group("host"),
            port=int(m.group("port")),
            database=m.group("db"),
        )

    m = _SQLSERVER_RE.match(url)
    if m:
        return JdbcUrlInfo(
            platform=MSSQL_PLATFORM,
            host=m.group("host"),
            port=_to_port(m.group("port")),
            database=_sqlserver_database(m.group("params") or ""),
        )

    m = _JTDS_RE.match(url)
    if m:
        return JdbcUrlInfo(
            platform=MSSQL_PLATFORM,
            host=m.group("host"),
            port=_to_port(m.group("port")),
            database=m.group("db"),
        )

    body = url[len(JDBC_PREFIX) :]
    scheme = body.partition(":")[0].lower()
    platform = _platform_for_scheme(scheme, body)
    parts = urlsplit(body)
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


def _platform_for_scheme(scheme: str, body: str) -> Optional[str]:
    override = JDBC_SCHEME_PLATFORM_OVERRIDES.get(scheme)
    if override is not None:
        return override
    platform = get_platform_from_sqlalchemy_uri(body)
    if platform != _SQLALCHEMY_UNKNOWN_PLATFORM:
        return platform
    if scheme in JDBC_PROTOCOL_PLATFORM_FALLBACK:
        return scheme
    return None


def _sqlserver_database(params: str) -> Optional[str]:
    for param in params.split(";"):
        key, _, value = param.partition("=")
        if key.strip().lower() in SQLSERVER_DATABASE_KEYS and value.strip():
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
