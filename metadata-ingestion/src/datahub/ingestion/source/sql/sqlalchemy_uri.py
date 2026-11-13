import logging
import re
import urllib.parse
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

logger = logging.getLogger(__name__)

# Query parameters that carry a credential in a connection URL: the password
# aliases libpq and most DBAPI drivers accept instead of the userinfo password,
# plus scalar credentials drivers document as URI parameters (snowflake-sqlalchemy
# `token` / `private_key`, sqlalchemy-bigquery `credentials_base64`).
_SECRET_QUERY_PARAMS = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "sslpassword",
        "token",
        "private_key",
        "credentials_base64",
    }
)
# An ODBC connection string passed whole as a query parameter (mssql+pyodbc).
_ODBC_CONNECT_PARAM = "odbc_connect"
# ODBC lets a value hold ";" when it is wrapped in braces: PWD={pa;ss}.
_ODBC_PASSWORD = re.compile(
    r"(?:^|;)\s*(?:PWD|PASSWORD)\s*=\s*(?:\{(?P<braced>[^}]*)\}|(?P<plain>[^;]*))",
    re.IGNORECASE,
)
# The same, read from the connect string as written in the URI, where its
# separators may be percent-encoded (";" as %3B, "=" as %3D, braces as %7B/%7D)
# and spaces form-encoded. Both patterns fall back to the plain form for an
# unterminated brace, keeping the opening brace in the value: that is exactly
# what appears in the log, and masking a superset only over-masks. Both may
# match an empty value; _query_passwords drops it.
_ODBC_SPACE = r"(?:\s|\+|%20)*"
_ODBC_PASSWORD_AS_WRITTEN = re.compile(
    rf"(?:^|;|%3B){_ODBC_SPACE}(?:PWD|PASSWORD){_ODBC_SPACE}(?:=|%3D){_ODBC_SPACE}"
    r"(?:(?:\{|%7B)(?P<braced>.*?)(?:\}|%7D)|(?P<plain>.*?)(?=;|%3B|$))",
    re.IGNORECASE,
)
# The userinfo password exactly as written in the URI. Bounded by the last "@"
# before the path or query, so an unencoded "@" inside the password -- which
# SQLAlchemy splits into the host instead -- stays part of it.
_VERBATIM_USERINFO_PASSWORD = re.compile(r"^[\w+.-]+://[^:/?#@]*:(?P<password>[^/?]*)@")


def parse_host_port(
    host_port: str, default_port: Optional[int] = None
) -> Tuple[str, Optional[int]]:
    """
    Parse a host:port string into separate host and port components.

    Args:
        host_port: String in format "host:port" or just "host"
        default_port: Optional default port to use if not specified in host_port

    Returns:
        Tuple of (hostname, port) where port may be None if not specified

    Examples:
        >>> parse_host_port("localhost:3306")
        ('localhost', 3306)
        >>> parse_host_port("localhost")
        ('localhost', None)
        >>> parse_host_port("localhost", 5432)
        ('localhost', 5432)
        >>> parse_host_port("db.example.com:invalid", 3306)
        ('db.example.com', 3306)
    """
    try:
        host, port_str = host_port.rsplit(":", 1)
        port: Optional[int]
        try:
            port = int(port_str)
        except ValueError:
            # Port is not a valid integer
            port = default_port
        return host, port
    except ValueError:
        # No colon found, entire string is the hostname
        return host_port, default_port


def make_sqlalchemy_uri(
    scheme: str,
    username: Optional[str],
    password: Optional[str],
    at: Optional[str],
    db: Optional[str],
    uri_opts: Optional[Dict[str, Any]] = None,
) -> str:
    host: Optional[str] = None
    port: Optional[int] = None
    if at:
        host, port = parse_host_port(at)
    if uri_opts:
        uri_opts = {k: v for k, v in uri_opts.items() if v is not None}

    # str(URL) masks the password as "***" on SQLAlchemy 2.0 (its __str__ defaults
    # to hide_password=True). Since this string is fed straight to create_engine(),
    # we must render the real password — otherwise the engine connects with "***".
    return URL.create(
        drivername=scheme,
        username=username,
        password=password,
        host=host,
        port=port,
        database=db,
        query=uri_opts or {},
    ).render_as_string(hide_password=False)


def sqlalchemy_uri_secrets(url: str) -> Dict[str, str]:
    """Passwords embedded in a SQLAlchemy URL, keyed by a name for the secret
    registry: the userinfo password, credential query parameters and the PWD
    of an ODBC connect string, each both exactly as written and decoded.

    The as-written form matters because sources log the URI verbatim, in
    whatever encoding the user chose; the decoded form covers the URL that
    SQLAlchemy renders from it. The as-written forms are read from the raw
    string, so they don't depend on the URI being valid."""
    secrets: Dict[str, str] = {}
    verbatim = _VERBATIM_USERINFO_PASSWORD.match(url)
    if verbatim and verbatim.group("password"):
        secrets["sqlalchemy_uri.userinfo_password.verbatim"] = verbatim.group(
            "password"
        )
    raw_query = _raw_query_passwords(url, start=verbatim.end() if verbatim else 0)
    for key, passwords in raw_query.items():
        for index, password in enumerate(passwords):
            secrets[f"sqlalchemy_uri.query.{key}.{index}.verbatim"] = password
    try:
        parsed = make_url(url)
    except (ArgumentError, ValueError):
        if not secrets:
            logger.warning(
                "sqlalchemy_uri could not be parsed; a password it contains "
                "may not be masked in logs"
            )
        return secrets
    # When the password holds an unencoded "@", SQLAlchemy splits it into the
    # host and returns only a prefix; registering that prefix would mask an
    # unrelated short string everywhere, and the verbatim form already covers it.
    if parsed.password and (
        verbatim is None
        or urllib.parse.unquote(verbatim.group("password")) == parsed.password
    ):
        secrets["sqlalchemy_uri.userinfo_password"] = str(parsed.password)
    for key, value in parsed.query.items():
        # make_url has already decoded the values; don't decode them again.
        values = (value,) if isinstance(value, str) else tuple(value)
        for index, password in enumerate(_query_passwords(key, values, _ODBC_PASSWORD)):
            secrets[f"sqlalchemy_uri.query.{key}.{index}"] = password
    return secrets


def _raw_query_passwords(url: str, start: int) -> Dict[str, List[str]]:
    """Password-like query values exactly as written, by key."""
    query_start = url.find("?", start)
    if query_start < 0:
        return {}
    values_by_key: Dict[str, List[str]] = {}
    for pair in url[query_start + 1 :].split("&"):
        raw_key, separator, raw_value = pair.partition("=")
        if separator:
            key = urllib.parse.unquote_plus(raw_key)
            values_by_key.setdefault(key, []).append(raw_value)
    passwords_by_key: Dict[str, List[str]] = {}
    for key, values in values_by_key.items():
        passwords = _query_passwords(key, values, _ODBC_PASSWORD_AS_WRITTEN)
        if passwords:
            passwords_by_key[key] = passwords
    return passwords_by_key


def _query_passwords(
    key: str, values: Sequence[str], odbc_password: "re.Pattern[str]"
) -> List[str]:
    """The passwords one query parameter carries: its values when the key names
    a password, the PWD of each value when it is an ODBC connect string."""
    if key.lower() in _SECRET_QUERY_PARAMS:
        return [value for value in values if value]
    if key.lower() != _ODBC_CONNECT_PARAM:
        return []
    passwords: List[str] = []
    for value in values:
        for match in odbc_password.finditer(value):
            password = match.group("braced") or match.group("plain")
            if password:
                passwords.append(password)
    return passwords


def url_dialect_and_driver(url: str) -> Tuple[str, str]:
    """The `dialect+driver` a SQLAlchemy URL names, lowercased; driver is ""
    when the URL names none. Read from the string, so it never raises and
    needs no dialect installed."""
    head = url.split("://", 1)[0].lower()
    dialect, _, driver = head.partition("+")
    return dialect, driver
