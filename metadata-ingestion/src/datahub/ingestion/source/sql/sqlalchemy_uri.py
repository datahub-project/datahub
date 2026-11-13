import logging
import re
import urllib.parse
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

logger = logging.getLogger(__name__)

# Query parameters that carry a password in a connection URL (libpq and most
# DBAPI drivers accept these as an alternative to the userinfo password).
_PASSWORD_QUERY_PARAMS = frozenset({"password", "passwd", "pwd", "sslpassword"})
# An ODBC connection string passed whole as a query parameter (mssql+pyodbc).
_ODBC_CONNECT_PARAM = "odbc_connect"
# ODBC lets a value hold ";" when it is wrapped in braces: PWD={pa;ss}.
_ODBC_PASSWORD = re.compile(
    r"(?:^|;)\s*(?:PWD|PASSWORD)\s*=\s*(?:\{(?P<braced>[^}]*)\}|(?P<plain>[^;]*))",
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
    registry: the userinfo password both exactly as written and decoded,
    password-like query parameters, and the PWD of an ODBC connect string.

    The as-written form matters because sources log the URI verbatim, in
    whatever encoding the user chose; the decoded form covers the URL that
    SQLAlchemy renders from it. Neither depends on the URI being valid."""
    secrets: Dict[str, str] = {}
    verbatim = _VERBATIM_USERINFO_PASSWORD.match(url)
    if verbatim and verbatim.group("password"):
        secrets["sqlalchemy_uri.userinfo_password.verbatim"] = verbatim.group(
            "password"
        )
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
        values = (value,) if isinstance(value, str) else tuple(value)
        if key.lower() in _PASSWORD_QUERY_PARAMS:
            passwords = list(values)
        elif key.lower() == _ODBC_CONNECT_PARAM:
            passwords = [
                match.group("braced") or match.group("plain")
                for item in values
                for match in _ODBC_PASSWORD.finditer(urllib.parse.unquote_plus(item))
            ]
        else:
            continue
        for index, password in enumerate(passwords):
            secrets[f"sqlalchemy_uri.query.{key}.{index}"] = password
    return secrets


def url_dialect_and_driver(url: str) -> Tuple[str, str]:
    """The `dialect+driver` a SQLAlchemy URL names, lowercased; driver is ""
    when the URL names none. Read from the string, so it never raises and
    needs no dialect installed."""
    head = url.split("://", 1)[0].lower()
    dialect, _, driver = head.partition("+")
    return dialect, driver
