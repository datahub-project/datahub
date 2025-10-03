from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import URL


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

    return str(
        URL.create(
            drivername=scheme,
            username=username,
            password=password,
            host=host,
            port=port,
            database=db,
            query=uri_opts or {},
        )
    )
