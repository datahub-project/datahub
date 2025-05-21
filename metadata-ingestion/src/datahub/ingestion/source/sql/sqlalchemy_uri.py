from typing import Any, Dict, Optional

from sqlalchemy.engine import URL


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
        try:
            host, port_str = at.rsplit(":", 1)
            port = int(port_str)
        except ValueError:
            host = at
            port = None
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
