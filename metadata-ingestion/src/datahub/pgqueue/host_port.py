"""Host/port parsing for PostgreSQL connectivity (stdlib-only; avoids ingestion SQL helpers)."""

from __future__ import annotations

from typing import Optional, Tuple


def parse_host_port(
    host_port: str, default_port: Optional[int] = None
) -> Tuple[str, Optional[int]]:
    """
    Parse a host:port string into separate host and port components.

    Same behavior as ``datahub.ingestion.source.sql.sqlalchemy_uri.parse_host_port``,
    kept local so pgQueue's psycopg2 stack does not depend on ingestion sources.
    """
    try:
        host, port_str = host_port.rsplit(":", 1)
        port: Optional[int]
        try:
            port = int(port_str)
        except ValueError:
            port = default_port
        return host, port
    except ValueError:
        return host_port, default_port
