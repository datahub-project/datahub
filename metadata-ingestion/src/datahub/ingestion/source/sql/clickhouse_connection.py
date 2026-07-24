from typing import Final

from sqlalchemy.engine.url import URL

# Stable product token (same value as DATABRICKS_USER_AGENT_ENTRY in unity/connection.py).
CLICKHOUSE_CLIENT_NAME: Final = "datahub"

_HTTP_UA_PARAM: Final = "header__User-Agent"
_NATIVE_PARAM: Final = "client_name"

_IDENTITY_PARAM_BY_DRIVER: Final = {
    "clickhouse": _HTTP_UA_PARAM,
    "clickhouse+http": _HTTP_UA_PARAM,
    "clickhouse+native": _NATIVE_PARAM,
    "clickhouse+asynch": _NATIVE_PARAM,
}


def with_client_identity(url: URL) -> URL:
    param = _IDENTITY_PARAM_BY_DRIVER.get(url.drivername)
    if param is None or param in url.query:
        return url
    return url.set(query={**url.query, param: CLICKHOUSE_CLIENT_NAME})
