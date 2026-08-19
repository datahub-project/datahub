from typing import Final, Mapping

from sqlalchemy.engine.url import URL

# Stable client-identity token so DataHub's own reads are attributable in
# system.query_log, mirroring Databricks partner tagging. Kept bare (no version
# suffix) so attribution queries can match it exactly.
CLICKHOUSE_CLIENT_NAME: Final = "datahub"

_HTTP_UA_PARAM: Final = "header__User-Agent"
_NATIVE_PARAM: Final = "client_name"

_IDENTITY_PARAM_BY_DRIVER: Final[Mapping[str, str]] = {
    "clickhouse": _HTTP_UA_PARAM,
    "clickhouse+http": _HTTP_UA_PARAM,
    "clickhouse+native": _NATIVE_PARAM,
    "clickhouse+asynch": _NATIVE_PARAM,
}


def with_client_identity(url: URL) -> URL:
    """Tag the connection URL with a ``datahub`` client identity.

    HTTP drivers get a ``User-Agent`` request header; native/asynch drivers get
    ``client_name``. A user-supplied value is left untouched.

    Note the native drivers prefix their own product name, so the value that
    lands in ``system.query_log`` is ``ClickHouse datahub`` on native versus
    exactly ``datahub`` in ``http_user_agent`` on HTTP.
    """
    param = _IDENTITY_PARAM_BY_DRIVER.get(url.drivername)
    if param is None:
        return url
    # HTTP header names are case-insensitive; requests would silently collapse a
    # user's header__user-agent and our header__User-Agent into one key, dropping
    # the override. Compare case-insensitively so an existing value always wins.
    existing = {key.casefold() for key in url.query}
    if param.casefold() in existing:
        return url
    return url.set(query={**url.query, param: CLICKHOUSE_CLIENT_NAME})
