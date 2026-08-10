"""Resolve and query the product usage-events source of truth (ES or Postgres)."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from tests.utilities import env_vars

logger = logging.getLogger(__name__)

_SYSTEM_INFO_PATH = "/openapi/v1/system-info/properties/simple"
_USAGE_EVENTS_PROP = "platformAnalytics.usage-events.implementation"
_SYSTEM_INFO_TIMEOUT_SEC = 10

# LoginSource enum name ↔ camelCase ``source`` string (see LoginSource.java).
_LOGIN_SOURCE_ENUM_BY_CANON = {
    "passwordreset": "PASSWORD_RESET",
    "passwordlogin": "PASSWORD_LOGIN",
    "fallbacklogin": "FALLBACK_LOGIN",
    "signuplinklogin": "SIGN_UP_LINK_LOGIN",
    "guestlogin": "GUEST_LOGIN",
    "ssologin": "SSO_LOGIN",
}


def canonicalize_login_source(value: Any | None) -> Optional[str]:
    """Normalize loginSource to the OpenAPI enum-name form (e.g. PASSWORD_LOGIN)."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return _LOGIN_SOURCE_ENUM_BY_CANON.get(raw.replace("_", "").lower(), raw)


def login_sources_equivalent(actual: Any | None, expected: Any | None) -> bool:
    """True when values match as enum name or camelCase LoginSource.source."""
    if expected is None:
        return True
    if actual is None:
        return False
    return canonicalize_login_source(actual) == canonicalize_login_source(expected)


def resolve_usage_events_implementation(
    auth_session: Any | None = None,
) -> str:
    """Return ``postgres`` or ``elasticsearch`` for the running deployment.

    Order:
    1. Live GMS system-info (preferred — matches the running containers).
    2. Env / common-env / compose-profile inference as fallback.
    """
    live = _usage_events_implementation_from_gms(auth_session)
    if live:
        return live
    return env_vars.get_usage_events_implementation()


def usage_events_stored_in_postgres(auth_session: Any | None = None) -> bool:
    return resolve_usage_events_implementation(auth_session) == "postgres"


def _candidate_base_urls(auth_session: Any | None) -> List[str]:
    """URLs that may expose GMS system-info (direct GMS or frontend proxy)."""
    urls: List[str] = []
    if auth_session is not None:
        gms_url_fn = getattr(auth_session, "gms_url", None)
        if callable(gms_url_fn):
            try:
                url = gms_url_fn()
                if url:
                    urls.append(str(url).rstrip("/"))
            except Exception:
                pass

    env_gms = env_vars.get_gms_url()
    if env_gms:
        urls.append(env_gms.rstrip("/"))

    try:
        from tests.utils import get_frontend_url

        frontend = get_frontend_url()
        if frontend:
            urls.append(str(frontend).rstrip("/"))
    except Exception:
        pass

    deduped: List[str] = []
    seen: set[str] = set()
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def _get_system_info_props(
    auth_session: Any | None, base_url: str
) -> Optional[Dict[str, Any]]:
    url = f"{base_url}{_SYSTEM_INFO_PATH}"
    try:
        if auth_session is not None and hasattr(auth_session, "get"):
            try:
                response = auth_session.get(url, timeout=_SYSTEM_INFO_TIMEOUT_SEC)
            except TypeError:
                # Some session wrappers may not forward timeout.
                response = auth_session.get(url)
        else:
            response = requests.get(url, timeout=_SYSTEM_INFO_TIMEOUT_SEC)
        if response.status_code != 200:
            return None
        props = response.json()
        return props if isinstance(props, dict) else None
    except Exception as exc:
        logger.debug("system-info fetch failed for %s: %s", url, exc)
        return None


def _usage_events_implementation_from_gms(
    auth_session: Any | None = None,
) -> Optional[str]:
    for base_url in _candidate_base_urls(auth_session):
        props = _get_system_info_props(auth_session, base_url)
        if not props:
            continue
        raw = props.get(_USAGE_EVENTS_PROP)
        if raw is None:
            continue
        value = str(raw).strip().lower()
        if value in ("postgres", "elasticsearch"):
            logger.info(
                "Usage-events SoT from GMS system-info=%s (via %s)", value, base_url
            )
            return value
    return None


def search_usage_events(
    *,
    auth_session: Any,
    size: int,
    event_types: List[str],
    actor_urns: List[str],
    aspect_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Search recent usage/audit events in the active SoT.

    Returns the same shape as ``POST /openapi/v1/events/audit/search``:
    ``{"usageEvents": [ {...}, ... ]}``.
    """
    aspect_names = aspect_names or []
    if usage_events_stored_in_postgres(auth_session):
        return {
            "usageEvents": _search_usage_events_postgres(
                size=size,
                event_types=event_types,
                actor_urns=actor_urns,
                aspect_names=aspect_names,
            )
        }

    from tests.utils import get_frontend_url

    payload = {
        "eventTypes": event_types,
        "actorUrns": actor_urns,
        "aspectTypes": aspect_names,
    }
    response = auth_session.post(
        f"{get_frontend_url()}/openapi/v1/events/audit/search?size={size}",
        json=payload,
    )
    response.raise_for_status()
    return response.json()


def _search_usage_events_postgres(
    *,
    size: int,
    event_types: List[str],
    actor_urns: List[str],
    aspect_names: List[str],
) -> List[Dict[str, Any]]:
    import psycopg2
    import psycopg2.extras

    host_port = env_vars.get_postgres_url()
    host, _, port_s = host_port.partition(":")
    port = int(port_s or "5432")

    clauses = ["metric_family = 'datahub_usage'"]
    params: List[Any] = []
    if event_types:
        clauses.append("event_type = ANY(%s)")
        params.append(list(event_types))
    if actor_urns:
        clauses.append("actor_urn = ANY(%s)")
        params.append(list(actor_urns))
    if aspect_names:
        clauses.append("(aspect_name = ANY(%s) OR document->>'aspectName' = ANY(%s))")
        params.append(list(aspect_names))
        params.append(list(aspect_names))

    sql = f"""
        SELECT
            event_type,
            actor_urn,
            entity_urn,
            entity_type,
            aspect_name,
            (EXTRACT(EPOCH FROM event_time) * 1000)::bigint AS timestamp_ms,
            document
        FROM metadata_analytics_event
        WHERE {" AND ".join(clauses)}
        ORDER BY event_time DESC
        LIMIT %s
    """
    params.append(size)

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=env_vars.get_postgres_username(),
        password=env_vars.get_postgres_password(),
        dbname="datahub",
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    events: List[Dict[str, Any]] = []
    for row in rows:
        document = row.get("document") or {}
        if not isinstance(document, dict):
            document = {}
        login_source = canonicalize_login_source(document.get("loginSource"))
        event = {
            "eventType": row.get("event_type") or document.get("type"),
            "actorUrn": row.get("actor_urn") or document.get("actorUrn"),
            "entityUrn": row.get("entity_urn") or document.get("entityUrn"),
            "entityType": row.get("entity_type") or document.get("entityType"),
            "aspectName": row.get("aspect_name") or document.get("aspectName"),
            "timestamp": int(row.get("timestamp_ms") or document.get("timestamp") or 0),
            "loginSource": login_source,
        }
        events.append(event)
    return events


def assert_tracking_event_indexed(
    unique_id: str,
    *,
    event_type: str,
    auth_session: Any | None = None,
) -> None:
    """Assert a tracking event landed in the active SoT (and not the inactive one)."""
    if usage_events_stored_in_postgres(auth_session):
        _assert_tracking_event_in_postgres(unique_id, event_type=event_type)
        _assert_no_tracking_event_in_elasticsearch(unique_id, event_type=event_type)
    else:
        _assert_tracking_event_in_elasticsearch(unique_id, event_type=event_type)


def _assert_tracking_event_in_elasticsearch(unique_id: str, *, event_type: str) -> None:
    es_url = env_vars.get_elasticsearch_url()
    es_index = env_vars.get_elasticsearch_index()
    es_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"customField": unique_id}},
                    {"term": {"type": event_type}},
                ]
            }
        }
    }
    es_response = requests.post(f"{es_url}/{es_index}/_search", json=es_query)
    assert es_response.status_code == 200, (
        f"Failed to query Elasticsearch: {es_response.text}"
    )
    hits = es_response.json().get("hits", {}).get("hits", [])
    assert len(hits) > 0, "No matching tracking events found in Elasticsearch"
    event = hits[0].get("_source", {})
    assert event.get("type") == event_type
    assert event.get("actorUrn") == "urn:li:corpuser:test_user"
    assert event.get("customField") == unique_id


def _assert_tracking_event_in_postgres(unique_id: str, *, event_type: str) -> None:
    import psycopg2

    host_port = env_vars.get_postgres_url()
    host, _, port_s = host_port.partition(":")
    port = int(port_s or "5432")
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=env_vars.get_postgres_username(),
        password=env_vars.get_postgres_password(),
        dbname="datahub",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_type, actor_urn,
                       COALESCE(browser_id, document->>'browserId') AS browser_id,
                       document->>'customField' AS custom_field
                FROM metadata_analytics_event
                WHERE document->>'customField' = %s
                   OR browser_id = %s
                   OR document->>'browserId' = %s
                ORDER BY event_time DESC
                LIMIT 5
                """,
                (unique_id, unique_id, unique_id),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    assert rows, "No matching tracking events found in Postgres pgAnalytics"
    got_type, actor_urn, _browser_id, custom_field = rows[0]
    assert got_type == event_type
    assert actor_urn == "urn:li:corpuser:test_user"
    assert custom_field == unique_id


def _assert_no_tracking_event_in_elasticsearch(
    unique_id: str, *, event_type: str
) -> None:
    es_url = env_vars.get_elasticsearch_url()
    es_index = env_vars.get_elasticsearch_index()
    es_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"customField": unique_id}},
                    {"term": {"type": event_type}},
                ]
            }
        }
    }
    es_response = requests.post(f"{es_url}/{es_index}/_search", json=es_query)
    assert es_response.status_code == 200, (
        f"Failed to query Elasticsearch: {es_response.text}"
    )
    hits = es_response.json().get("hits", {}).get("hits", [])
    assert len(hits) == 0, (
        "Expected no usage events in Elasticsearch when SoT is postgres; "
        f"found {len(hits)} for customField={unique_id}"
    )
