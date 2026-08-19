"""Secret redaction for recording archives.

This module centralizes secret detection and redaction for everything that
ends up inside a recording archive:

- Recipes: secret values are replaced with REPLAY_DUMMY_MARKER based on key
  names (see redact_secrets).
- HTTP cassettes: request bodies, response bodies, query parameters, and
  sensitive headers are scrubbed before VCR persists them (see
  scrub_request_for_recording / scrub_response_for_recording). Without this,
  OAuth token exchanges would leak client_secret values and issued bearer
  tokens into http/cassette.yaml even though the recipe itself is redacted.

Secrets are always *replaced* with the stable REPLAY_DUMMY_MARKER rather than
deleted. Replay matches requests on URI/method/body, so a token that a source
embeds in a follow-up request (e.g. a query parameter) must be scrubbed to the
same value in both the token response and the recorded follow-up request for
the replayed traffic to keep matching.
"""

import json
import logging
import re
from collections.abc import MutableMapping
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from datahub.ingestion.recording.config import REPLAY_DUMMY_MARKER

logger = logging.getLogger(__name__)

# Patterns for secret field names (case-insensitive). Used for recipe keys,
# JSON body keys, form parameters, query parameters, and header names.
SECRET_PATTERNS = [
    r".*password.*",
    r".*secret.*",
    r".*token.*",
    r".*api[_-]?key.*",
    r".*private[_-]?key.*",
    r".*access[_-]?key.*",
    r".*credential.*",
]

_SECRET_PATTERNS_COMPILED = [
    re.compile(pattern, re.IGNORECASE) for pattern in SECRET_PATTERNS
]

# Fields that contain auth-related words but are NOT secrets
# These are typically enum values, config options, or metadata
NON_SECRET_FIELDS = [
    "authentication_type",
    "authenticator",
    "auth_type",
    "auth_method",
    "authorization_type",
    # OAuth metadata: "token_type" is an enum like "Bearer" that sources may
    # validate during replay, and token endpoint URLs are not secrets.
    "token_type",
    "token_url",
    "token_endpoint",
]

# Request/response headers that carry credentials but whose names don't match
# SECRET_PATTERNS. Note that VCR's filter_headers option only applies to
# *request* headers, so response headers (Set-Cookie in particular) must be
# scrubbed explicitly.
SENSITIVE_HEADERS = [
    "authorization",
    "proxy-authorization",
    "x-datahub-auth",
    "cookie",
    "set-cookie",
]


def is_secret_key(key: str) -> bool:
    """Check if a key name indicates a secret value.

    Checks against known secret patterns while excluding fields that
    contain auth-related words but are not secrets (e.g., authentication_type).
    """
    key_lower = key.lower()

    # First check if this is a known non-secret field
    if key_lower in NON_SECRET_FIELDS:
        return False

    # Then check against secret patterns
    return any(pattern.match(key_lower) for pattern in _SECRET_PATTERNS_COMPILED)


def _is_sensitive_header(name: str) -> bool:
    return name.lower() in SENSITIVE_HEADERS or is_secret_key(name)


def redact_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
    """Replace secret values with replay-safe dummy markers.

    This function recursively traverses the config and replaces any values
    that appear to be secrets (based on key names) with REPLAY_DUMMY_MARKER.
    This allows the recipe to be safely stored while still being loadable
    during replay.

    Args:
        config: Configuration dictionary (e.g., recipe)

    Returns:
        New dictionary with secrets replaced by REPLAY_DUMMY_MARKER
    """
    return _redact_recursive(config)


def _redact_recursive(obj: Any, parent_key: str = "") -> Any:
    """Recursively redact secrets in a nested structure."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if is_secret_key(key) and isinstance(value, str):
                result[key] = REPLAY_DUMMY_MARKER
                logger.debug(f"Redacted secret field: {key}")
            else:
                result[key] = _redact_recursive(value, key)
        return result

    if isinstance(obj, list):
        return [_redact_recursive(item, parent_key) for item in obj]

    return obj


def scrub_uri(uri: str) -> str:
    """Scrub secret-bearing query parameters from a URI.

    The URI is only rewritten when a secret parameter is actually found, so
    URIs without secrets keep their exact original encoding (replay matches
    requests by exact URI string).
    """
    split = urlsplit(uri)
    if not split.query:
        return uri

    params: List[Tuple[str, str]] = parse_qsl(split.query, keep_blank_values=True)
    changed = False
    scrubbed = []
    for key, value in params:
        if value and is_secret_key(key):
            scrubbed.append((key, REPLAY_DUMMY_MARKER))
            changed = True
        else:
            scrubbed.append((key, value))

    if not changed:
        return uri
    return urlunsplit(split._replace(query=urlencode(scrubbed)))


def scrub_body(body: Union[str, bytes, None]) -> Union[str, bytes, None]:
    """Scrub secrets from an HTTP request or response body.

    Handles JSON bodies (recursively, by key name) and form-encoded bodies
    (by parameter name). Binary and unrecognized bodies are returned as-is.
    The body is only rewritten when a secret is actually found, so bodies
    without secrets keep their exact original serialization (replay matches
    requests by body).
    """
    if not body:
        return body

    was_bytes = isinstance(body, bytes)
    if isinstance(body, bytes):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            # Binary body (protobuf, gRPC, etc.) - cannot scrub by key
            return body
    else:
        text = body

    scrubbed = _scrub_json_text(text)
    if scrubbed is None:
        scrubbed = _scrub_form_text(text)
    if scrubbed is None or scrubbed == text:
        return body

    return scrubbed.encode("utf-8") if was_bytes else scrubbed


def _scrub_json_text(text: str) -> Optional[str]:
    """Scrub a JSON body by key name. Returns None if the body is not JSON."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None

    if not isinstance(data, (dict, list)):
        return None

    redacted = _redact_recursive(data)
    if redacted == data:
        return text
    return json.dumps(redacted)


def _scrub_form_text(text: str) -> Optional[str]:
    """Scrub a form-encoded body by parameter name.

    Returns None if the body doesn't look form-encoded. The body is only
    re-encoded when a secret parameter is found (e.g. client_secret in an
    OAuth token exchange); those requests hit auth endpoints where replay
    skips body matching, so re-encoding is safe.
    """
    if "=" not in text or "\n" in text:
        return None

    params = parse_qsl(text, keep_blank_values=True)
    if not params:
        return None

    changed = False
    scrubbed = []
    for key, value in params:
        if value and is_secret_key(key):
            scrubbed.append((key, REPLAY_DUMMY_MARKER))
            changed = True
        else:
            scrubbed.append((key, value))

    if not changed:
        return text
    return urlencode(scrubbed)


def _scrub_headers(headers: "MutableMapping[str, Any]") -> None:
    """Scrub sensitive headers in place.

    Header values may be strings or lists of strings depending on where VCR
    is in its serialization pipeline.
    """
    for name in list(headers):
        if not _is_sensitive_header(name):
            continue
        if isinstance(headers[name], list):
            headers[name] = [REPLAY_DUMMY_MARKER]
        else:
            headers[name] = REPLAY_DUMMY_MARKER


def scrub_request_for_recording(request: Any) -> Any:
    """Scrub secrets from a VCR request before it is written to the cassette.

    VCR deep-copies the request before invoking before_record_request hooks,
    so mutating it here only affects what is persisted, never the live
    request.

    Args:
        request: A vcr.request.Request object.

    Returns:
        The scrubbed request.
    """
    request.uri = scrub_uri(request.uri)
    request.body = scrub_body(request.body)
    # filter_headers covers the well-known auth headers; this additionally
    # catches pattern-based ones like X-Api-Key. Note: VCR's HeadersDict is a
    # CaseInsensitiveDict (a MutableMapping, not a dict subclass).
    if isinstance(request.headers, MutableMapping):
        _scrub_headers(request.headers)
    return request


def scrub_response_for_recording(response: Dict[str, Any]) -> Dict[str, Any]:
    """Scrub secrets from a VCR response before it is written to the cassette.

    This is the critical half for OAuth flows: token endpoint responses
    contain access_token / refresh_token values that VCR would otherwise
    record verbatim. VCR deep-copies the response before invoking
    before_record_response hooks, so in-place mutation is safe.

    Args:
        response: VCR response dict ({"status": ..., "headers": ...,
            "body": {"string": ...}}).

    Returns:
        The scrubbed response.
    """
    headers = response.get("headers")
    if isinstance(headers, MutableMapping):
        _scrub_headers(headers)

    body = response.get("body")
    if isinstance(body, dict) and "string" in body:
        original = body["string"]
        scrubbed = scrub_body(original)
        if scrubbed != original:
            body["string"] = scrubbed
            if isinstance(headers, MutableMapping):
                _update_content_length(headers, scrubbed)

    return response


def _update_content_length(
    headers: "MutableMapping[str, Any]", body: Union[str, bytes, None]
) -> None:
    """Keep Content-Length consistent with a scrubbed (shorter) body.

    Replay serves the recorded body with the recorded headers; a stale
    Content-Length causes IncompleteRead errors in the HTTP client.
    """
    length = len(body.encode("utf-8") if isinstance(body, str) else (body or b""))
    for name in headers:
        if name.lower() == "content-length":
            if isinstance(headers[name], list):
                headers[name] = [str(length)]
            else:
                headers[name] = str(length)
            return
